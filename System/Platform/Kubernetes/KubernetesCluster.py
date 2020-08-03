import logging
import requests
import time
import os
import json

from System import CC_MAIN_DIR
from System.Platform import Process
from System.Platform.Platform import Platform
from threading import Thread

from System.Platform.Kubernetes import KubernetesJob
from System.Platform.Kubernetes.utils import api_request, get_api_sleep

from kubernetes import config, client


class KubernetesCluster(Platform):
    CONFIG_SPEC = f"{CC_MAIN_DIR}/System/Platform/Kubernetes/Platform.validate"

    def __init__(self, name, platform_config_file, final_output_dir):
        super(KubernetesCluster, self).__init__(name, platform_config_file, final_output_dir, config_spec=self.CONFIG_SPEC)

        self.jobs = {}

        self.lockable = False

        self.configuration = None
        self.batch_api = None
        self.core_api = None

        self.service_provider = self.config.get("provider", 'GKE')
        self.gcp_secret_configured = "gcp_secret_configured" in self.config and self.config["gcp_secret_configured"]
        self.aws_secret_configured = "aws_secret_configured" in self.config and self.config["aws_secret_configured"]

        self.region = self.config["region"]
        self.zone = self.config.get("zone", None)

        self.cpu_reserve = self.config.get("cpu_reserve", 0.1)
        self.mem_reserve = self.config.get("mem_reserve", 1)
        self.pools = self.config.get("pools", [])
        self.persistent_volumes = self.config.get("persistent_volumes", [])
        self.storage_price = self.config.get("storage_price", 0)

    def get_instance(self, nr_cpus, mem, disk_space, **kwargs):
        """Initialize new job and register with platform"""

        # Obtain task_id that will be used
        task_id = kwargs.pop("task_id", "NONAME")

        job_name = f'{self.name[:20]}-{task_id[:25]}-{self.generate_unique_id()}'

        if task_id is not None:
            logging.debug(f'({job_name}) Creating job for task "{task_id}"!')
        else:
            logging.debug(f'({job_name}) Creating job!')

        # Standardize instance type
        job_name, nr_cpus, mem, disk_space = self.standardize_instance(job_name, nr_cpus, mem, disk_space)

        preemptible = "preemptible" in self.config and self.config["preemptible"]

        # Load job kwargs with platform variables
        kwargs.update({
            "identity": self.identity,
            "cmd_retries": self.cmd_retries,
            "final_output_dir": self.final_output_dir,

            "preemptible": preemptible,
            "provider": self.service_provider,
            "region": self.region,
            "zone": self.zone,
            "pools": self.pools,
            "persistent_volumes": self.persistent_volumes,
            "cpu_reserve": self.cpu_reserve,
            "mem_reserve": self.mem_reserve,
            "storage_price": self.storage_price,
            "gcp_secret_configured": self.gcp_secret_configured,
            "aws_secret_configured": self.aws_secret_configured,
            "batch_api": self.batch_api,
            "core_api": self.core_api
        })

        # Initialize new instance
        try:
            self.jobs[job_name] = KubernetesJob(job_name, nr_cpus, mem, disk_space, **kwargs)

            logging.info(f'({job_name}) Job was successfully created!')

            return self.jobs[job_name]

        except BaseException:

            # Raise the actual exception
            raise

    def authenticate_platform(self):
        cert_path, host, api_token, api_prefix = self.__parse_identity_json(self.identity)
        self.configuration = client.Configuration()
        self.configuration.api_key["authorization"] = api_token
        self.configuration.api_key_prefix['authorization'] = api_prefix
        self.configuration.host = host
        self.configuration.ssl_ca_cert = cert_path
        self.batch_api = client.BatchV1Api(client.ApiClient(self.configuration))
        self.core_api = client.CoreV1Api(client.ApiClient(self.configuration))

    def init_platform(self):
        # Authenticate the current platform
        self.authenticate_platform()

        # Validate the current platform
        self.validate()

    def validate(self):
        try:
            namespace_list = api_request(self.batch_api.list_namespaced_job, namespace='cloud-conductor')
            if not namespace_list or not namespace_list.items:
                logging.error("Failed to validate Kubernetes platform.")
                raise RuntimeError("Failed to validate Kubernetes platform.")
        except BaseException as e:
            logging.error(f"Failed to validate Kubernetes platform.")
            raise

    @staticmethod
    def standardize_instance(job_name, nr_cpus, mem, disk_space):

        # Ensure instance name does not contain weird characters
        job_name = job_name.replace("_", "-").replace(".", "-").lower()

        return job_name, nr_cpus, mem, disk_space

    def get_disk_image_size(self):
        # no disk images for Kubernetes. return 0
        return 0

    def publish_report(self, report_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir, os.path.basename(report_path))

        # Authenticate for gsutil use
        cmd = "gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS"
        Process.run_local_cmd(cmd, err_msg="Authentication to Google Cloud failed!")

        # Transfer report file to bucket
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp -r '%s' '%s' 1>/dev/null 2>&1 " % (options_fast, report_path, dest_path)
        Process.run_local_cmd(cmd, err_msg="Could not transfer final report to the final output directory!")

    def push_log(self, log_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir, os.path.basename(log_path))

        # Authenticate for gsutil use
        cmd = "gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS"
        Process.run_local_cmd(cmd, err_msg="Authentication to Google Cloud failed!")

        # Transfer report file to bucket
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp -r '%s' '%s' 1>/dev/null 2>&1 " % (options_fast, log_path, dest_path)
        Process.run_local_cmd(cmd, err_msg="Could not transfer final log to the final output directory!")

        # Transfer failed module log file to bucket
        failed_module_log_path = log_path.replace("cc_log.txt", "failed_module_log.txt")
        failed_module_dest_path = dest_path.replace("cc_log.txt", "failed_module_log.txt")
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp -r '%s' '%s' 1>/dev/null 2>&1 " % (options_fast, failed_module_log_path, failed_module_dest_path)
        Process.run_local_cmd(cmd, err_msg="Could not transfer failed module log to the final output directory!")

    def clean_up(self):
        # Initialize the list of threads
        destroy_threads = []

        # Launch the destroy process for each instance
        for name, job_obj in self.jobs.items():
            if job_obj is None:
                continue

            thr = Thread(target=job_obj.destroy, daemon=True)
            thr.start()
            destroy_threads.append(thr)

        # Wait for all threads to finish
        for _thread in destroy_threads:
            _thread.join()

    def __parse_identity_json(self, identity):
        api_key_prefix = 'Bearer'
        f = open(self.identity) 
        auth_config = json.load(f)
        f.close()

        return auth_config['cert'], auth_config['host'], auth_config['api_key'], api_key_prefix
