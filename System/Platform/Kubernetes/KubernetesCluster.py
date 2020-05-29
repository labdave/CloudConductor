import logging
import requests
import time
import os

from System.Platform import Process
from System.Platform import Process
from System.Platform.Platform import Platform

from System.Platform.Kubernetes import KubernetesJob
from System.Platform.Kubernetes.utils import api_request

from kubernetes import config, client


class KubernetesCluster(Platform):

    def __init__(self, name, platform_config_file, final_output_dir):
        super(KubernetesCluster, self).__init__(name, platform_config_file, final_output_dir)

        self.jobs = {}

        self.lockable = False

        self.configuration = None
        self.batch_api = None
        self.core_api = None

        self.service_provider = self.extra.get("provider", 'GKE')
        self.gcp_secret_configured = "gcp_secret_configured" in self.extra and self.extra["gcp_secret_configured"]
        self.aws_secret_configured = "aws_secret_configured" in self.extra and self.extra["aws_secret_configured"]

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

        preemptible = "preemptible" in self.extra and self.extra["preemptible"]

        # Load job kwargs with platform variables
        kwargs.update({
            "identity": self.identity,
            "cmd_retries": self.cmd_retries,

            "platform": self,
            "preemptible": preemptible
        })

        # Also add the extra information
        kwargs.update(self.extra)

        # Initialize new instance
        try:
            self.jobs[job_name] = KubernetesJob(job_name, nr_cpus, mem, disk_space, **kwargs)

            logging.info(f'({job_name}) Job was successfully created!')

            return self.jobs[job_name]

        except BaseException:

            # Raise the actual exception
            raise

    def authenticate_platform(self):
        config.load_kube_config(config_file=self.identity)
        self.configuration = client.Configuration()
        self.batch_api = client.BatchV1Api(client.ApiClient(self.configuration))
        self.core_api = client.CoreV1Api(client.ApiClient(self.configuration))

    def init_platform(self):
        # Authenticate the current platform
        self.authenticate_platform()

        # Validate the current platform
        self.validate()

    def validate(self):
        try:
            namespace_list = api_request(self.batch_api.list_namespaced_job, namespace='default')
            if not namespace_list or not namespace_list.items:
                logging.error("Failed to validate Kubernetes platform.")
                raise RuntimeError("Failed to validate Kubernetes platform.")
        except BaseException:
            logging.error(f"Failed to validate Kubernetes platform.")
            raise

    @staticmethod
    def standardize_instance(job_name, nr_cpus, mem, disk_space):

        # Ensure instance name does not contain weird characters
        job_name = job_name.replace("_", "-").replace(".", "-").lower()

        return job_name, nr_cpus, mem, disk_space

    def get_disk_image_size(self):
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

    def clean_up(self):
        # Clear all jobs using self.jobs
        pass
