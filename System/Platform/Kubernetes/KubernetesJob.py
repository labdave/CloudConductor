import logging
import requests
import time
import os
import ast
import re

from System.Platform.Instance import Instance
from System.Platform.Kubernetes.utils import api_request
from collections import OrderedDict

from kubernetes import client


class KubernetesJob(Instance):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        super(KubernetesJob, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        self.NODE_POOLS = {
            "cc2-highmem-pool" :  {"max_cpu": 2, "max_mem": 13},
            "cc4-highmem-pool" :  {"max_cpu": 4, "max_mem": 26},
            "cc8-highmem-pool" :  {"max_cpu": 8, "max_mem": 52},
            "cc16-highmem-pool" :  {"max_cpu": 16, "max_mem": 104},
            "cc32-highmem-pool" :  {"max_cpu": 32, "max_mem": 208},
        }

        self.NODE_POOLS_PREEMPTIBLE = {
            "cc2-highmem-preemptible-pool" :  {"max_cpu": 2, "max_mem": 13},
            "cc4-highmem-preemptible-pool" :  {"max_cpu": 4, "max_mem": 26},
            "cc8-highmem-preemptible-pool" :  {"max_cpu": 8, "max_mem": 52},
            "cc16-highmem-preemptible-pool" :  {"max_cpu": 16, "max_mem": 104},
            "cc32-highmem-preemptible-pool" :  {"max_cpu": 32, "max_mem": 208},
        }

        self.task_prefix_filter = ['mkdir_wrk_dir', 'docker_pull']

        self.namespace = 'cloud-conductor'

        self.batch_processing = True
        self.job_def = None
        self.pvc_name = ''
        self.task_pvc = None

        # define how long the job lasts after completion
        self.termination_seconds = 600

        # Obtain the mother platform object
        self.platform = kwargs.pop("platform")
        self.preemptible = kwargs.pop("preemptible", False)

        self.batch_api = self.platform.batch_api
        self.core_api = self.platform.core_api

        self.start_time = 0
        self.stop_time = 0

    def get_nodepool_label(self):
        node_pool_dict = self.NODE_POOLS
        if self.preemptible:
            node_pool_dict = self.NODE_POOLS_PREEMPTIBLE

        for k, v in node_pool_dict.items():
            if v['max_cpu'] >= self.nr_cpus and v['max_mem'] >= self.mem:
                return k

    def run(self, job_name, cmd, num_retries=None, docker_image=None):
        logging.debug(f"({self.name}) Adding {job_name} to the task list.")

        for prefix in self.task_prefix_filter:
            if prefix in job_name:
                return

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = f"{job_name}.log"
            if self.wrk_log_dir is not None:
                log_file = os.path.join(self.wrk_log_dir, log_file)

            # Generating the logging pipes
            log_cmd_all     = f" >>{log_file}"

            # Replacing the placeholders with the logging pipes
            cmd = re.sub(r"!LOG.*!", log_cmd_all, cmd)

        # Save original command
        original_cmd = cmd

        # Generating process arguments
        task = {
            # Add CloudConductor specific arguments
            "original_cmd": original_cmd,
            "num_retries": self.default_num_cmd_retries if num_retries is None else num_retries,
            "docker_image": docker_image
        }
        self.processes[job_name] = task

    def destroy(self):
        # Destroy the job
        # only want to destroy a job if it's active
        status = self.get_status()
        if status and isinstance(status, dict) and status.get("active"):
            delete_response = api_request(self.batch_api.delete_namespaced_job, self.name, self.namespace)

            # Save the status if the job is no longer active
            delete_status = delete_response.get("status", None)
            if not isinstance(delete_status, dict):
                delete_status = ast.literal_eval(delete_status)
            if delete_status and isinstance(delete_status, dict) or delete_status == 'Failure' or delete_status.get("failed") or delete_status.get("succeeded"):
                logging.debug(f"({self.name}) Kubernetes job successfully destroyed.")
            else:
                raise RuntimeError(f"({self.name}) Failure to destroy the Kubernetes Job on the cluster!")

        # Destroy the persistent volume claim
        pvc_response = api_request(self.core_api.delete_namespaced_persistent_volume_claim, self.pvc_name, self.namespace)

        # Save the status if the job is no longer active
        pvc_status = pvc_response.get("status", None)
        if not isinstance(pvc_status, dict):
            pvc_status = ast.literal_eval(pvc_status)
        if pvc_status and isinstance(pvc_status, dict):
            logging.debug(f"({self.name}) Persistent Volume Claim successfully destroyed.")
        else:
            raise RuntimeError(f"({self.name}) Failure to destroy the Persistent Volume Claim on the cluster!")     

    def wait_process(self, proc_name):
        # Kubernetes Job waits until all tasks have been assigned to run
        return '', ''

    def wait(self):
        # launch kubernetes job with all processes
        logging.debug(f"({self.name}) Starting creation of Kubernetes job.")

        # create the persistent volume claim for the job
        self.__create_volume_claim()

        # create the job definition
        self.__create_job_def()

        try:
            creation_response = api_request(self.batch_api.create_namespaced_job, self.namespace, self.job_def)
            creation_status = creation_response.get("status", None)
            if creation_status and isinstance(creation_status, dict) and creation_status != 'Failure':
                logging.debug(f"({self.name}) Kubernetes job successfully created! Begin monitoring.")
            else:
                raise RuntimeError(f"({self.name}) Failure to create the job on the cluster")
        except:
            raise RuntimeError(f"({self.name}) Failure to create the job on the cluster")

        # begin monitoring job for completion/failure
        monitoring = True
        while monitoring:
            time.sleep(30)
            job_status = self.get_status()
            if isinstance(job_status, dict) and not job_status.get("active"):
                monitoring = False
                self.stop_time = time.mktime(job_status['completion_time'].timetuple())
                if job_status.get("succeeded"):
                    logging.info(f"({self.name}) Process complete!")
                if job_status.get("failed"):
                    raise RuntimeError(f"({self.name}) Instance failed!")

    def finalize(self):
        pass

    def get_start_time(self):
        return self.start_time

    def get_stop_time(self):
        return self.stop_time

    def get_status(self, log_status=False):
        s = api_request(self.batch_api.read_namespaced_job_status, self.name, self.namespace)
        # Save the status if the job is no longer active
        job_status = s.get("status", dict())
        if log_status:
            logging.debug(f"({self.name}) Job Status: {job_status}")
        if job_status:
            if self.start_time == 0:
                self.start_time = time.mktime(job_status['start_time'].timetuple())
            return job_status
        return s

    def add_checkpoint(self, clear_output=True):
        pass

    def compute_cost(self):
        return 0

    def get_compute_price(self):
        return 0

    def get_storage_price(self):
        return 0

    def __create_volume_claim(self):
        # create the persistent volume claim for the task
        self.pvc_name = self.name+'-vc'
        pvc_meta = client.V1ObjectMeta(name=self.pvc_name, namespace=self.namespace)
        pvc_resources = client.V1ResourceRequirements(requests={'storage': str(self.disk_space)+'Gi'})
        pvc_spec = client.V1PersistentVolumeClaimSpec(access_modes=['ReadWriteOnce'], resources=pvc_resources, storage_class_name='standard')
        self.task_pvc = client.V1PersistentVolumeClaim(metadata=pvc_meta, spec=pvc_spec)

        pvc_response = api_request(self.core_api.create_namespaced_persistent_volume_claim, self.namespace, self.task_pvc)

        # Save the status if the job is no longer active
        pvc_status = pvc_response.get("status", None)
        if pvc_status and isinstance(pvc_status, dict):
            logging.debug(f"({self.name}) Persistent Volume Claim created.")
        else:
            raise RuntimeError(f"({self.name}) Failure to create a Persistent Volume Claim on the cluster")

    def __create_job_def(self):
        # initialize the job def body
        self.job_def = client.V1Job(kind="Job")
        self.job_def.metadata = client.V1ObjectMeta(namespace=self.namespace, name=self.name)

        # initialize job pieces
        volume_mounts = []
        volumes = []
        containers = []
        init_containers = []
        env_variables = []

        volume_name = self.name+'-pd'
        # build volume mounts
        volume_mounts = []
        volume_mounts.append(
            client.V1VolumeMount(
                mount_path=self.wrk_dir,
                name=volume_name
            )
        )

        # define resource limits/requests
        resource_def = client.V1ResourceRequirements(
            limits={'cpu': self.nr_cpus, 'memory': str(self.mem)+'G'},
            requests={'cpu': self.nr_cpus*.8, 'memory': str(self.mem-1)+'G'}
        )

        # place the job in the appropriate node pool
        node_label_dict = {'poolName': str(self.get_nodepool_label())}


        # build volumes
        volumes.append(
            client.V1Volume(
                name=volume_name,
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=self.pvc_name
                )
            )
        )

        # incorporate configured secrets
        if self.platform.gcp_secret_configured:
            volume_mounts.append(
                client.V1VolumeMount(
                    mount_path="/etc/cloud_conductor/gcp.json",
                    sub_path="gcp.json",
                    name="secret-volume",
                    read_only=True
                )
            )
            volumes.append(
                client.V1Volume(
                    name="secret-volume",
                    secret=client.V1SecretVolumeSource(
                        secret_name="cloud-conductor-config",
                        items=[client.V1KeyToPath(key="gcp_json", path="gcp.json")]
                    )
                )
            )
            env_variables.append(client.V1EnvVar(name='GOOGLE_APPLICATION_CREDENTIALS', value='/etc/cloud_conductor/gcp.json'))

        if self.platform.aws_secret_configured:
            env_variables.append(client.V1EnvVar(name='AWS_ACCESS_KEY_ID', value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='cloud-conductor-config', key='aws_id'))))
            env_variables.append(client.V1EnvVar(name='AWS_SECRET_ACCESS_KEY', value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='cloud-conductor-config', key='aws_access'))))

        storage_image = 'gcr.io/cloud-builders/gsutil'
        storage_tasks = ['load_input', 'save_output', 'mkdir_', 'grant_', 'return_logs']

        for k, v in self.processes.items():
            # if the process is for storage (i.e. mkdir, etc.)
            if any(x in k for x in storage_tasks):
                container_image = storage_image
            else:
                container_image = v['docker_image']
            args = v['original_cmd']
            if not isinstance(args, list):
                args = [v['original_cmd'].replace("sudo ", "")]
            args = " && ".join(args)

            if "gsutil" in args:
                args = "gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS && " + args

            # format the container name
            container_name = k.replace("_", "-").replace(".", "-").lower()

            containers.append(client.V1Container(
                    # lifecycle=client.V1Lifecycle(post_start=post_start_handler),
                    image=container_image,
                    command=["/bin/sh", "-c"],
                    args=[args],
                    name=container_name,
                    volume_mounts=volume_mounts,
                    env=env_variables,
                    resources=resource_def,
                    image_pull_policy='Always'
                )
            )

        job_spec = dict(
            backoff_limit=0
        )

        # Run jobs in order using init_containers
        # See https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
        if len(containers) > 1:
            init_containers = containers[:-1]
            containers = [containers[-1]]
        else:
            containers = containers
            init_containers = None

        # define the pod spec
        job_template = client.V1PodTemplateSpec()
        job_template.spec = client.V1PodSpec(
            init_containers=init_containers,
            containers=containers,
            volumes=volumes,
            restart_policy='Never',
            termination_grace_period_seconds=self.termination_seconds,
            node_selector=node_label_dict
        )

        self.job_def.spec = client.V1JobSpec(template=job_template, **job_spec)
