import logging
import requests
import time
import os
import ast
import re
import tempfile

from System.Platform.Instance import Instance
from System.Platform import Platform, Process
from System.Platform.Kubernetes.utils import api_request
from collections import OrderedDict

from kubernetes import client, config


class KubernetesJob(Instance):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        super(KubernetesJob, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        self.stoppable = False
        self.job_count = 1
        self.inst_name = name
        self.job_containers = []

        self.task_prefix_filter = ['mkdir_wrk_dir', 'docker_pull']

        self.namespace = 'cloud-conductor'
        self.nodepool_info = None
        self.volume_name = None

        self.batch_processing = True
        self.job_def = None
        self.pvc_name = ''
        self.task_pvc = None
        self.monitoring = False
        self.start_time = 0
        self.stop_time = 0

        self.failed_container = None

        # define how long the job lasts after completion
        self.termination_seconds = 600

        # Obtain location specific information
        self.region = kwargs.pop("region")
        self.zone = kwargs.pop("zone")

        # Get API clients
        self.batch_api = kwargs.pop("batch_api")
        self.core_api = kwargs.pop("core_api")

        # Get platform/cluster specific data
        self.storage_price = kwargs.pop("storage_price")
        self.k8s_provider = kwargs.pop("provider")

        self.preemptible = kwargs.pop("preemptible", False)

        all_pools = kwargs.pop("pools", [])

        self.node_pools = [x for x in all_pools if not x["preemptible"]]
        self.preemptible_node_pools = [x for x in all_pools if x["preemptible"]]

        # amount to subtract from max cpu / max mem for monitoring or general k8s overhead pods
        self.cpu_reserve = kwargs.pop("cpu_reserve", 0)
        self.mem_reserve = kwargs.pop("mem_reserve", 0)
        self.gcp_secret_configured = kwargs.pop("gcp_secret_configured", 0)
        self.aws_secret_configured = kwargs.pop("aws_secret_configured", 0)

        self.node_label, self.nodepool_info = self.get_nodepool_info()

    def get_nodepool_info(self):
        node_pool_dict = self.node_pools
        if self.preemptible and self.preemptible_node_pools:
            node_pool_dict = self.preemptible_node_pools

        for pool in node_pool_dict:
            if pool['max_cpu'] >= self.nr_cpus and pool['max_mem'] >= self.mem:
                return pool['name'], pool

    def run(self, job_name, cmd, **kwargs):

        docker_image = kwargs.get("docker_image", None)
        num_retries = kwargs.get("num_retries", self.default_num_cmd_retries)
        docker_entrypoint = kwargs.get("docker_entrypoint", None)

        logging.debug(f"({self.name}) Adding {job_name} to the task list.")

        for prefix in self.task_prefix_filter:
            if prefix in job_name:
                return

        # strip out docker brackets
        # cmd = re.sub(r"{ (.*[\s\S]+);}", r"\1", cmd)

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = f"{job_name}.log"
            if self.wrk_log_dir is not None:
                log_file = os.path.join(self.wrk_log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1"
            log_cmd_stdout  = f" | tee -a {log_file}"
            log_cmd_stderr  = f" 2>>{log_file}"
            log_cmd_all     = f" 2>&1 | tee -a {log_file}"

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Save original command
        original_cmd = cmd

        # Generating process arguments
        task = {
            # Add CloudConductor specific arguments
            "original_cmd": original_cmd,
            "num_retries": self.default_num_cmd_retries if num_retries is None else num_retries,
            "docker_image": docker_image,
            "docker_entrypoint": docker_entrypoint
        }
        self.processes[job_name] = task

    def destroy(self):
        # Destroy the job
        # only want to destroy a job if it's active
        if self.job_def:
            status = self.get_status()
            if status and isinstance(status, dict) and status.get("active"):
                delete_response = api_request(self.batch_api.delete_namespaced_job, self.name, self.namespace)

                # Save the status if the job is no longer active
                delete_status = delete_response.get("status", None)
                if delete_status and delete_status == 'Failure':
                    logging.warning(f"({self.name}) Failed to destroy Kubernetes Job. Message: {delete_response.get('message', '')}")
                elif delete_status and not isinstance(delete_status, dict):
                    delete_status = ast.literal_eval(delete_status)
                elif delete_status and isinstance(delete_status, dict) or delete_status.get("failed") or delete_status.get("succeeded"):
                    logging.debug(f"({self.name}) Kubernetes job successfully destroyed.")

        if self.start_time > 0:
            self.stop_time = time.time()
        # stop monitoring the job
        self.monitoring = False

        if self.pvc_name:
            # Destroy the persistent volume claim
            pvc_response = api_request(self.core_api.delete_namespaced_persistent_volume_claim, self.pvc_name, self.namespace)

            # Save the status if the job is no longer active
            pvc_status = pvc_response.get("status", None)
            if pvc_status and pvc_status == 'Failure':
                logging.warning(f"({self.name}) Failed to destroy Persistent Volume Claim. Message: {pvc_response.get('message', '')}")
            elif pvc_status and not isinstance(pvc_status, dict):
                pvc_status = ast.literal_eval(pvc_status)
            elif pvc_status and isinstance(pvc_status, dict):
                logging.debug(f"({self.name}) Persistent Volume Claim successfully destroyed.")

    def wait_process(self, proc_name):
        if proc_name == "return_logs" and self.failed_container:
            # job has failed so we will push the log to a temporary file which will be pushed to the bucket at the end
            with open("failed_module_log.txt", "w") as log_file:
                log_file.write(self.failed_container['module_log'])
        # Kubernetes Job waits until all tasks have been assigned to run
        return '', ''

    def wait(self, return_last_task_log=False):
        # launch kubernetes job with all processes
        if self.job_count == 1:
            logging.debug(f"({self.name}) Starting creation of Kubernetes job.")
        else:
            logging.debug(f"({self.name}) Starting creation of Kubernetes followup job.")

        # create the persistent volume claim for the job if one doesn't already exist
        if not self.pvc_name:
            self.__create_volume_claim()

        # create the job definition
        self.job_def = self.__create_job_def()

        try:
            creation_response = api_request(self.batch_api.create_namespaced_job, self.namespace, self.job_def)
            creation_status = creation_response.get("status", None)
            if creation_status and isinstance(creation_status, dict) and creation_status != 'Failure':
                logging.debug(f"({self.name}) Kubernetes job successfully created! Begin monitoring.")
            else:
                raise RuntimeError(f"({self.name}) Failure to create the job on the cluster")
        except Exception as e:
            raise RuntimeError(f"({self.name}) Failure to create the job on the cluster")

        # begin monitoring job for completion/failure
        self.monitoring = True
        while self.monitoring:
            time.sleep(30)
            job_status = self.get_status(log_status=True)
            if isinstance(job_status, dict) and not job_status.get("active"):
                self.monitoring = False
                if job_status.get("succeeded"):
                    self.stop_time = job_status['completion_time'].timestamp()
                    logging.info(f"({self.name}) Process complete!")
                    if return_last_task_log:
                        logging.debug("Returning logs from last process.")
                        logs = self.__get_container_log(self.job_containers[len(self.job_containers)-1].name)
                        return logs, ''
                elif job_status.get("failed"):
                    # check for this last ( only when job is no longer active ) because our job is allowed to fail multiple times
                    # job_status will hold the number of times it has failed
                    self.stop_time = job_status['conditions'][0]['last_transition_time'].timestamp()
                    self.failed_container = self.__get_failed_container()
                    if self.failed_container:
                        logging.error(f"({self.name}) Instance failed during task: {self.failed_container['name']} with command {self.failed_container['args']}.")
                        logging.error(f"({self.name}) Logs from failed task: \n {self.failed_container['log']}")
                    raise RuntimeError(f"({self.name}) Instance failed!")

    def finalize(self):
        pass

    def get_start_time(self):
        return self.start_time

    def get_stop_time(self):
        return self.stop_time

    def get_status(self, log_status=False):
        s = api_request(self.batch_api.read_namespaced_job_status, self.inst_name, self.namespace)
        # Save the status if the job is no longer active
        job_status = s.get("status", dict())
        if log_status:
            logging.debug(f"({self.name}) Job Status: {job_status}")
        if job_status and job_status != "Failure":
            if self.start_time == 0 and job_status['start_time']:
                self.start_time = job_status['start_time'].timestamp()
            return job_status
        elif job_status == "Failure":
            logging.debug(f"({self.name}) Failure to get status. Reason: {s.get('message', '')}")
        return s

    def add_checkpoint(self, clear_output=True):
        pass

    def compute_cost(self):
        # calculate run time and convert to hours
        run_time = (self.get_stop_time() - self.get_start_time()) / 3600.0

        # get pricing
        compute_price = self.get_compute_price()
        storage_price = self.get_storage_price()

        # calculate totals
        total_compute_cost = run_time * compute_price
        total_storage_cost = run_time * storage_price

        return total_compute_cost + total_storage_cost

    def get_compute_price(self):
        if self.k8s_provider == 'EKS':
            pricing_url = f"https://banzaicloud.com/cloudinfo/api/v1/providers/amazon/services/eks/regions/{self.region}/products"
        else:
            pricing_url = f"https://banzaicloud.com/cloudinfo/api/v1/providers/google/services/gke/regions/{self.region}/products"

        products = requests.get(pricing_url).json()
        product_info = next((x for x in products['products'] if x['type'] == self.nodepool_info["inst_type"]), None)

        if product_info:
            return product_info['onDemandPrice'] if not self.preemptible else product_info['spotPrice'][0]['price']
        else:
            logging.warning(f"Unable to retrieve pricing info for the instance type: {self.nodepool_info['inst_type']}")
            return 0

    def get_storage_price(self):
        return self.storage_price

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

    def __create_job_def(self, rerun=False):
        # initialize the job def body
        self.inst_name = self.name
        if self.job_count > 1:
            self.inst_name = self.inst_name + '-' + str(self.job_count)
        if not rerun:
            self.job_count += 1
        job_def = client.V1Job(kind="Job")
        job_def.metadata = client.V1ObjectMeta(namespace=self.namespace, name=self.inst_name)

        # initialize job pieces
        self.job_containers = []
        volume_mounts = []
        volumes = []
        containers = []
        init_containers = []
        env_variables = []

        if not self.volume_name:
            # use the task name so it can be used across multiple jobs
            self.volume_name = self.name+'-pd'

        # build volume mounts
        volume_mounts = []
        volume_mounts.append(
            client.V1VolumeMount(
                mount_path=self.wrk_dir,
                name=self.volume_name
            )
        )

        cpu_request_max = self.nodepool_info['max_cpu'] - self.cpu_reserve
        mem_request_max = self.nodepool_info['max_mem'] - self.mem_reserve

        # define resource limits/requests
        resource_def = client.V1ResourceRequirements(
            limits={'cpu': cpu_request_max, 'memory': str(mem_request_max)+'G'},
            requests={'cpu': cpu_request_max*.8, 'memory': str(mem_request_max-1)+'G'}
        )

        # place the job in the appropriate node pool
        node_label_dict = {'poolName': str(self.node_label)}

        # build volumes
        volumes.append(
            client.V1Volume(
                name=self.volume_name,
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=self.pvc_name
                )
            )
        )

        # incorporate configured secrets
        if self.gcp_secret_configured:
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
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_GS_TYPE', value='google cloud storage'))
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_GS_SERVICE_ACCOUNT_FILE', value='$GOOGLE_APPLICATION_CREDENTIALS'))
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_GS_OBJECT_ACL', value='projectPrivate'))
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_GS_BUCKET_ACL', value='projectPrivate'))

        if self.aws_secret_configured:
            env_variables.append(client.V1EnvVar(name='AWS_ACCESS_KEY_ID', value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='cloud-conductor-config', key='aws_id'))))
            env_variables.append(client.V1EnvVar(name='AWS_SECRET_ACCESS_KEY', value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='cloud-conductor-config', key='aws_access'))))
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_S3_TYPE', value='s3'))
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_S3_ACCESS_KEY_ID', value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='cloud-conductor-config', key='aws_id'))))
            env_variables.append(client.V1EnvVar(name='RCLONE_CONFIG_S3_SECRET_ACCESS_KEY', value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='cloud-conductor-config', key='aws_access'))))

        storage_image = 'gcr.io/cloud-builders/gsutil'
        storage_tasks = ['mkdir_', 'grant_']
        entrypoint = ["/bin/sh", "-c"]

        for k, v in self.processes.items():
            # if the process is for storage (i.e. mkdir, etc.)
            if any(x in k for x in storage_tasks) or not v['docker_image']:
                container_image = storage_image
            else:
                container_image = v['docker_image']
                if v['docker_entrypoint'] is not None and v['original_cmd'].find(v['docker_entrypoint']) == -1:
                    v['original_cmd'] = v['docker_entrypoint'] + ' ' + v['original_cmd']
                if 'rclone' in container_image:
                    v['original_cmd'] = v['original_cmd'].replace("copyto", "copy")
            args = v['original_cmd']
            if not isinstance(args, list):
                args = [v['original_cmd'].replace("sudo ", "")]
            args = " && ".join(args)
            args = args.replace("\n", " ")

            if "gsutil" in args:
                args = "gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS && " + args

            # format the container name and roll call to logging
            container_name = k.replace("_", "-").replace(".", "-").lower()
            formatted_container_name = container_name[:57] + '-' + Platform.generate_unique_id(id_len=5)

            # args = f">&2 echo STARTING TASK {container_name} && " + args

            containers.append(client.V1Container(
                    # lifecycle=client.V1Lifecycle(post_start=post_start_handler),
                    image=container_image,
                    command=entrypoint,
                    args=[args],
                    name=formatted_container_name,
                    volume_mounts=volume_mounts,
                    env=env_variables,
                    resources=resource_def,
                    image_pull_policy='IfNotPresent'
                )
            )

        job_spec = dict(
            backoff_limit=self.default_num_cmd_retries
        )

        self.job_containers = containers

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
        job_labels = {}
        job_labels[self.inst_name] = 'CC-Job'
        job_template.metadata = client.V1ObjectMeta(labels=job_labels)
        job_template.spec = client.V1PodSpec(
            init_containers=init_containers,
            containers=containers,
            volumes=volumes,
            restart_policy='Never',
            termination_grace_period_seconds=self.termination_seconds,
            node_selector=node_label_dict
        )

        job_def.spec = client.V1JobSpec(template=job_template, **job_spec)

        return job_def

    def __get_failed_container(self):
        """ Returns the logs for the specified container name in the currently running job """
        response = api_request(self.core_api.list_namespaced_pod, namespace=self.namespace, label_selector=self.inst_name, watch=False, pretty='true')
        if response.get("items"):
            pod = response["items"][len(response["items"])-1]
            pod_name = pod.get("metadata", {}).get("name", '')
            module_log = ''
            init_container_statuses = pod['status']['init_container_statuses']
            container_index = 0
            for status in init_container_statuses:
                if not status['ready']:
                    failed_container = pod['spec']['init_containers'][container_index]
                    failed_container['log'] = api_request(self.core_api.read_namespaced_pod_log, pod_name, self.namespace, container=status['name'], follow=False, pretty='true')
                    failed_container['module_log'] = module_log
                    return failed_container
                else:
                    module_log += '\n' + api_request(self.core_api.read_namespaced_pod_log, pod_name, self.namespace, container=status['name'], follow=False, pretty='true')
                container_index += 1
            container_statuses = pod['status']['container_statuses']
            container_index = 0
            for status in container_statuses:
                if not status['ready']:
                    failed_container = pod['spec']['containers'][container_index]
                    failed_container['log'] = api_request(self.core_api.read_namespaced_pod_log, pod_name, self.namespace, container=status['name'], follow=False, pretty='true')
                    failed_container['module_log'] = module_log
                    return failed_container
                else:
                    module_log += '\n' + api_request(self.core_api.read_namespaced_pod_log, pod_name, self.namespace, container=status['name'], follow=False, pretty='true')
                container_index += 1
        logging.warning(f"Failed to retrieve failed container in job {self.inst_name}")
        return None

    def __get_container_log(self, container_name):
        """ Returns the logs for the specified container name in the currently running job """
        response = api_request(self.core_api.list_namespaced_pod, namespace=self.namespace, label_selector=self.inst_name, watch=False, pretty='true')
        if response.get("error"):
            logging.warning(f"Failed to retrieve logs for container {container_name} in job {self.inst_name}")
            return ""
        # Loop through all the pods to find the pods for the job
        for pod in response.get("items"):
            if pod.get("metadata", {}).get("labels", {}).get("job-name") == self.inst_name:
                pod_name = pod.get("metadata", {}).get("name", '')
                if pod_name:
                    response = api_request(self.core_api.read_namespaced_pod_log, pod_name, self.namespace, container=container_name, follow=False, pretty='true')
                    return response
        logging.warning(f"Failed to retrieve logs for container {container_name} in job {self.inst_name}")
        return ""
