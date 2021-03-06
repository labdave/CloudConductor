import logging
import requests
import time
import os
import ast
import re
import tempfile
import yaml
import json

from System.Platform.Kubernetes import KubernetesStatusManager
from System.Platform.Instance import Instance
from System.Platform import Platform, Process
from System.Platform.Kubernetes.utils import api_request, get_api_sleep
from collections import OrderedDict

from kubernetes import client, config


class KubernetesJob(Instance):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        super(KubernetesJob, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        self.stoppable = False
        self.job_count = 1
        self.inst_name = name
        self.job_containers = []
        self.job_names = {}

        self.task_prefix_filter = ['mkdir_wrk_dir', 'docker_pull']

        self.namespace = kwargs.pop("namespace", "cloud-conductor")
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

        # Get script task
        self.script_task = kwargs.pop("script_task", None)

        # Obtain location specific information
        self.region = kwargs.pop("region")
        self.zone = kwargs.pop("zone")

        # Get API clients
        self.batch_api = kwargs.pop("batch_api")
        self.core_api = kwargs.pop("core_api")
        self.status_manager = kwargs.pop("status_manager")

        # Get platform/cluster specific data
        self.storage_price = kwargs.pop("storage_price")
        self.k8s_provider = kwargs.pop("provider")

        self.preemptible = kwargs.pop("preemptible", False)
        self.final_output_dir = kwargs.pop("final_output_dir", "")

        all_pools = kwargs.pop("pools", [])
        self.extra_persistent_volumes = kwargs.pop("persistent_volumes", [])

        self.node_pools = [x for x in all_pools if not x["preemptible"]]
        self.preemptible_node_pools = [x for x in all_pools if x["preemptible"]]

        # amount to subtract from max cpu / max mem for monitoring or general k8s overhead pods
        self.cpu_reserve = kwargs.pop("cpu_reserve", 0)
        self.mem_reserve = kwargs.pop("mem_reserve", 0)
        self.gcp_secret_configured = kwargs.pop("gcp_secret_configured", 0)
        self.aws_secret_configured = kwargs.pop("aws_secret_configured", 0)

        self.node_label, self.nodepool_info = self.get_nodepool_info()

        if not self.script_task:
            self.status_manager.check_monitoring_status()

        self.run('mkdir_tmp_dir', 'sudo mkdir -p /data/tmp')

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
        if "!LOG" in cmd or (self.script_task and self.script_task.post_processing_required and job_name == self.script_task.task_id):

            # Generate name of log file
            log_file = f"{job_name}.log"
            if self.wrk_log_dir is not None:
                log_file = os.path.join(self.wrk_log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = f" |& tee -a /dev/null"
            log_cmd_stdout  = f" | tee -a {log_file}"
            log_cmd_stderr  = f" > >(tee -a /dev/null) 2> >(tee -a {log_file} >&2)"
            log_cmd_all     = f" |& tee -a {log_file}"

            if "!LOG2!" in cmd and ("> /" in cmd or ">/" in cmd):
                log_cmd_stderr = f" 2> >(tee -a {log_file} >&2)"

            # Replacing the placeholders with the logging pipes
            if self.script_task and self.script_task.post_processing_required and job_name == self.script_task.task_id:
                if "!LOG" in cmd:
                    cmd = re.sub("!LOG[0-9]!", log_cmd_stdout, cmd)
                else:
                    cmd += f" {log_cmd_stdout}"
            else:
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
        final_output = self.final_output_dir.replace("//", "") + self.script_task.task_id.replace("-", "_")
        submodule_arg = "" if not self.script_task.submodule_name else ' -sm '+self.script_task.submodule_name
        if self.script_task and self.script_task.update_input_required and job_name == self.script_task.task_id:
            # if the input for the given task needs to be updated because a parent's output will update
            update_command_task = {
                "original_cmd": 'cd /CloudConductor && git pull && git checkout "${GIT_COMMIT}" && python3 ModuleRunner -m '+self.script_task.module_name+submodule_arg+' -task '+ self.script_task.task_id +' -inputs "${MODULE_INPUTS}" -c '+self.wrk_out_dir+'/command.txt -o '+self.wrk_out_dir+'/output_values.json',
                "num_retries": self.default_num_cmd_retries,
                "docker_image": "davelabhub/cloudconductor",
                "docker_entrypoint": None,
            }
            self.processes["update_command_task"] = update_command_task
            save_command = {
                "original_cmd": f"rclone copyto {self.wrk_out_dir}/command.txt {final_output}/command.txt",
                "num_retries": self.default_num_cmd_retries,
                "docker_image": "rclone/rclone:1.52",
                "docker_entrypoint": None,
            }
            self.processes["save_command"] = save_command
            save_output_values = {
                "original_cmd": f"rclone copyto {self.wrk_out_dir}/output_values.json {final_output}/output_values.json",
                "num_retries": self.default_num_cmd_retries,
                "docker_image": "rclone/rclone:1.52",
                "docker_entrypoint": None,
            }
            self.processes["save_output_json"] = save_output_values
            task["original_cmd"] = f"cat {self.wrk_out_dir}/command.txt | bash"
            self.processes[job_name] = task
        if self.script_task and self.script_task.post_processing_required and job_name == self.script_task.task_id:
            self.processes[job_name] = task
            process_output_task = {
                "original_cmd": 'cd /CloudConductor && git pull && git checkout "${GIT_COMMIT}" && python3 ModuleRunner -m '+self.script_task.module_name+submodule_arg+' -task '+ self.script_task.task_id +' -inputs "${MODULE_INPUTS}" -ro '+log_file+' -o '+self.wrk_out_dir+'/output_values.json',
                "num_retries": self.default_num_cmd_retries,
                "docker_image": "davelabhub/cloudconductor",
                "docker_entrypoint": None,
            }
            self.processes["process_output"] = process_output_task
            save_output_values = {
                "original_cmd": f"rclone copyto {self.wrk_out_dir}/output_values.json {final_output}/output_values.json",
                "num_retries": self.default_num_cmd_retries,
                "docker_image": "rclone/rclone:1.52",
                "docker_entrypoint": None,
            }
            self.processes["save_output_json"] = save_output_values
        else:
            self.processes[job_name] = task

    def destroy(self):
        if not self.script_task:
            self.__cleanup_job()

        if self.start_time > 0 and self.stop_time == 0:
            self.stop_time = time.time()
        # stop monitoring the job
        self.monitoring = False
        
        if not self.script_task:
            self.__cleanup_volume_claim()

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
            logging.debug(f"{self.name}) Cleaning up prerequisite job resources.")
            self.__cleanup_job()
            logging.debug(f"({self.name}) Starting creation of Kubernetes followup job.")

        # create the persistent volume claim for the job if one doesn't already exist
        if not self.pvc_name and not self.script_task:
            self.__create_volume_claim()
        elif self.script_task:
            self.script_task.storage_request = self.disk_space

        # create the job definition
        self.job_def = self.__create_job_def(return_last_task_log)

        # launch and monitor the job if we're not just generating a script
        if not self.script_task:
            self.__launch_job()

            # begin monitoring job for completion/failure
            self.monitoring = True
            while self.monitoring:
                time.sleep(5)
                job_status = self.get_status(log_status=True)
                if job_status and (job_status.succeeded or (job_status.failed and job_status.failed >= self.default_num_cmd_retries and not job_status.active)):
                    self.monitoring = False
                    if job_status.succeeded:
                        self.stop_time = job_status.completion_time.timestamp()
                        logging.info(f"({self.name}) Process complete!")
                        if return_last_task_log:
                            logging.debug("Returning logs from last process.")
                            logs = self.__get_container_log(self.job_containers[len(self.job_containers)-1].name)
                            return logs, ''
                    elif job_status.failed and job_status.failed >= self.default_num_cmd_retries and not job_status.active:
                        logging.warning(f"{self.name}) Job marked as failed. Status response:\n{str(job_status)}")
                        # check for this last ( only when job is no longer active ) because our job is allowed to fail multiple times
                        # job_status will hold the number of times it has failed
                        if job_status and job_status.conditions:
                            self.stop_time = job_status.conditions[0].last_transition_time.timestamp()
                        else:
                            self.stop_time = time.time()
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

    def get_status(self, retries=0, log_status=False, force_refresh=False):
        job_status, preemptions = self.status_manager.get_job_status(self.inst_name, self.name)
        if preemptions >= 2 and self.preemptible:
            # implement change from preemptible to standard
            logging.info(f"({self.name}) Job was preempted 2 times. Switching to standard node pool.")
            # recreate job with different node selector
            self.__rebuild_job(preemptible=False)
            return None
        # Save the status if the job is no longer active
        if job_status and job_status != "Failure":
            if self.start_time == 0 and job_status.start_time:
                self.start_time = job_status.start_time.timestamp()
            return job_status
        elif job_status == "Failure":
            reason = job_status.message
            if 'rpc error' in reason or 'Timeout: ' in reason and retries < 5:
                logging.warning(f"({self.name}) Request issue when getting status. We'll try again.")
                time.sleep(30)
                return self.get_status(retries + 1, log_status)
            else:
                logging.error(f"({self.name}) Failure to get status. Reason: {reason}")
        return job_status

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
        # initialize product_info to None in case API call fails
        product_info = None
        # skip price calculations
        """
        if self.k8s_provider == 'EKS':
            pricing_url = f"https://banzaicloud.com/cloudinfo/api/v1/providers/amazon/services/eks/regions/{self.region}/products"
        else:
            pricing_url = f"https://banzaicloud.com/cloudinfo/api/v1/providers/google/services/gke/regions/{self.region}/products"

        pricing_received = False
        request_count = 0
        while not pricing_received and request_count < 5:
            try:
                request_count += 1
                products = requests.get(pricing_url).json()
                product_info = next((x for x in products['products'] if x['type'] == self.nodepool_info["inst_type"]), None)
                pricing_received = True
            except Exception as e:
                if request_count > 5:
                    raise RuntimeError(f"({self.name}) Failure to get pricing info. Reason: {str(e)}")
                logging.warning(f"({self.name}) Exception when retrieving pricing info. We will retry the request ({request_count}/5).\nReason: {str(e)}")
                time.sleep(30)
        """
        if product_info:
            return product_info['onDemandPrice'] if not self.preemptible else product_info['spotPrice'][0]['price']
        else:
            logging.warning(f"Unable to retrieve pricing info for the instance type: {self.nodepool_info['inst_type']}")
            return 0

    def get_storage_price(self):
        return self.storage_price

    def __rebuild_job(self, preemptible=False):
        self.__cleanup_job()
        self.preemptible = preemptible
        self.node_label, self.nodepool_info = self.get_nodepool_info()
        node_label_dict = {'poolName': str(self.node_label)}
        job_exists = True
        while job_exists:
            response = api_request(self.batch_api.read_namespaced_job, name=self.inst_name, namespace=self.namespace)
            if response and 'not found' in response.get('message', ''):
                job_exists = False
                break
            time.sleep(30)

        self.job_def.spec.template.spec.node_selector = node_label_dict
        self.__launch_job(log_yaml=False)

    def __launch_job(self, log_yaml=True):
        try:
            creation_response = api_request(self.batch_api.create_namespaced_job, self.namespace, self.job_def)
            creation_status = creation_response.get("status", None)
            if creation_status and isinstance(creation_status, dict) and creation_status != 'Failure':
                if log_yaml:
                    job_yaml = yaml.dump(creation_response).split("\n")
                    stripped_yaml = []
                    for line in job_yaml:
                        if ": null" not in line and "status:" not in line and "self_link" :
                            stripped_yaml.append(line)
                    job_yaml = "\n".join(stripped_yaml)
                    logging.debug(f"({self.name}) KUBERENETES JOB YAML : \n\n{job_yaml}")
                    logging.debug(f"({self.name}) Kubernetes job successfully created! Begin monitoring.")
            else:
                raise RuntimeError(f"({self.name}) Failure to create the job on the cluster")
        except Exception as e:
            raise RuntimeError(f"({self.name}) Failure to create the job on the cluster Reason: {str(e)}")

    def __create_volume_claim(self):
        # create the persistent volume claim for the task
        self.pvc_name = self.name+'-vc'
        pvc_meta = client.V1ObjectMeta(name=self.pvc_name, namespace=self.namespace)
        pvc_resources = client.V1ResourceRequirements(requests={'storage': str(self.disk_space)+'Gi'})
        pvc_spec = client.V1PersistentVolumeClaimSpec(access_modes=['ReadWriteOnce'], resources=pvc_resources, storage_class_name='standard')
        self.task_pvc = client.V1PersistentVolumeClaim(metadata=pvc_meta, spec=pvc_spec)

        for i in range(10):
            try:
                pvc_response = api_request(self.core_api.create_namespaced_persistent_volume_claim, self.namespace, self.task_pvc)
            except Exception as e:
                raise RuntimeError(f"({self.name}) Failure to create the Persistent Volume Claim on the cluster. Reason: {str(e)}")

            # Save the status if the job is no longer active
            pvc_status = pvc_response.get("status", None)
            if pvc_status and isinstance(pvc_status, dict):
                logging.debug(f"({self.name}) Persistent Volume Claim created.")
                break
            else:
                if 'Connection aborted' in str(pvc_response) or 'Connection reset' in str(pvc_response):
                    sleep_time = get_api_sleep(i+1)
                    logging.debug(f"({self.name}) Connection issue when creating Persistent Volume Claim. Sleeping for: {sleep_time}")
                    time.sleep(sleep_time)
                    continue
                else:
                    raise RuntimeError(f"({self.name}) Failure to create a Persistent Volume Claim on the cluster. Response: {str(pvc_response)}")

    def __create_job_def(self, post_processing_required=False):
        # initialize the job def body
        self.inst_name = self.name
        if self.job_count > 1:
            self.inst_name = self.inst_name + '-' + str(self.job_count)
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

        # update script task with job info
        if self.script_task:
            self.script_task.cpu_request = cpu_request_max*.8
            self.script_task.cpu_max = cpu_request_max
            self.script_task.memory_request = mem_request_max-1
            self.script_task.memory_max = mem_request_max
            self.script_task.instance_name = self.inst_name
            self.script_task.force_standard = not self.preemptible
            self.script_task.pool_name = str(self.node_label)
            self.script_task.instance_type = str(self.nodepool_info["inst_type"])

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

        # incorporate configured persistent volumes if associated with the current task
        if self.extra_persistent_volumes:
            for pv in self.extra_persistent_volumes:
                if pv['task_prefix'] in self.name:
                    # need to add the extra persistent volume
                    volume_mounts.append(
                        client.V1VolumeMount(
                            mount_path=pv["path"],
                            name=pv['volume_name'],
                            read_only=pv['read_only']
                        )
                    )
                    volumes.append(
                        client.V1Volume(
                            name=pv['volume_name'],
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=pv['pvc_name']
                            )
                        )
                    )

                    # specify volumes for script task
                    if self.script_task:
                        self.script_task.extra_volumes.append({"path": pv["path"], "name": pv["volume_name"], "read_only": pv["read_only"], "claim_name": pv["pvc_name"]})

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
        container_name_list = []

        for k, v in self.processes.items():
            # if the process is for storage (i.e. mkdir, etc.)
            entrypoint = ["/bin/bash", "-c"]
            if any(x in k for x in storage_tasks) or not v['docker_image']:
                container_image = storage_image
            else:
                container_image = v['docker_image']
                if v['docker_entrypoint'] is not None and v['original_cmd'].find(v['docker_entrypoint']) == -1:
                    v['original_cmd'] = v['docker_entrypoint'] + ' ' + v['original_cmd']
                if 'rclone' in container_image:
                    v['original_cmd'] = v['original_cmd'].replace("|&", "2>&1 |")
                    entrypoint = ["/bin/sh", "-c"]
            args = v['original_cmd']
            if not isinstance(args, list):
                args = [v['original_cmd'].replace("sudo ", "")]
            args = " && ".join(args)
            args = args.replace("\n", " ")
            args = args.replace("java.io.tmpdir=/tmp/", "java.io.tmpdir=/data/tmp/")

            if "awk " in args:
                args = re.sub("'\"'\"'", "'", args)

            if "gsutil" in args:
                args = "gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS && " + args

            logging.debug(f"({self.name}) Command for task {k} is : {args}")

            # format the container name and roll call to logging
            container_name = k.replace("_", "-").replace(".", "-").lower()
            formatted_container_name = container_name[:57] + '-' + Platform.generate_unique_id(id_len=5)
            while formatted_container_name in container_name_list:
                # make sure all container names are unique
                formatted_container_name = container_name[:57] + '-' + Platform.generate_unique_id(id_len=5)
            container_name_list.append(formatted_container_name)

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

            if self.script_task and container_name not in self.script_task.commands:
                self.script_task.commands[container_name] = ({"name": formatted_container_name, "docker_image": container_image, "entrypoint": entrypoint, "args": [args]})

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
        # add annotation to prevent autoscaler from killing nodes running jobs
        annotations = {'cluster-autoscaler.kubernetes.io/safe-to-evict': 'false'}
        job_template.metadata = client.V1ObjectMeta(labels=job_labels, annotations=annotations)
        job_template.spec = client.V1PodSpec(
            init_containers=init_containers,
            containers=containers,
            volumes=volumes,
            restart_policy='Never',
            termination_grace_period_seconds=self.termination_seconds,
            node_selector=node_label_dict
        )

        job_def.spec = client.V1JobSpec(template=job_template, **job_spec)

        if self.script_task:
            self.script_task.num_retries = self.default_num_cmd_retries
            for k, v in job_labels.items():
                self.script_task.labels.append({"key": k, "value": v})
            for k, v in annotations.items():
                self.script_task.annotations.append({"key": k, "value": v})

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
            if init_container_statuses:
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
            if container_statuses:
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

    def __cleanup_job(self):
        # Destroy the job
        if self.job_names:
            for job_name in self.job_names:
                delete_response = api_request(self.batch_api.delete_namespaced_job, job_name, self.namespace)

                # Save the status if the job is no longer active
                delete_status = delete_response.get("status", None)
                if delete_status and delete_status == 'Failure':
                    if 'not found' not in delete_response.get('message', ''):
                        logging.warning(f"({self.name}) Failed to destroy Kubernetes Job. Message: {delete_response.get('message', '')}")
                elif delete_status and not isinstance(delete_status, dict):
                    delete_status = ast.literal_eval(delete_status)
                else:
                    logging.debug(f"({job_name}) Kubernetes job successfully destroyed.")
                # Destroy all pods associated with the job as well
                self.__cleanup_pods(job_name)

    def __cleanup_volume_claim(self):
        if self.pvc_name:
            # Destroy the persistent volume claim
            pvc_response = api_request(self.core_api.delete_namespaced_persistent_volume_claim, self.pvc_name, self.namespace)

            # Save the status if the job is no longer active
            pvc_status = pvc_response.get("status", None)
            if pvc_status and pvc_status == 'Failure':
                if 'not found' not in pvc_response.get('message', ''):
                    logging.warning(f"({self.name}) Failed to destroy Persistent Volume Claim. Message: {pvc_response.get('message', '')}")
            elif pvc_status and not isinstance(pvc_status, dict):
                pvc_status = ast.literal_eval(pvc_status)
            else:
                logging.debug(f"({self.name}) Persistent Volume Claim successfully destroyed.")

    def __cleanup_pods(self, job_name):
        response = api_request(self.core_api.list_namespaced_pod, namespace=self.namespace, label_selector=job_name, watch=False, pretty='true')
        if response.get("error"):
            logging.warning(f"Failed to delete pods for job {job_name}")
            return ""
        # Loop through all the pods to delete them from the cluster
        for pod in response.get("items"):
            if pod.get("metadata", {}).get("labels", {}).get("job-name") == job_name:
                pod_name = pod.get("metadata", {}).get("name", '')
                if pod_name:
                    response = api_request(self.core_api.delete_namespaced_pod, pod_name, self.namespace)
                    logging.debug(f"({self.name}) Pod removed successfully")
