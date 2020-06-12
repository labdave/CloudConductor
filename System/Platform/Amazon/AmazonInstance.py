import logging
import os
import subprocess as sp
import boto3
import json
import statistics
import time
import inspect

from requests.exceptions import BaseHTTPError, HTTPError
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

from pkg_resources import resource_filename

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.common.exceptions import RateLimitReachedError

from System.Platform import CloudInstance
from System.Platform import Process

from System.Platform.Amazon.EnhancedEC2NodeDriver import EnhancedEC2NodeDriver


class AmazonInstance(CloudInstance):

    def __init__(self, name, nr_cpus, mem, disk_space, disk_image, **kwargs):

        super(AmazonInstance, self).__init__(name, nr_cpus, mem, disk_space, disk_image, **kwargs)

        # Create libcloud driver
        # driver_class = get_driver(Provider.EC2)
        # self.driver = driver_class(self.identity, self.secret, region=self.region)
        self.driver = EnhancedEC2NodeDriver(self.identity, self.secret, region=self.region)

        self.instance_type = None
        self.is_preemptible = False
        self.instance_type_list = None

        # Obtain the Google JSON path
        self.google_json = kwargs.get("google_json", None)

        # Initialize the node variable
        self.node = None

        self.boto_config = Config(
            retries = dict(
                max_attempts = 20,
                mode = 'adaptive'
            )
        )

        self.instance_type_list = self.platform.get_instance_type_list()

        # Set additional SSH options
        self.set_ssh_option("SendEnv", "AWS_ACCESS_KEY_ID")
        self.set_ssh_option("SendEnv", "AWS_SECRET_ACCESS_KEY")

    def get_instance_size(self):
        '''Select optimal instance type for provided region, number of cpus, and memory allocation'''
        selected_instance_type = None

        for instance_type in self.instance_type_list:
            # get number of cpus in instance type
            type_cpus = instance_type['VCpuInfo']['DefaultVCpus']
            # get amount of mem in instance type in MiB
            type_mem = instance_type['MemoryInfo']['SizeInMiB']
            if type_cpus >= self.nr_cpus and type_mem >= self.mem * 1024:
                if not selected_instance_type:
                    selected_instance_type = instance_type
                else:
                    # select the cheaper of the two instance types
                    if self.is_preemptible and hasattr(instance_type, 'spotPrice') and instance_type['spotPrice'] < selected_instance_type['spotPrice']:
                        selected_instance_type = instance_type
                    elif instance_type['price'] < selected_instance_type['price']:
                        selected_instance_type = instance_type

        return selected_instance_type

    def create_instance(self):

        # Generate NodeSize for instance
        self.instance_type = self.get_instance_size()
        size_name = self.instance_type['InstanceType']
        logging.info(f"({self.name}) SELECTED AWS INSTANCE TYPE: {self.instance_type}")
        node_size = [size for size in self.driver.list_sizes() if size.id == size_name][0]

        device_mappings = [
            {
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'VolumeSize': self.disk_space,
                    'VolumeType': 'standard'
                }
            }
        ]

        # Create instance
        if self.name.startswith("helper-"):
            # don't want helper instances to be preemptible
            self.is_preemptible = False

        node = None
        if self.is_preemptible:
            node = self.__create_spot_instance(node_size, device_mappings)
        else:
            node = self.__create_on_demand_instance(node_size, device_mappings)

        if not node:
            raise RuntimeError(f"({self.name}) There was an issue with creating the new instance.")

        # Get list of running nodes
        running_nodes = self.__aws_request(self.driver.wait_until_running, [node], wait_period=20)

        # Obtain our node
        self.node, external_IP = [(n, ext_IP[0]) for n, ext_IP in running_nodes if n.uuid == node.uuid][0]

        # Return the external IP from node
        return external_IP

    def post_startup(self):
        # Copy Google key to instance and authenticate
        if self.google_json is not None:

            # Transfer key to instance
            cmd = f'scp -i {self.ssh_private_key} -o CheckHostIP=no -o StrictHostKeyChecking=no {self.google_json} ' \
                  f'{self.ssh_connection_user}@{self.external_IP}:GCP.json'

            Process.run_local_cmd(cmd, err_msg="Could not authenticate Google SDK on instance!")

            # Activate service account
            cmd = f'gcloud auth activate-service-account --key-file /home/{self.ssh_connection_user}/GCP.json'
            self.run("authenticate_google", cmd)
            self.wait_process("authenticate_google")

            # Setup Google SA path
            os.environ["GOOGLE_SA"] = f"/home/{self.ssh_connection_user}/GCP.json"
            self.set_ssh_option('SendEnv', 'GOOGLE_SA')

        else:
            logging.warning("(%s) Google JSON key not provided! "
                            "Instance will not be able to access GCP buckets!" % self.name)

        # Authenticate AWS CLI
        cmd = f'aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID \
                && aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY \
                && aws configure set default.region {self.region} \
                && aws configure set default.output json'
        self.run("aws_configure", cmd)
        self.wait_process("aws_configure")

    def destroy_instance(self):
        if self.is_preemptible:
            self.__cancel_spot_instance_request()
        self.__aws_request(self.driver.destroy_node, self.node)

    def start_instance(self):
        instance_started = False

        counter = 10
        while not instance_started and counter > 0:
            try:
                logging.info(f"Attempting to restart instance {self.name}")
                instance_started = self.__aws_request(self.driver.ex_start_node, self.node)
            except Exception as e:
                exception_string = str(e)
                if 'IncorrectInstanceState' in exception_string:
                    logging.info(f"({self.name}) Instance is in the incorrect state to be started.")
                    status = self.get_status(log_status=True)
                    logging.info(f"({self.name}) Instance state = {status.upper()}")
            if not instance_started:
                logging.warning(f"({self.name}) Failed to restart instance, waiting 30 seconds before retrying")
                # wait 30 seconds before trying to restart again
                time.sleep(30)
                counter -= 1

        if not instance_started:
            raise RuntimeError("(%s) Instance was unable to restart" % self.name)

        # Initializing the cycle count
        cycle_count = 0
        ready = False

        # Waiting for 5 minutes for instance to be SSH-able
        while cycle_count < 30:
            if self.get_status() == CloudInstance.AVAILABLE:
                ready = True
                break

            # Wait for 10 seconds before checking the status again
            time.sleep(self.get_api_sleep(cycle_count+1))

            # Increment the cycle count
            cycle_count += 1

        if not ready:
            raise RuntimeError("(%s) Instance was unable to restart" % self.name)

        return self.node.public_ips[0]

    def stop_instance(self):
        self.__aws_request(self.driver.ex_stop_node, self.node)

    def get_status(self, log_status=False):

        if self.node is None:
            return CloudInstance.OFF

        node_list = self.__aws_request(self.driver.list_nodes, ex_node_ids=[self.node.id])

        if not node_list or len(node_list) == 0:
            return CloudInstance.OFF

        self.node = node_list[0]

        # Define mapping between the cloud status and the current class status
        status_map = {
            "pending":          CloudInstance.CREATING,
            'running':          CloudInstance.AVAILABLE,
            'shutting-down':    CloudInstance.DESTROYING,
            'stopping':         CloudInstance.DESTROYING,
            'terminated':       CloudInstance.TERMINATED,
            'stopped':          CloudInstance.OFF
        }

        if log_status:
            logging.debug(f"({self.name}) Current status is: {self.node.extra['status']}")

        return status_map[self.node.extra["status"]]

    def get_compute_price(self):
        if self.instance_type:
            if self.is_preemptible and self.instance_type["spotPrice"]:
                return self.instance_type["spotPrice"]
            elif self.instance_type["price"]:
                return self.instance_type["price"]
        return 0

    def get_storage_price(self):
        if self.instance_type and self.instance_type["storagePrice"]:
            return self.instance_type["storagePrice"]
        return 0

    def generate_docker_env(self):
        env_vars = [
            "RCLONE_CONFIG_GS_TYPE='google cloud storage'",
            "RCLONE_CONFIG_GS_SERVICE_ACCOUNT_FILE=$GOOGLE_SA",
            "RCLONE_CONFIG_S3_TYPE='s3'",
            "RCLONE_CONFIG_S3_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID",
            "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
        ]

        return " ".join([f"-e {e}" for e in env_vars])

    def list_nodes(self, instance_type=None):
        inst_type_filter = None if not instance_type else {'instance-type': instance_type}
        node_list = self.__aws_request(self.driver.list_nodes, ex_filters=inst_type_filter)
        return node_list

    def __create_spot_instance(self, node_size, device_mappings):
        try:
            logging.info(f"({self.name}) Attempting to create a spot instance of type: {self.instance_type['InstanceType']}")
            node = self.__aws_request(self.driver.create_node, name=self.name,
                                            image=self.disk_image,
                                            size=node_size,
                                            ex_keyname=self.platform.get_ssh_key_pair(),
                                            ex_security_groups=[self.platform.get_security_group()],
                                            ex_blockdevicemappings=device_mappings,
                                            ex_spot_market=True,
                                            ex_spot_price=self.instance_type['price'],
                                            interruption_behavior='stop',
                                            ex_terminate_on_shutdown=False)
            return node
        except Exception as e:
            exception_string = str(e)
            if 'alreadyExists' in exception_string:
                logging.warning(f"({self.name}) Instance already exists. Getting status...")
                self.get_status(log_status=True)
                return self.node

            logging.debug(f"({self.name}) Failed to create a spot instance of type: {self.instance_type['InstanceType']}")
            logging.debug(f"({self.name}) There was an issue when creating a spot instance: {exception_string}")
            if 'MaxSpotInstanceCountExceeded' in exception_string or 'InsufficientInstanceCapacity' in exception_string or 'InstanceLimitExceeded' in exception_string:
                logging.info(f"({self.name}) Changing from spot instance to on-demand because we hit our limit of spot instances!")
                self.is_preemptible = False
                node = self.__create_on_demand_instance(node_size, device_mappings)
                return node
            else:
                return None

    def __create_on_demand_instance(self, node_size, device_mappings):
        try:
            logging.info(f"({self.name}) Attempting to create an on demand instance of type: {self.instance_type['InstanceType']}")
            node = self.__aws_request(self.driver.create_node, name=self.name,
                                            image=self.disk_image,
                                            size=node_size,
                                            ex_keyname=self.platform.get_ssh_key_pair(),
                                            ex_security_groups=[self.platform.get_security_group()],
                                            ex_blockdevicemappings=device_mappings,
                                            ex_terminate_on_shutdown=False)
            return node
        except Exception as e:
            exception_string = str(e)
            if 'alreadyExists' in exception_string:
                logging.warning(f"({self.name}) Instance already exists. Getting status...")
                self.get_status(log_status=True)
                return self.node

            logging.info(f"({self.name}) Failed to create an on demand instance of type: {self.instance_type['InstanceType']}")
            logging.error(f"({self.name}) There was an issue when creating an on demand instance: {exception_string}")
            if 'InsufficientInstanceCapacity' in exception_string or 'InstanceLimitExceeded' in exception_string:
                instance_list = self.list_nodes(instance_type=self.instance_type['InstanceType'])
                logging.info(f"There are currently {str(len(instance_list))} instances of type {self.instance_type['InstanceType']}. Changing instance type")
                self.__filter_instance_type(self.instance_type['InstanceType'])
                self.instance_type = self.get_instance_size()
                size_name = self.instance_type['InstanceType']
                logging.info(f"({self.name}) NEWLY SELECTED AWS INSTANCE TYPE: {self.instance_type}")
                node_size = [size for size in self.driver.list_sizes() if size.id == size_name][0]
                return self.__create_on_demand_instance(node_size, device_mappings)
            else:
                return None

    def __aws_request(self, method, *args, **kwargs):
        """ Function for handling AWS requests and rate limit issues """
        # retry command up to 8 times
        for i in range(8):
            try:
                return method(*args, **kwargs)
            except Exception as e:
                if self.__handle_api_error(e, method, i+1):
                    continue
                raise RuntimeError(str(e))
        raise RuntimeError("Exceeded number of retries for function %s" % method.__name__)

    def __handle_api_error(self, e, method, count):
        exception_string = str(e)
        logging.debug(f"({self.name}) [AMAZONINSTANCE] Handling issues with api")
        logging.debug(f"({self.name}) Handling API exception: {exception_string}")
        if 'MaxSpotInstanceCountExceeded' in exception_string or 'InsufficientInstanceCapacity' in exception_string or 'InstanceLimitExceeded' in exception_string:
            logging.info(f"({self.name}) Maximum number of spot instances exceeded.")
            return False
        if 'RequestLimitExceeded' in exception_string or 'Rate limit exceeded' in exception_string or 'ThrottlingException' in exception_string or 'RequestResourceCountExceeded' in exception_string:
            sleep_time = self.get_api_sleep(count)
            logging.debug(f"({self.name}) Rate Limit Exceeded during request {method.__name__}. Sleeping for {sleep_time} seconds before retrying.")
            time.sleep(sleep_time)
            return True
        if 'Job did not complete in 180 seconds' in exception_string or 'Timed out' in exception_string:
            sleep_time = self.get_api_sleep(count)
            logging.debug(f"({self.name}) Libcloud command timed out sleeping for {sleep_time} seconds before retrying.")
            time.sleep(sleep_time)
            return True
        return False

    def __filter_instance_type(self, instance_type):
        if self.instance_type_list:
            for inst_type in self.instance_type_list:
                if inst_type['InstanceType'] == instance_type:
                    self.instance_type_list.remove(inst_type)
                    break

    def __cancel_spot_instance_request(self):
        client = boto3.client('ec2', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1', config=self.boto_config)
        spot_requests = self.__aws_request(client.describe_spot_instance_requests, Filters=[{'Name': 'instance-id', 'Values': [self.node.id]}], MaxResults=5)

        if spot_requests and spot_requests['SpotInstanceRequests']:
            request_id = spot_requests['SpotInstanceRequests'][0]['SpotInstanceRequestId']
            logging.debug(f"({self.name} Attempting to cancel spot instance {request_id}")
            response = client.cancel_spot_instance_requests(SpotInstanceRequestIds=[request_id])
            logging.debug(f"({self.name} Cancel spot instance response {str(response)}")
            if response and response['CancelledSpotInstanceRequests']:
                return response['CancelledSpotInstanceRequests'][0]['SpotInstanceRequestId'] == request_id
