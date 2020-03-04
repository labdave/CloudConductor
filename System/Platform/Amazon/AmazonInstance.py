import logging
import os
import subprocess as sp
import boto3
import json
import statistics
import time

from pkg_resources import resource_filename

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

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

        # Obtain the Google JSON path
        self.google_json = kwargs.get("google_json", None)

        # Initialize the node variable
        self.node = None

    def get_instance_size(self):
        '''Select optimal instance type for provided region, number of cpus, and memory allocation'''
        ec2 = boto3.client('ec2', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name=self.region)
        region_name = self.__get_region_name()
        describe_args = {'Filters': [
                            {'Name': 'vcpu-info.default-vcpus', 'Values': [str(self.nr_cpus)]},
                            {'Name': 'current-generation', 'Values': ['true']}
                        ]}
        selected_instance_type = {}
        while True:
            describe_result = ec2.describe_instance_types(**describe_args)
            for instance_type in describe_result['InstanceTypes']:
                # get number of cpus in instance type
                type_cpus = instance_type['VCpuInfo']['DefaultVCpus']
                # get amount of mem in instance type in MiB
                type_mem = instance_type['MemoryInfo']['SizeInMiB']
                # get network performance
                type_network_perf = instance_type['NetworkInfo']['NetworkPerformance']
                perf_number = ''.join([s for s in type_network_perf.split() if s.isdigit()])
                high_perf = False
                if perf_number:
                    high_perf = int(perf_number) >= 10
                else:
                    high_perf = type_network_perf == 'High'
                # make sure instance type has more resources than our minimum requirement
                if type_cpus >= self.nr_cpus and type_mem >= self.mem * 1024 and high_perf and 'm5a.' in instance_type['InstanceType']:
                    if not selected_instance_type:
                        selected_instance_type = instance_type
                        if self.is_preemptible:
                            selected_instance_type['price'] = self.get_spot_price(self.zone, selected_instance_type['InstanceType'])
                        else:
                            selected_instance_type['price'] = self.get_price(region_name, selected_instance_type['InstanceType'])
                    else:
                        # select the cheaper of the two instance types
                        if self.is_preemptible:
                            type_price = self.get_spot_price(self.zone, instance_type['InstanceType'])
                        else:
                            type_price = self.get_price(region_name, instance_type['InstanceType'])
                        if type_price < selected_instance_type['price']:
                            selected_instance_type = instance_type
                            selected_instance_type['price'] = type_price
            if 'NextToken' not in describe_result:
                break
            describe_args['NextToken'] = describe_result['NextToken']
        if selected_instance_type:
            selected_instance_type['storage_price'] = self.get_ebs_price(region_name)
            return selected_instance_type
        else:
            return None

    def create_instance(self):

        # Generate NodeSize for instance
        self.instance_type = self.get_instance_size()
        size_name = self.instance_type['InstanceType']
        logging.info(f"SELECTED AWS INSTANCE TYPE: {self.instance_type}")
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

        if self.is_preemptible:
            node = self.driver.create_node(name=self.name,
                                            image=self.disk_image,
                                            size=node_size,
                                            ex_keyname=self.platform.get_ssh_key_pair(),
                                            ex_security_groups=[self.platform.get_security_group()],
                                            ex_blockdevicemappings=device_mappings,
                                            ex_spot_market=True,
                                            ex_spot_price=self.instance_type['price'],
                                            interruption_behavior='stop',
                                            ex_terminate_on_shutdown=False)
        else:
            node = self.driver.create_node(name=self.name,
                                            image=self.disk_image,
                                            size=node_size,
                                            ex_keyname=self.platform.get_ssh_key_pair(),
                                            ex_security_groups=[self.platform.get_security_group()],
                                            ex_blockdevicemappings=device_mappings,
                                            ex_terminate_on_shutdown=False)

        # Get list of running nodes
        running_nodes = self.driver.wait_until_running([node])

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

        else:
            logging.warning("(%s) Google JSON key not provided! "
                            "Instance will not be able to access GCP buckets!" % self.name)

    def destroy_instance(self):
        if self.is_preemptible:
            self.cancel_spot_instance_request()
            self.driver.destroy_node(self.node)
        else:
            self.driver.destroy_node(self.node)

    def start_instance(self):
        instance_started = False

        counter = 10
        while not instance_started and counter > 0:
            try:
                instance_started = self.driver.ex_start_node(self.node)
            except:
                # we don't care if it fails, we'll retry the attempt
                pass
            if not instance_started:
                logging.debug("(%s) Failed to restart instance, waiting 30 seconds before retrying" % self.name)
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
            time.sleep(10)

            # Increment the cycle count
            cycle_count += 1

        if not ready:
            raise RuntimeError("(%s) Instance was unable to restart" % self.name)

        return self.node.public_ips[0]

    def stop_instance(self):
        self.driver.stop_node(self.node)

    def run(self, job_name, cmd, num_retries=None, docker_image=None):
        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = f"{job_name}.log"
            if self.wrk_log_dir is not None:
                log_file = os.path.join(self.wrk_log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1 "
            log_cmd_stdout  = f" >>{log_file}"
            log_cmd_stderr  = f" 2>>{log_file}"
            log_cmd_all     = f" >>{log_file} 2>&1"

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Save original command
        original_cmd = cmd

        # Run in docker image if specified
        if docker_image is not None:
            cmd = f"sudo docker run --rm --user root -v {self.wrk_dir}:{self.wrk_dir} {docker_image} " \
                f"/bin/bash -c '{cmd}'"

        # Modify quotation marks to be able to send through SSH
        cmd = cmd.replace("'", "'\"'\"'")

        # Wrap the command around ssh
        cmd = f"ssh -i {self.ssh_private_key} " \
              f"-o CheckHostIP=no -o StrictHostKeyChecking=no " \
              f"-o SendEnv=AWS_ACCESS_KEY_ID " \
              f"-o SendEnv=AWS_SECRET_ACCESS_KEY " \
              f"-o SendEnv=GOOGLE_APPLICATION_CREDENTIALS " \
              f"{self.ssh_connection_user}@{self.external_IP} -- '{cmd}'"

        # Run command using subprocess popen and add Popen object to self.processes
        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, original_cmd))

        # Generating process arguments
        kwargs = {

            # Add Popen specific arguments
            "shell": True,
            "stdout": sp.PIPE,
            "stderr": sp.PIPE,
            "close_fds": True,
            "env":  {
                "GOOGLE_APPLICATION_CREDENTIALS": f"/home/{self.ssh_connection_user}/GCP.json",
                "AWS_ACCESS_KEY_ID": self.identity,
                "AWS_SECRET_ACCESS_KEY": self.secret
            },

            # Add CloudConductor specific arguments
            "original_cmd": original_cmd,
            "num_retries": self.default_num_cmd_retries if num_retries is None else num_retries,
            "docker_image": docker_image
        }

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def get_status(self):

        if self.node is None:
            return CloudInstance.OFF

        self.node = self.driver.list_nodes(ex_node_ids=[self.node.id])[0]

        # Define mapping between the cloud status and the current class status
        status_map = {
            "pending":          CloudInstance.CREATING,
            'running':          CloudInstance.AVAILABLE,
            'shutting-down':    CloudInstance.DESTROYING,
            'stopping':         CloudInstance.DESTROYING,
            'terminated':       CloudInstance.OFF,
            'stopped':          CloudInstance.OFF
        }

        return status_map[self.node.extra["status"]]

    def get_compute_price(self):
        if self.instance_type and self.instance_type["price"]:
            return self.instance_type["price"]
        return 0

    def get_storage_price(self):
        if self.instance_type and self.instance_type["storage_price"]:
            return self.instance_type["storage_price"]
        return 0

    def __get_region_name(self):
        default_region = 'EU (Ireland)'
        endpoint_file = resource_filename('botocore', 'data/endpoints.json')
        try:
            with open(endpoint_file, 'r') as f:
                data = json.load(f)
            return data['partitions'][0]['regions'][self.region]['description']
        except IOError:
            return default_region

    def get_price(self, region, instance_type):
        # Search product filter
        FLT = '[{{"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"}},'\
            '{{"Field": "operatingSystem", "Value": "Linux", "Type": "TERM_MATCH"}},'\
            '{{"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"}},'\
            '{{"Field": "instanceType", "Value": "{t}", "Type": "TERM_MATCH"}},'\
            '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}},'\
            '{{"Field": "capacitystatus", "Value": "Used", "Type": "TERM_MATCH"}}]'

        client = boto3.client('pricing', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1')
        f = FLT.format(r=region, t=instance_type)
        data = client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
        od = json.loads(data['PriceList'][0])['terms']['OnDemand']
        id1 = list(od)[0]
        id2 = list(od[id1]['priceDimensions'])[0]
        return float(od[id1]['priceDimensions'][id2]['pricePerUnit']['USD'])

    def get_ebs_price(self, region, storage_type="standard"):
        ebs_name_map = {
            'standard': 'Magnetic',
            'gp2': 'General Purpose',
            'io1': 'Provisioned IOPS',
            'st1': 'Throughput Optimized HDD',
            'sc1': 'Cold HDD'
        }

        # Search product filter
        FLT = '[{{"Field": "volumeType", "Value": "{t}", "Type": "TERM_MATCH"}},'\
            '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]'

        client = boto3.client('pricing', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1')
        f = FLT.format(r=region, t=ebs_name_map[storage_type])
        data = client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
        od = json.loads(data['PriceList'][0])['terms']['OnDemand']
        id1 = list(od)[0]
        id2 = list(od[id1]['priceDimensions'])[0]
        return float(od[id1]['priceDimensions'][id2]['pricePerUnit']['USD'])

    def get_spot_price(self, zone, instance_type):
        client = boto3.client('ec2', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1')
        prices = client.describe_spot_price_history(InstanceTypes=[instance_type], MaxResults=100, ProductDescriptions=['Linux/UNIX (Amazon VPC)'], AvailabilityZone=zone)
        # return the average of the most recent prices multiplied by 10%
        return statistics.mean([float(x['SpotPrice']) for x in prices['SpotPriceHistory']]) * 1.10

    def cancel_spot_instance_request(self):
        client = boto3.client('ec2', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1')
        describe_args = {'Filters': [
                            {'Name': 'instance-id', 'Values': [self.node.id]}
                        ],
                        'MaxResults': 5}
        spot_requests = client.describe_spot_instance_requests(**describe_args)

        if spot_requests and spot_requests['SpotInstanceRequests']:
            request_id = spot_requests['SpotInstanceRequests'][0]['SpotInstanceRequestId']
            response = client.cancel_spot_instance_requests(SpotInstanceRequestIds=[request_id])
            if response and response['CancelledSpotInstanceRequests']: 
                return response['CancelledSpotInstanceRequests'][0]['SpotInstanceRequestId'] == request_id
