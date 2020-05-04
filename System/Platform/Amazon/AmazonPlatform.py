import os
import logging
import random
import csv
import inspect
import time
import boto3
import json
import statistics

from requests.exceptions import BaseHTTPError, HTTPError
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

from pkg_resources import resource_filename

from threading import Thread

from System.Platform import Process, CloudPlatform
from System.Platform.Amazon import AmazonInstance, AmazonSpotInstance
from requests.exceptions import BaseHTTPError

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.common.exceptions import RateLimitReachedError


class AmazonPlatform(CloudPlatform):

    def __init__(self, name, platform_config_file, final_output_dir):

        # Initialize the base class
        super(AmazonPlatform, self).__init__(name, platform_config_file, final_output_dir)

        self.identity, self.secret = self.parse_identity_file_csv()

        # Initialize libcloud driver
        self.driver = None

        # Initialize platform variables
        self.ssh_key_pair = None
        self.security_group = self.extra.get("security_group", None)

        self.boto_config = Config(
            retries = dict(
                max_attempts = 20,
                mode = 'adaptive'
            )
        )

        # Retrieve pricing info for AWS instances
        self.instance_type_list_filter = self.extra.get("instance_type_list", [])
        self.instance_type_list = None
        self.__build_instance_type_list()

    def parse_identity_file_csv(self):

        # Parse service account file
        with open(self.identity) as csv_inp:
            aws_dict = csv.DictReader(csv_inp)

            for row in aws_dict:
                return row["Access key ID"], row["Secret access key"]
        return None, None

    def get_random_zone(self):

        # Get list of zones and filter them to start with the current region
        zones_in_region = [zone_obj.name for zone_obj in self.driver.driver.ex_list_availability_zones()]

        return random.choice(zones_in_region)

    def get_disk_image_size(self):

        # Obtain image information
        if self.disk_image_obj is None:
            self.disk_image_obj = self.driver.get_image(self.disk_image)
            logging.info(f"DISK IMAGE****************************{self.disk_image_obj}")

        # Obtain the disk size
        for disk_info in self.disk_image_obj.extra["block_device_mapping"]:
            if "ebs" in disk_info:
                return int(disk_info["ebs"]["volume_size"])
        else:
            raise RuntimeError(f"Could not obtain disk size in GB for the image '{self.disk_image}'!")

    def get_cloud_instance_class(self):
        if "preemptible" in self.extra and self.extra["preemptible"]:
            return AmazonSpotInstance
        return AmazonInstance

    def authenticate_platform(self):

        # Retry all HTTP requests
        os.environ['LIBCLOUD_RETRY_FAILED_HTTP_REQUESTS'] = "True"

        # Initialize libcloud driver
        driver_class = get_driver(Provider.EC2)
        self.driver = driver_class(self.identity, self.secret, region=self.region)

        # Add an SSH key pair for the current run
        unique_id = f"{self.name[:10]}-{self.generate_unique_id()}"
        self.ssh_key_pair = f"cc-key-{unique_id}"
        key_pub_path = f"{self.ssh_private_key}.pub"
        self.driver.import_key_pair_from_file(self.ssh_key_pair, key_pub_path)

    def get_ssh_key_pair(self):
        return self.ssh_key_pair

    def get_security_group(self):
        return self.security_group

    def get_instance_type_list(self):
        return self.instance_type_list

    def validate(self):

        # Check if security group exists
        if self.security_group is None:
            logging.error("Please specify the 'security_group' field in the platform config!")
            raise IOError("Platform config is missing the 'security_group' field!")
        else:
            try:
                self.__aws_request(self.driver.ex_get_security_groups, group_names=[self.security_group])
            except:
                logging.error(f"Security group '{self.security_group}' could not be retrieved!")
                raise

        # Validate if image exists
        try:
            self.disk_image_obj = self.driver.get_image(self.disk_image)
        except:
            logging.error(f"Disk image '{self.disk_image}' not found is cannot be accessed!")
            raise

    @staticmethod
    def standardize_instance(inst_name, nr_cpus, mem, disk_space):

        # Ensure instance name does not contain weird characters
        inst_name = inst_name.replace("_", "-").replace(".", "-").lower()

        return inst_name, nr_cpus, mem, disk_space

    def publish_report(self, report_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir, os.path.basename(report_path))
        logging.info("Destinatinon path for report: %s" % dest_path)

        # Transfer report file to bucket
        cmd = "aws s3 cp $( [ -d %s ] && echo --recursive ) %s %s" % \
               (report_path, report_path, dest_path)
        err_msg = "Could not transfer final report to the final output directory!"
        env_var = {
            "AWS_ACCESS_KEY_ID": self.identity,
            "AWS_SECRET_ACCESS_KEY": self.secret
        }
        Process.run_local_cmd(cmd, err_msg=err_msg, env_var=env_var)

    def push_log(self, log_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir,  os.path.basename(log_path))

        # Transfer report file to bucket
        cmd = "aws s3 cp $( [ -d %s ] && echo --recursive ) %s %s" % \
               (log_path, log_path, dest_path)
        err_msg = "Could not transfer final log to the final output directory!"
        env_var = {
            "AWS_ACCESS_KEY_ID": self.identity,
            "AWS_SECRET_ACCESS_KEY": self.secret
        }
        Process.run_local_cmd(cmd, err_msg=err_msg, env_var=env_var)

    def clean_up(self):

        # Initialize the list of threads
        destroy_threads = []

        # Launch the destroy process for each instance
        for name, instance_obj in self.instances.items():
            if instance_obj is None:
                continue

            thr = Thread(target=instance_obj.destroy, daemon=True)
            thr.start()
            destroy_threads.append(thr)

        # Wait for all threads to finish
        for _thread in destroy_threads:
            _thread.join()

        # Destroy SSH key pair
        key_pair = self.__aws_request(self.driver.get_key_pair, self.ssh_key_pair)
        self.__aws_request(self.driver.delete_key_pair, key_pair)

    def __aws_request(self, method, *args, **kwargs):
        """ Function for handling AWS requests and rate limit issues """
        # retry command up to 20 times
        for i in range(20):
            try:
                return method(*args, **kwargs)
            except Exception as e:
                if self.__handle_api_error(e, method, i+1):
                    continue
                raise RuntimeError(str(e))
        raise RuntimeError("Exceeded number of retries for function %s" % method.__name__)

    def __handle_api_error(self, e, method, count):
        exception_string = str(e)
        logging.debug("[AMAZONPLATFORM] Handling issues with api")
        logging.debug(f"Print out of exception {exception_string}")
        if 'MaxSpotInstanceCountExceeded' in exception_string or 'InstanceLimitExceeded' in exception_string:
            logging.info("Maximum number of spot instances exceeded.")
            return False
        if 'RequestLimitExceeded' in exception_string or 'Rate limit exceeded' in exception_string or 'ThrottlingException' in exception_string or 'RequestResourceCountExceeded' in exception_string:
            logging.debug(f"Rate Limit Exceeded during request {method.__name__}. Sleeping for {10*count} seconds before retrying.")
            time.sleep(10*count)
            return True
        if 'Job did not complete in 180 seconds' in exception_string or 'Timed out' in exception_string:
            logging.debug(f"({self.name}) Libcloud command timed out sleeping for 30 seconds before retrying.")
            time.sleep(30)
            return True
        return False

    def __build_instance_type_list(self):
        if not self.instance_type_list:
            ec2 = boto3.client('ec2', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name=self.region, config=self.boto_config)
            region_name = self.__get_region_name()
            describe_args = {'Filters': [
                                {'Name': 'current-generation', 'Values': ['true']}
                            ]}
            self.instance_type_list = []
            # get all instance types
            while True:
                describe_result = self.__aws_request(ec2.describe_instance_types, **describe_args)
                for instance_type in describe_result['InstanceTypes']:
                    if self.instance_type_list_filter and instance_type['InstanceType'] in self.instance_type_list_filter:
                        self.instance_type_list.append(instance_type)
                    elif not self.instance_type_list_filter:
                        self.instance_type_list.append(instance_type)
                if 'NextToken' not in describe_result:
                    break
                describe_args['NextToken'] = describe_result['NextToken']

            # get pricing for instance types
            self.__map_instance_type_pricing(region_name, self.zone, self.instance_type_list)

    def __get_region_name(self):
        default_region = 'EU (Ireland)'
        endpoint_file = resource_filename('botocore', 'data/endpoints.json')
        try:
            with open(endpoint_file, 'r') as f:
                data = json.load(f)
            return data['partitions'][0]['regions'][self.region]['description']
        except IOError:
            return default_region

    def __map_instance_type_pricing(self, region, zone, instance_types):
        # construct list of instance type names
        inst_type_list = [x['InstanceType'] for x in instance_types]
        # Search product filter
        pricing_args = {'ServiceCode': 'AmazonEC2',
                        'Filters': [
                            {"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"},
                            {"Field": "operatingSystem", "Value": "Linux", "Type": "TERM_MATCH"},
                            {"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"},
                            {"Field": "location", "Value": region, "Type": "TERM_MATCH"},
                            {"Field": "capacitystatus", "Value": "Used", "Type": "TERM_MATCH"}
                        ],
                        'MaxResults': 100}

        # get standard pricing for products
        client = boto3.client('pricing', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1')
        pricing_data = {}
        while True:
            pricing_result = self.__aws_request(client.get_products, **pricing_args)
            # build pricing dictionary
            for prod in pricing_result['PriceList']:
                prod_data = json.loads(prod)
                inst_type = prod_data['product']['attributes']['instanceType']
                if inst_type in inst_type_list:
                    od = prod_data['terms']['OnDemand']
                    id1 = list(od)[0]
                    id2 = list(od[id1]['priceDimensions'])[0]
                    price = float(od[id1]['priceDimensions'][id2]['pricePerUnit']['USD'])
                    pricing_data[inst_type] = price
            if 'NextToken' not in pricing_result or pricing_result['NextToken'] == '':
                break
            pricing_args['NextToken'] = pricing_result['NextToken']

        client = boto3.client('ec2', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1', config=self.boto_config)
        spot_pricing_data = {}
        start_time = datetime.today() - timedelta(days=2)
        next_token = ''

        while True:
            spot_pricing_result = self.__aws_request(client.describe_spot_price_history, StartTime=start_time, InstanceTypes=inst_type_list, MaxResults=1000, ProductDescriptions=['Linux/UNIX (Amazon VPC)'], AvailabilityZone=zone, NextToken=next_token)
            # build pricing dictionary
            for price in spot_pricing_result['SpotPriceHistory']:
                if price['InstanceType'] not in spot_pricing_data:
                    spot_pricing_data[price['InstanceType']] = []

                spot_pricing_data[price['InstanceType']].append(price['SpotPrice'])
            if 'NextToken' not in spot_pricing_result or spot_pricing_result['NextToken'] == '':
                break
            next_token = spot_pricing_result['NextToken']

        for key in spot_pricing_data:
            spot_pricing_data[key] = statistics.mean([float(x) for x in spot_pricing_data[key]]) * 1.30
        storage_price = self.__get_ebs_price(region)

        for inst_type in instance_types:
            if inst_type['InstanceType'] in pricing_data:
                inst_type['price'] = pricing_data[inst_type['InstanceType']]
            if inst_type['InstanceType'] in spot_pricing_data:
                inst_type['spotPrice'] = spot_pricing_data[inst_type['InstanceType']]
            inst_type['storagePrice'] = storage_price

    def __get_ebs_price(self, region, storage_type="standard"):
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

        client = boto3.client('pricing', aws_access_key_id=self.identity, aws_secret_access_key=self.secret, region_name='us-east-1', config=self.boto_config)
        f = FLT.format(r=region, t=ebs_name_map[storage_type])
        data = self.__aws_request(client.get_products, ServiceCode='AmazonEC2', Filters=json.loads(f))
        od = json.loads(data['PriceList'][0])['terms']['OnDemand']
        id1 = list(od)[0]
        id2 = list(od[id1]['priceDimensions'])[0]
        return float(od[id1]['priceDimensions'][id2]['pricePerUnit']['USD'])
