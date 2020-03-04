import os
import logging
import random
import csv

from threading import Thread

from System.Platform import Process, CloudPlatform
from System.Platform.Amazon import AmazonInstance, AmazonSpotInstance

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver


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

    def validate(self):

        # Check if security group exists
        if self.security_group is None:
            logging.error("Please specify the 'security_group' field in the platform config!")
            raise IOError("Platform config is missing the 'security_group' field!")
        else:
            try:
                self.driver.ex_get_security_groups(group_names=[self.security_group])
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
        inst_name = inst_name.replace("_", "-").lower()

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
        self.driver.delete_key_pair(self.driver.get_key_pair(self.ssh_key_pair))
