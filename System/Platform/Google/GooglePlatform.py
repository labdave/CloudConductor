import math
import os
import logging
import base64
import random
import json
from threading import Thread

from System import CC_MAIN_DIR
from System.Platform import Process, CloudPlatform
from System.Platform.Google import GoogleInstance, GooglePreemptibleInstance

from google.cloud import pubsub_v1
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.common.google import ResourceNotFoundError


class GooglePlatform(CloudPlatform):

    def __init__(self, name, platform_config_file, final_output_dir):

        # Initialize the base class
        super(GooglePlatform, self).__init__(name, platform_config_file, final_output_dir)

        # Obtain the service account and the project ID
        self.service_account, self.project_id = self.parse_service_account_json()

        self.extra = self.config.get("extra", {})

        # Initialize libcloud driver
        self.driver = None

    def parse_service_account_json(self):

        # Parse service account file
        with open(self.identity) as json_inp:
            service_account_data = json.load(json_inp)

        # Save data locally
        service_account = service_account_data["client_email"]
        project_id = service_account_data["project_id"]

        return service_account, project_id

    def get_random_zone(self):

        # Get list of zones and filter them to start with the current region
        zones_in_region = [
            zone_obj.name for zone_obj in self.driver.ex_list_zones() if zone_obj.name.startswith(self.region)
        ]

        return random.choice(zones_in_region)

    def get_disk_image_size(self):

        # Obtain image information
        if self.disk_image_obj is None:
            self.disk_image_obj = self.driver.ex_get_image(self.disk_image)

        return int(self.disk_image_obj.extra["diskSizeGb"])

    def get_cloud_instance_class(self):
        if "preemptible" in self.extra and self.extra["preemptible"]:
            return GooglePreemptibleInstance
        return GoogleInstance

    def authenticate_platform(self):

        # Retry all HTTP requests
        os.environ['LIBCLOUD_RETRY_FAILED_HTTP_REQUESTS'] = "True"

        # Export google cloud credential file
        if os.path.isabs(self.identity):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.identity
        else:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CC_MAIN_DIR, self.identity)

        # Initialize libcloud driver
        driver_class = get_driver(Provider.GCE)
        self.driver = driver_class(self.service_account, self.identity,
                                   datacenter=self.zone,
                                   project=self.project_id)

    def validate(self):

        # Validate if image exists
        try:
            self.disk_image_obj = self.driver.ex_get_image(self.disk_image)
        except ResourceNotFoundError:
            logging.error(f"Disk image '{self.disk_image}' not found!")
            raise

    @staticmethod
    def standardize_instance(inst_name, nr_cpus, mem, disk_space):

        # Ensure instance name does not contain weird characters
        inst_name = inst_name.replace("_", "-").replace(".","-").lower()

        # Ensure number of CPUs is an even number or 1
        if nr_cpus != 1 and nr_cpus % 2 == 1:
            nr_cpus += 1

        # Ensure the memory is within GCP range:
        if mem / nr_cpus < 0.9:
            mem = math.ceil(nr_cpus * 0.9)
        elif mem / nr_cpus > 6.5:
            nr_cpus = math.ceil(mem / 6.5)

        return inst_name, nr_cpus, mem, disk_space

    def publish_report(self, report_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir, os.path.basename(report_path))

        # Authenticate for gsutil use
        cmd = "gcloud auth activate-service-account --key-file %s" % self.identity
        Process.run_local_cmd(cmd, err_msg="Authentication to Google Cloud failed!")

        # Transfer report file to bucket
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp -r '%s' '%s' 1>/dev/null 2>&1 " % (options_fast, report_path, dest_path)
        logging.debug(f"Publish report cmd: {cmd}")
        Process.run_local_cmd(cmd, err_msg="Could not transfer final report to the final output directory!")

        # Check if the user has provided a Pub/Sub report topic
        pubsub_topic = self.extra.get("report_topic", None)
        pubsub_project = self.extra.get("pubsub_project", None)

        # Send report to the Pub/Sub report topic if it's known to exist
        if pubsub_topic and pubsub_project:
            GooglePlatform.__send_pubsub_message(pubsub_topic, pubsub_project, dest_path)

    def push_log(self, log_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir, os.path.basename(log_path))

        # Authenticate for gsutil use
        cmd = "gcloud auth activate-service-account --key-file %s" % self.identity
        Process.run_local_cmd(cmd, err_msg="Authentication to Google Cloud failed!")

        # Transfer report file to bucket
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp -r '%s' '%s' 1>/dev/null 2>&1 " % (options_fast, log_path, dest_path)
        Process.run_local_cmd(cmd, err_msg="Could not transfer final log to the final output directory!")

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

    @staticmethod
    def __send_pubsub_message(topic_name, project_id, message, encode=True):

        # Generate correct topic ID
        topic_id = f"projects/{project_id}/topics/{topic_name}"

        # Create a Pub/Sub publisher
        pubsub = pubsub_v1.PublisherClient()

        # Encode message is requested
        if encode:
            message = base64.b64encode(message.encode("utf8"))
        else:
            message = message.encode("utf8")

        # Send message to platform Pub/Sub
        pubsub.publish(topic_id, message)
