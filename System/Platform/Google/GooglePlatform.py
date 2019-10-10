import math
import os
import subprocess as sp
import logging
import base64
import random
from threading import Thread

from System import CC_MAIN_DIR
from System.Platform import CloudPlatform
from System.Platform.Google import GoogleInstance

from google.cloud import pubsub_v1
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver


class GooglePlatform(CloudPlatform):

    def __init__(self, name, platform_config_file, final_output_dir):

        # Initialize the base class
        super(GooglePlatform, self).__init__(name, platform_config_file, final_output_dir)

        # Initialize additional necessary variables
        self.driver = None

        # Obtain the service account and the project ID
        self.service_account, self.project_id = GoogleInstance.parse_service_account_json(self.identity)

    def get_random_zone(self):

        # Get list of zones and filter them to start with the current region
        zones_in_region = [
            zone_obj.name for zone_obj in self.driver.ex_list_zones() if zone_obj.name.startswith(self.region)
        ]

        return random.choice(zones_in_region)

    def get_cloud_instance_class(self):
        return GoogleInstance

    def authenticate_platform(self):

        # Export google cloud credential file
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CC_MAIN_DIR, self.identity)

        # Initialize libcloud driver
        driver_class = get_driver(Provider.GCE)
        self.driver = driver_class(self.service_account, self.identity,
                                   datacenter=self.zone,
                                   project=self.project_id)

    def validate(self):
        # Nothing to validate for Google Cloud
        pass

    @staticmethod
    def standardize_instance(inst_name, nr_cpus, mem, disk_space):

        # Ensure instance name does not contain weird characters
        inst_name = inst_name.replace("_", "-").lower()

        # Ensure the memory is withing GCP range:
        if mem / nr_cpus < 0.9:
            mem = nr_cpus * 0.9
        elif mem / nr_cpus > 6.5:
            nr_cpus = math.ceil(mem / 6.5)

        # Ensure number of CPUs is an even number or 1
        if nr_cpus != 1 and nr_cpus % 2 == 1:
            nr_cpus += 1

        return inst_name, nr_cpus, mem, disk_space

    def publish_report(self, report_path):

        # Generate destination file path
        dest_path = os.path.join(self.final_output_dir, os.path.basename(report_path))

        # Transfer report file to bucket
        options_fast = '-m -o "GSUtil:sliced_object_download_max_components=200"'
        cmd = "gsutil %s cp -r %s %s 1>/dev/null 2>&1 " % (options_fast, report_path, dest_path)
        GooglePlatform.__run_cmd(cmd, err_msg="Could not transfer final report to the final output directory!")

        # Check if the user has provided a Pub/Sub report topic
        pubsub_topic = self.extra.get("report_topic", None)
        pubsub_project = self.extra.get("pubsub_project", None)

        # Send report to the Pub/Sub report topic if it's known to exist
        if pubsub_topic and pubsub_project:
            GooglePlatform.__send_pubsub_message(pubsub_topic, pubsub_project, dest_path)

    def clean_up(self):

        # Initialize the list of threads
        destroy_threads = []

        # Launch the destroy process for each instance
        for name, instance_obj in self.instances.items():
            thr = Thread(target=instance_obj.destroy, daemon=True)
            thr.start()
            destroy_threads.append(thr)

        # Wait for all threads to finish
        for _thread in destroy_threads:
            _thread.join()

    @staticmethod
    def __run_cmd(cmd, err_msg=None, num_retries=5):
        # Running and waiting for the command
        proc = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out, err = proc.communicate()

        # Convert to string formats
        out = out.decode("utf8")
        err = err.decode("utf8")

        # Check if any error has appeared
        if len(err) != 0 and "error" in err.lower():

            # Retry command if possible
            if num_retries > 0:
                return GooglePlatform.__run_cmd(cmd, err_msg, num_retries=num_retries-1)

            logging.error(f"GooglePlatform could not run the following command:\n{cmd}")

            if err_msg is not None:
                logging.error(f"{err_msg}.\nThe following error appeared:\n    {err}")

            raise RuntimeError("Error running command in GooglePlatform!")

        return out

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
