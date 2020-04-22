import math
import os
import logging
import base64
import random
import json
from threading import Thread

from System import CC_MAIN_DIR
from System.Platform import Process
from System.Platform.Platform import KubernetesPlatform
from System.Platform.Amazon import EKSInstance

from google.cloud import pubsub_v1
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.common.google import ResourceNotFoundError

from kube_api import config


class EKSPlatform(KubernetesPlatform):

    def get_random_zone(self):
        return self.zone

    def get_instance_class(self):
        return EKSPlatform

    def authenticate_platform(self):
        config.load_configuration(self.identity)

    def validate(self):
        pass

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
