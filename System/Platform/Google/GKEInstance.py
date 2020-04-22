import logging
import requests
import time

from System.Platform.Instance import KubernetesInstance


class GKEInstance(KubernetesInstance):

    def create(self):
        pass

    def destroy(self):
        pass

    def run(self):
        pass

    def stop(self):
        pass

    def wait_process(self, proc_name):
        pass

    def handle_failure(self, proc_name, proc_obj):
        pass

    def get_start_time(self):
        pass

    def get_stop_time(self):
        pass

    def set_workspace(self, wrk_dir, wrk_log_dir, wrk_out_dir):
        pass

    def get_status(self, log_status=False):
        pass

    def get_compute_price(self):
        pass

    def get_storage_price(self):
        pass