import logging
import requests
import time

from System.Platform.Instance import Instance


class KubernetesJob(Instance):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        super(KubernetesJob, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

    def get_nodepool_label(self):
        return NotImplementedError("Identify from instance resources what type of binning label should be put")

    def run(self):
        pass

    def wait_process(self, proc_name):
        pass

    def finalize(self):
        pass

    def get_start_time(self):
        pass

    def get_stop_time(self):
        pass

    def get_status(self, log_status=False):
        pass

    def add_checkpoint(self, clear_output=True):
        pass

    def compute_cost(self):
        pass

    def get_compute_price(self):
        pass

    def get_storage_price(self):
        pass
