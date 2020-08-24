import logging
import requests
import time
import os
import ast
import re
import tempfile
import yaml
import json

from System.Platform.Instance import Instance
from System.Platform import Platform, Process
from System.Platform.Kubernetes.utils import api_request, get_api_sleep
from collections import OrderedDict
from threading import Thread

from kubernetes import client, config


class KubernetesStatusManager(object):

    def __init__(self, batch_api, core_api):

        self.batch_api = batch_api
        self.core_api = core_api

        self.job_list = None

        self.monitoring_thread = Thread(target=self.__monitor_jobs)

    def start_job_monitoring(self):
        self.is_monitoring = True
        self.monitoring_thread.start()

    def stop_job_monitoring(self):
        self.is_monitoring = False

    def __monitor_jobs(self):
        while self.is_monitoring:
            self.update_statuses()
            time.sleep(25)

    def update_statuses(self):
        try:
            response = api_request(self.batch_api.list_namespaced_job, namespace='cloud-conductor')
            if response and response.get("items"):
                self.job_list = {job.get("metadata", {}).get("name"): job for job in response.get("items")}
        except Exception as e:
            logging.error("Error with updating the job list to check statuses.")

    def get_job_status(self, job_name, force_refresh=False):
        if not self.job_list or force_refresh:
            self.update_statuses()
        if self.job_list and job_name in self.job_list:
            return self.job_list[job_name].get("status", "")
        return ""
