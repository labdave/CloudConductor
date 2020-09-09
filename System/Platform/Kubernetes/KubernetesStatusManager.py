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

from kubernetes import client, config, watch


class KubernetesStatusManager(object):

    def __init__(self, batch_api, core_api):

        self.batch_api = batch_api
        self.core_api = core_api

        self.job_list = None
        self.job_watch = None
        # list of jobs that we want to log when we have updates
        self.log_update_list = {}

        self.monitoring_thread = Thread(target=self.__monitor_jobs)

    def start_job_monitoring(self):
        self.is_monitoring = True
        self.monitoring_thread.start()

    def stop_job_monitoring(self):
        self.is_monitoring = False
        self.job_watch.stop()
        logging.info("Stopped watching job status updates.")

    def __monitor_jobs(self):
        self.job_watch = watch.Watch()
        self.job_list = {}
        for event in self.job_watch.stream(self.batch_api.list_namespaced_job, namespace='cloud-conductor'):
            job_name = event['object'].metadata.name
            if job_name:
                self.job_list[str(job_name)] = event['object']

    def add_job_to_log_list(self, job_name):
        self.log_update_list[job_name] = True

    def remove_job_from_log_list(self, job_name):
        del self.log_update_list[job_name]

    def update_statuses(self):
        try:
            response = api_request(self.batch_api.list_namespaced_job, namespace='cloud-conductor')
            if response and response.get("items"):
                self.job_list = {job.get("metadata", {}).get("name"): job for job in response.get("items")}
        except Exception as e:
            logging.error("Error with updating the job list to check statuses.")

    def get_job_status(self, job_name, force_refresh=False):
        if self.job_list and job_name in self.job_list:
            return self.job_list[job_name].status
        return ""

    def get_job_info(self, job_name, force_refresh=False):
        if self.job_list and job_name in self.job_list:
            return self.job_list[job_name]
        return ""
