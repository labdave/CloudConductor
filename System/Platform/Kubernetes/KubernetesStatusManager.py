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
        self.is_monitoring = False

        self.job_list = None
        self.job_watch = None
        self.job_watch_reset = 0
        self.pod_watch = None
        self.pod_watch_reset = 0
        # list of jobs that we want to log when we have updates
        self.log_update_list = {}
        self.pod_status_dict = {}

        self.monitoring_thread = Thread(target=self.__monitor_jobs)
        self.pod_monitoring_thread = Thread(target=self.__monitor_pods)

    def start_job_monitoring(self):
        self.is_monitoring = True
        self.monitoring_thread.start()
        self.pod_monitoring_thread.start()

    def stop_job_monitoring(self):
        self.is_monitoring = False
        if self.job_watch:
            self.job_watch.stop()
        if self.pod_watch:
            self.pod_watch.stop()
        logging.info("Stopped watching job status updates.")

    def __monitor_pods(self):
        global pod_monitoring_failure
        pod_monitoring_failure = False
        self.pod_status_dict = {}
        while self.pod_watch_reset < 5 and self.is_monitoring:
            self.pod_watch = watch.Watch()
            for event in self.pod_watch.stream(self.core_api.list_namespaced_pod, namespace='cloud-conductor'):
                pod = event['object']
                if pod.kind == 'Pod':
                    pod_job = None
                    pod_name = pod.metadata.name
                    if pod and 'job-name' in pod.metadata.labels:
                        pod_job = pod.metadata.labels['job-name']
                    if pod_job:
                        if pod_job in self.log_update_list:
                            self.pod_watch_reset = 0
                            if pod.status.init_container_statuses and pod.status.container_statuses:
                                num_containers = len(pod.status.init_container_statuses) + len(pod.status.container_statuses)
                                current_running_container = None
                                container_index = 0
                                for container in pod.status.init_container_statuses:
                                    if container.state.running:
                                        current_running_container = container
                                        break
                                    container_index += 1
                                if not current_running_container:
                                    for container in pod.status.container_statuses:
                                        if container.state.running:
                                            current_running_container = container
                                            break
                                        container_index += 1
                                if current_running_container:
                                    new_index = container_index + 1
                                    if pod_job in self.pod_status_dict and pod_name != self.pod_status_dict[pod_job]["pod_name"]:
                                        self.pod_status_dict[pod_job] = {"checkpoint": new_index, "preemptions": self.pod_status_dict[pod_job]['preemptions'] + 1, "pod_name": pod_name}
                                        if self.pod_status_dict[pod_job]['preemptions'] <= 2:
                                            logging.info(f"({pod_job}) Job was preempted, is rerunning, and is on task {container_index + 1}/{num_containers}. Preemptions {self.pod_status_dict[pod_job]['preemptions']}/2")
                                    else:
                                        if (pod_job in self.pod_status_dict and new_index > self.pod_status_dict[pod_job]['checkpoint']) or pod_job not in self.pod_status_dict:
                                            logging.info(f"({pod_job}) Job is currently on task {container_index + 1}/{num_containers}. Current running task: ({current_running_container.name})")
                                        preempts = self.pod_status_dict[pod_job]['preemptions'] if pod_job in self.pod_status_dict else 0
                                        self.pod_status_dict[pod_job] = {"checkpoint": new_index, "preemptions": preempts, "pod_name": pod_name}
                else:
                    self.pod_watch.stop()
                    self.pod_watch_reset += 1
                    logging.warning(f"No pod info with event. We will try to reset the pod watch.")
                    break
        if self.pod_watch_reset >= 5:
            pod_monitoring_failure = True
            logging.error("Failure to setup the pod watch. Will not be able to get pod status from the Kubernetes cluster.")

    def __monitor_jobs(self):
        global job_monitoring_failure
        job_monitoring_failure = False
        while self.job_watch_reset < 5 and self.is_monitoring:
            self.job_watch = watch.Watch()
            self.job_list = {}
            for event in self.job_watch.stream(self.batch_api.list_namespaced_job, namespace='cloud-conductor'):
                job_name = event['object'].metadata.name
                if job_name:
                    self.job_watch_reset = 0
                    self.job_list[str(job_name)] = event['object']
                    if job_name in self.log_update_list:
                        status_str = str(event['object'].status).replace("\n", "")
                        logging.info(f"({job_name}) Job Status: {status_str}")
                        if event['object'].status.succeeded:
                            self.remove_job_from_log_list(job_name)
                else:
                    self.job_watch.stop()
                    self.job_watch_reset += 1
                    logging.warning(f"No job info with event. We will try to reset the job watch.")
                    break
        if self.job_watch_reset >= 5:

            job_monitoring_failure = True
            logging.error("Failure to setup the job watch. Will not be able to get job status from the Kubernetes cluster.")

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

    def check_monitoring_status(self):
        if job_monitoring_failure or pod_monitoring_failure:
            raise RuntimeError("Failure to get pod/job status stream connected. Stopping execution.")

    def get_job_status(self, job_name, force_refresh=False):
        self.check_monitoring_status()
        self.add_job_to_log_list(job_name)
        if self.job_list and job_name in self.job_list:
            pod_status = [val for key, val in self.pod_status_dict.items() if job_name in key]
            if pod_status and pod_status[0]:
                return self.job_list[job_name].status, pod_status[0]['preemptions']
            return self.job_list[job_name].status, 0
        return "", 0

    def get_job_info(self, job_name, force_refresh=False):
        if self.job_list and job_name in self.job_list:
            return self.job_list[job_name]
        return ""
