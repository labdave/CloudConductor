import logging
import importlib
import json
import os
import time
from collections import OrderedDict

from System.Graph import Graph, Scheduler, ScriptTask
from System.Datastore import ResourceKit, SampleSet, Datastore
from System.Validators import GraphValidator, InputValidator, SampleValidator
from System.Platform import StorageHelper, DockerHelper
from System import CC_MAIN_DIR


class GAPipeline(object):

    def __init__(self, pipeline_id,
                 graph_config,
                 resource_kit_config,
                 sample_data_config,
                 platform_config,
                 platform_module,
                 final_output_dir,
                 generate_script):

        # GAP run id
        self.pipeline_id    = pipeline_id

        # Paths to config files
        self.__graph_config         = graph_config
        self.__res_kit_config       = resource_kit_config
        self.__sample_set_config    = sample_data_config
        self.__platform_config      = platform_config

        # Name of platform class where tasks will be executed
        self.__plat_module          = platform_module

        # Final output directory where output is saved
        self.__final_output_dir     = final_output_dir

        # Flag to just generate the task script ( do not run commands)
        self.__generate_script = generate_script
        self.script_tasks = OrderedDict()

        # Obtain pipeline name and append to final output dir

        self.graph          = None
        self.resource_kit   = None
        self.sample_data    = None
        self.platform       = None

        # Create datastore from pipeline components
        self.datastore      = None

        # Task scheduler for running jobs
        self.scheduler = None

        # Helper classes for handling platform operations
        self.storage_helper     = None
        self.docker_helper      = None

    def load(self):

        # Load resource kit
        self.resource_kit = ResourceKit(self.__res_kit_config)

        # Load the sample data
        self.sample_data = SampleSet(self.__sample_set_config)

        # Load the graph
        self.graph = Graph(self.__graph_config)

        # Load platform
        plat_module     = importlib.import_module(self.__plat_module)
        plat_class      = plat_module.__dict__[self.__plat_module]
        self.platform   = plat_class(self.pipeline_id, self.__platform_config, self.__final_output_dir, self.__generate_script)

        # Initialize the platform
        self.platform.init_platform()

        # Create datastore and scheduler
        self.datastore = Datastore(self.graph, self.resource_kit, self.sample_data, self.platform)
        self.scheduler = Scheduler(self.graph, self.datastore, self.platform, self.script_tasks)

    def validate(self):

        # Assume all validations are working
        has_errors = False

        # Validate the sample set
        sample_validator = SampleValidator(self.sample_data)
        has_errors = sample_validator.validate() or has_errors
        if not has_errors:
            logging.debug("Sample sheet validated!")

        # Validate the graph
        graph_validator = GraphValidator(self.graph, self.resource_kit, self.sample_data)
        has_errors = graph_validator.validate() or has_errors
        if not has_errors:
            logging.debug("Graph validated!")

        # Validate the platform
        self.platform.validate()

        # Stop the pipeline before launching if there are any errors
        if has_errors:
            raise SystemError("One or more errors have been encountered during validation. "
                              "See the above logs for more information")

        # Create storage/docker helpers for checking input files
        self.storage_helper     = StorageHelper(None)
        self.docker_helper      = DockerHelper(None)

        # Validate all pipeline inputs can be found on platform
        input_validator = InputValidator(self.resource_kit, self.sample_data, self.storage_helper, self.docker_helper)
        has_errors = input_validator.validate() or has_errors

        # Stop the pipeline if there are any errors
        if has_errors:
            raise SystemError("One or more errors have been encountered during validation. "
                              "See the above logs for more information")

        logging.info("CloudCounductor run validated! Beginning pipeline execution.")

    def run(self, rm_tmp_output_on_success=True):
        # Run until all tasks are complete
        self.scheduler.run()

        # Remove temporary output on success
        if rm_tmp_output_on_success:
            workspace = self.datastore.get_task_workspace()
            self.storage_helper.rm(path=workspace.get_tmp_output_dir(), job_name="rm_tmp_output", wait=True)

    def save_progress(self):
        pass

    def publish_report(self, err=False, err_msg=None, git_version=None):
        # Create and publish GAP pipeline report
        try:
            
            # Generate report name and path
            if not self.__generate_script:
                report = self.__make_pipeline_report(err, err_msg, git_version)
                report_path = f"{CC_MAIN_DIR}/{self.pipeline_id}_final_report.json"

                # Publish report locally
                with open(report_path, "w") as out:
                    out.write(str(report))
            else:
                report = self.__make_pipeline_report(err, err_msg, git_version)
                report_path = f"{CC_MAIN_DIR}/{self.pipeline_id}_script_report.json"

                # Publish report locally
                with open(report_path, "w") as out:
                    out.write(str(report))
                
                self.platform.publish_report(report_path)

                report_path = f"{CC_MAIN_DIR}/{self.pipeline_id}_cc_script.json"

                # Publish report locally
                with open(report_path, "w") as out:
                    report = OrderedDict()
                    for k, task in self.script_tasks.items():
                        # convert commands into list rather than dictionary
                        cmd_list = []
                        index = 0
                        task.labels.append({"key": "analysis", "value": self.pipeline_id})
                        for cmd in task.commands.values():
                            cmd["index"] = index
                            cmd_list.append(cmd)
                            index += 1
                        task.commands = cmd_list
                        task.git_commit = git_version
                        task.run_name = self.pipeline_id
                    report["tasks"] = list(self.script_tasks.values())
                    out.write(json.dumps(report, default=lambda o: o.__dict__, indent=4))

            # Publish report on the platform
            if self.platform is not None:
                self.platform.publish_report(report_path)
                if not self.__generate_script:
                    self.platform.push_log(f"{CC_MAIN_DIR}/cc_log.txt")

        except BaseException as e:
            logging.error("Unable to publish report!")
            if str(e) != "":
                logging.error("Received the following message:\n%s" % e)
            raise

    def clean_up(self):
        # Cleaning up the platform (let the platform decide what that means)
        if self.platform is not None:
            self.platform.clean_up()

    def __make_pipeline_report(self, err, err_msg, git_version):

        # Create a pipeline report that summarizes features of pipeline
        report = GAPReport(self.pipeline_id, err, err_msg, git_version)

        # Register runtime data for pipeline tasks
        if self.scheduler is not None:
            task_workers = self.scheduler.get_task_workers()
            for task_name, task_worker in task_workers.items():

                # Register data about task runtime
                task        = task_worker.get_task()
                run_time    = task_worker.get_runtime()
                cost        = task_worker.get_cost()
                start_time  = task_worker.get_start_time()
                end_time    = task_worker.get_stop_time()
                cmd         = task_worker.get_cmd()
                task_data   = {"parent_task" : task_name.split(".")[0]}
                report.register_task(task_name=task_name,
                                     start_time=start_time,
                                     end_time=end_time,
                                     run_time=run_time,
                                     cost=cost,
                                     cmd=cmd,
                                     task_data=task_data)

                # Register data about task output files
                if task.is_complete():
                    output_files = self.datastore.get_task_output_files(task_id=task_name)
                    for output_file in output_files:
                        file_type       = output_file.get_type()
                        file_path       = output_file.get_path()
                        is_final_output = file_type in task.get_final_output_keys()
                        file_size       = output_file.get_size()
                        if is_final_output or err:
                            # Only declare output files if file is final output file
                            # OR file is temporary output file but pipeline failed
                            report.register_output_file(task_name, file_type, file_path, file_size, is_final_output)

        return report


class GAPReport(object):
    # Object for holding metadata related to a GAP pipeline run
    def __init__(self, pipeline_id, err=False, err_msg=None, git_version=None):

        # Id of pipeline being reports
        self.pipeline_id = pipeline_id

        # Whether pipeline was halted due to error
        self.err = err

        # Error msg that halted pipelines
        self.err_msg = err_msg

        # Git commit version
        self.git_version = git_version

        # Time of pipeline start
        self.start_time = None

        # Output files produced by successful modules
        self.output_files = []

        # Processors used by modules
        self.tasks = []

    @property
    def total_processing_time(self):
        proc_time = 0
        for task in self.tasks:
            proc_time += float(task["runtime(sec)"])
        return proc_time

    @property
    def total_cost(self):
        cost = 0
        for task in self.tasks:
            cost += float(task["cost"])
        return cost

    @property
    def total_runtime(self):
        start_time = time.time()
        end_time = start_time
        for task in self.tasks:
            if task["start_time"] and task["start_time"] < start_time:
                start_time = task["start_time"]
            if task["end_time"] and task["end_time"] > end_time:
                end_time = task["end_time"]
        return end_time - start_time

    @property
    def total_output_size(self):
        size = 0
        for output_file in self.output_files:
            if output_file["size"]:
                size += float(output_file["size"])
        return size

    def set_fail(self, err_msg=None):
        self.err = True
        self.err_msg = err_msg

    def set_success(self):
        self.err = False
        self.err_msg = None

    def set_start_time(self, start_time):
        self.start_time = start_time

    def register_task(self, task_name, start_time, end_time, run_time, cost, cmd=None, task_data=None):
        # Register information about a specific processor in the report

        # Make start time relative to pipeline start time
        if self.start_time is not None and start_time is not None:
            start_time = start_time - self.start_time

        proc_data = {
            "name" : task_name,
            "start_time" : start_time,
            "end_time": end_time,
            "runtime(sec)" : run_time,
            "cost" : cost,
            "cmd"   : cmd
        }
        logging.debug("Task report(%s). Start: %s, Runtime: %s, Cost: %s" % (task_name, start_time, run_time, cost))
        if task_data is not None:
            for key, val in task_data.items():
                if key not in proc_data:
                    proc_data[key] = val
        self.tasks.append(proc_data)

    def register_output_file(self, task_name, file_type, path, size=0, is_final_output=False):
        logging.debug("Task report(%s). file_type: %s, path: %s, size: %s" % (task_name, file_type, path, size))
        file_data = {"task_id" : task_name,
                     "file_type" : file_type,
                     "path" : path,
                     "size" : size,
                     "is_final_output" : is_final_output}
        self.output_files.append(file_data)

    def to_dict(self):
        report = OrderedDict()
        report["pipeline_id"] = self.pipeline_id
        report["git_commit"] = self.git_version
        report["status"] = "Complete" if not self.err else "Failed"
        report["error"] = "" if self.err_msg is None else self.err_msg
        report["total_cost"] = self.total_cost
        report["total_runtime"] = self.total_runtime
        report["total_proc_time"] = self.total_processing_time
        report["total_output_size"] = self.total_output_size
        report["files"] = self.output_files
        report["tasks"] = self.tasks
        return report

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)
