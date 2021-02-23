import threading
import time
import os
import math
import logging
import json

from System.Workers import Thread
from System.Graph import ModuleExecutor


class TaskWorker(Thread):

    IDLE            = 0
    LOADING         = 1
    RUNNING         = 2
    FINALIZING      = 3
    COMPLETE        = 4
    CANCELLING      = 5
    FINALIZED       = 6

    STATUSES        = ["IDLE", "LOADING", "RUNNING", "FINALIZING", "COMPLETE", "CANCELLING", "FINALIZED"]

    def __init__(self, task, datastore, platform, script_task=None):
        # Class for executing task

        # Initialize new thread
        err_msg = "TaskWorker for %s has stopped working!" % task.get_ID()
        super(TaskWorker, self).__init__(err_msg)

        # Task to be executed
        self.task = task
        self.script_task = script_task
        self.module = self.task.get_module()

        # Datastore for getting/setting task output
        self.datastore = datastore

        # Platform upon which task will be executed
        self.platform = platform

        # Status attributes
        self.status_lock = threading.Lock()
        self.status = TaskWorker.IDLE

        # Processor for executing task
        self.proc       = None

        # Garbage collector for destroying instance on cancellation
        self.garbage_collector = None

        # Module command executor
        self.module_executor = None

        # Flag for whether task successfully completed
        self.__err = True

        # Flag for whether TaskWorker was cancelled
        self.__cancelled = False

        # Command that was run to carry out task
        self.cmd = None

    def set_status(self, new_status):

        # Updates instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            logging.debug("({0}) TaskWorker change of status to {1}!".format(self.task.get_ID(),
                                                                             self.STATUSES[self.status]))
            self.status = new_status

    def get_status(self):
        # Returns instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            return self.status

    def get_task(self):
        return self.task

    def get_runtime(self):
        if self.proc is None:
            return 0
        else:
            return self.proc.get_runtime()

    def get_cost(self):
        if self.proc is None:
            return 0
        else:
            return self.proc.compute_cost()

    def get_start_time(self):
        if self.proc is None:
            return None
        else:
            return self.proc.get_start_time()

    def get_stop_time(self):
        if self.proc is None:
            return None
        else:
            return self.proc.get_stop_time()

    def get_cmd(self):
        return self.cmd

    def get_new_output_dirs(self):

        task_id = self.task.get_ID()
        visible_samples = self.task.get_visible_samples()

        # Create subfolders for split tasks
        if self.task.is_split():
            task_id = task_id.replace(".", "/")

        final_output_dir = os.path.join(self.platform.get_final_output_dir(), task_id)
        final_tmp_dir = os.path.join(self.platform.get_final_output_dir(), "tmp", task_id)

        if visible_samples is not None and len(visible_samples) <= 1 and not self.datastore.is_multisample():
            # Single sample output of multi-sample analysis always goes in sample level folder
            sample_name = visible_samples[0]
            # Remove sample name from path if it already appears and put it as the first directory
            task_id = task_id.replace(sample_name + "/", "")
            final_output_dir = os.path.join(self.platform.get_final_output_dir(), sample_name, task_id)
            final_tmp_dir = os.path.join(self.platform.get_final_output_dir(), "tmp", sample_name, task_id)

        return final_output_dir, final_tmp_dir

    def work(self):
        # Run task module command and save outputs
        try:
            # Set the input arguments that will be passed to the task module
            self.datastore.set_task_input_args(self.task.get_ID())

            # Compute task resource requirements
            cpus            = self.module.get_argument("nr_cpus")
            mem             = self.module.get_argument("mem")

            # see if we're forcing standard instances
            try:
                force_standard  = self.module.get_argument("force_standard").lower() == 'true'
            except RuntimeError:
                # don't care if the input isn't defined
                force_standard = False
                pass

            # Compute disk space requirements
            docker_image    = None
            input_files     = self.datastore.get_task_input_files(self.task.get_ID())
            if self.script_task:
                # if generating a task script save the input files
                self.script_task.input_files = [x.path for x in input_files]
                self.script_task.module_name = self.task.get_module_name()
                self.script_task.submodule_name = self.task.get_submodule_name()
                self.script_task.post_processing_required = self.module.does_process_output
            if self.task.get_docker_image_id() is not None:
                docker_image    = self.datastore.get_docker_image(docker_id=self.task.get_docker_image_id())
            disk_space      = self.__compute_disk_requirements(input_files, docker_image)
            logging.debug("(%s) CPU: %s, Mem: %s, Disk space: %s" % (self.task.get_ID(), cpus, mem, disk_space))

            # Quit if pipeline is cancelled
            self.__check_cancelled()

            # Execute command if one exists
            self.set_status(self.LOADING)

            # Check if there is any command that needs to be run
            has_command = self.module.get_command(self.script_task) is not None

            # Create the specific processor for the task
            if has_command:
                # Get processor capable of running job
                self.proc = self.platform.get_instance(cpus, mem, disk_space, task_id=self.task.get_ID(), force_standard=force_standard, script_task=self.script_task)
                logging.debug("(%s) Successfully acquired processor!" % self.task.get_ID())
            else:
                # Get small processor
                self.proc = self.platform.get_instance(1, 1, disk_space, task_id=self.task.get_ID(), force_standard=force_standard, script_task=self.script_task)
                logging.debug("(%s) Successfully acquired processor!" % self.task.get_ID())

            # Check to see if pipeline has been cancelled
            self.__check_cancelled()

            # Obtain final directories
            final_out_dir, final_tmp_dir = self.get_new_output_dirs()

            # Create module executor
            self.module_executor = ModuleExecutor(task_id=self.task.get_ID(),
                                                  processor=self.proc,
                                                  final_output_dir=final_out_dir,
                                                  final_tmp_dir= final_tmp_dir,
                                                  docker_image=docker_image)

            # Check to see if pipeline has been cancelled
            self.__check_cancelled()

            # Run the command if there is any command to be run
            if has_command:

                # Load task inputs onto module executor
                self.module_executor.load_input(input_files)

                # Check to see if pipeline has been cancelled
                self.__check_cancelled()

                # Update module's command to reflect changes to input paths
                self.set_status(self.RUNNING)
                self.cmd = self.module.update_command()

                if not self.module.is_resumable:
                    logging.debug("Module (%s) is not resumable adding checkpoint(s)!" % self.module.get_ID())
                    self.proc.add_checkpoint()  # mark a checkpoint after all the input is done

                # Check if we received a list of commands or only one
                if isinstance(self.cmd, list):

                    logging.info("Task '{0}' has a list of commands, so we will run them sequentially.".format(
                        self.task.get_ID()))

                    # Initialize the output and error placeholders
                    out, err = None, None

                    # Process each command
                    for cmd_id, cmd in enumerate(self.cmd):

                        # Create a unique job_name
                        job_name = "{0}_{1}".format(self.task.get_ID(), cmd_id)

                        # Run the actual command
                        out, err = self.module_executor.run(cmd, job_name=job_name)

                        # Check to see if pipeline has been cancelled
                        self.__check_cancelled()

                    # Post-process only last command output if necessary
                    if self.module.does_process_output:
                        # wait for all processes if processor runs them in a batch
                        if not self.script_task:
                            if self.proc.batch_processing:
                                # wait for all processes
                                out, err = self.proc.wait(return_last_task_log=True)
                            self.module.process_cmd_output(out, err)

                    if not self.module.is_resumable:
                        self.proc.add_checkpoint(False)  # mark a checkpoint after the command(s) have been run

                else:

                    # Run the actual command
                    out, err = self.module_executor.run(self.cmd)

                    # Check to see if pipeline has been cancelled
                    self.__check_cancelled()

                    # Post-process command output if necessary
                    if self.module.does_process_output:
                        # wait for all processes if processor runs them in a batch
                        if not self.script_task:
                            if self.proc.batch_processing:
                                # wait for all processes
                                out, err = self.proc.wait(return_last_task_log=True)
                            self.module.process_cmd_output(out, err)

                    if not self.module.is_resumable:
                        self.proc.add_checkpoint(False)  # mark a checkpoint after the command has been run

            if not self.proc.batch_processing:
                # Set the status to finalized
                self.set_status(self.FINALIZING)

            # Save output files in workspace output dirs (if any)
            output_files = self.datastore.get_task_output_files(self.task.get_ID())
            final_output_types = self.task.get_final_output_keys()
            if len(output_files) > 0:
                self.module_executor.save_output(output_files, final_output_types)

            # wait for all processes if processor runs them in a batch
            if self.proc.batch_processing:
                # add return logs to the commands before running
                self.module_executor.save_logs()
                # wait for all processes
                self.proc.wait()

                # Set the status to finalized
                self.set_status(self.FINALIZING)
            
            if self.script_task:
                self.script_task.input_values = {x: self.module.get_argument(x) for x in self.module.get_arguments()}

            if len(output_files) > 0:
                if not self.script_task:
                    self.module_executor.update_file_sizes(output_files)
                else:
                    self.script_task.output_files = [x.path for x in output_files]

            # Indicate that task finished without any errors
            if not self.__cancelled:
                with self.status_lock:
                    self.__err = False

        except BaseException as e:
            # Handle but do not raise exception if job was externally cancelled
            if self.__cancelled:
                logging.warning("Task '%s' failed due to cancellation!" % self.task.get_ID())

            else:
                # Raise exception if job failed for any reason other than cancellation
                self.set_status(self.FINALIZING)
                logging.error(f"Task '{self.task.get_ID()}' failed!")
                raise
        finally:
            # Return logs and destroy processor if they exist
            logging.debug("TaskWorker '%s' cleaning up..." % self.task.get_ID())
            self.__clean_up()
            # Notify that task worker has completed regardless of success
            self.set_status(TaskWorker.COMPLETE)

    def cancel(self):
        # Cancel pipeline during runtime

        # Don't do anything if task has already finished or is finishing
        if self.get_status() in [self.COMPLETE, self.CANCELLING, self.FINALIZED]:
            return

        # Set pipeline to cancelling and stop any currently running jobs
        logging.error("Task '%s' cancelled!" % self.task.get_ID())
        self.set_status(self.CANCELLING)
        self.__cancelled = True

        if self.proc is not None:
            if self.proc.stoppable:
                # Prevent further commands from being run on processor
                self.proc.stop()
            # Start garbage collector thread to destroy processor
            self.garbage_collector = GarbageCollector(proc=self.proc)
            self.garbage_collector.start()

    def is_success(self):
        return not self.__err

    def is_cancelled(self):
        with self.status_lock:
            return self.__cancelled

    def __clean_up(self):

        # Do nothing if errors occurred before processor was even created
        if self.proc is None:
            return

        # Try to return task log
        try:
            # Unlock processor if it's been locked so logs can be returned
            if self.module_executor is not None and not self.__cancelled:
                self.module_executor.save_logs()
        except BaseException as e:
            logging.error("Unable to return logs for task '%s'!" % self.task.get_ID())
            if str(e) != "":
                logging.error("Received following error:\n%s" % e)

        # Try to destroy platform if it's not off
        try:

            # Destroy processor
            self.proc.destroy()

        except BaseException as e:
            logging.error("Unable to destroy processor '%s' for task '%s'" % (self.proc.get_name(), self.task.get_ID()))
            if str(e) != "":
                logging.error("Received following error:\n%s" % e)

    def __compute_disk_requirements(self, input_files, docker_image, input_multiplier=None):
        # Compute size of disk needed to store input/output files
        input_size = 0

        # Add size of docker image if one needs to be loaded for task
        if docker_image is not None:
            input_size += docker_image.get_size()

        # Add sizes of each input file
        for input_file in input_files:
            if not input_file.get_size() and self.script_task:
                self.script_task.calculate_storage = True
                continue
            # Overestimate for gzipped files
            if input_file.get_path().endswith(".gz"):
                input_size += input_file.get_size()*5
            else:
                input_size += input_file.get_size()

        # Obtain the input multiplier if not provided
        if input_multiplier is None:
            input_multiplier = int(self.platform.config.get("input_multiplier", 5))

        # Set size of desired disk
        disk_size = int(math.ceil(input_multiplier * input_size))
        logging.debug(f"({self.task.get_ID()}) Calculated Input size: {disk_size}."
                      f" Input Multiplier: {input_multiplier}")

        # Make sure platform can create a disk that size
        min_disk_size = self.platform.get_min_disk_space()
        max_disk_size = self.platform.get_max_disk_space()

        # Obtain platform disk image
        disk_image_size = self.platform.get_disk_image_size()

        # Must be at least as big as minimum disk size + disk image
        disk_size = disk_size + disk_image_size + min_disk_size

        # And smaller than max disk size
        if disk_size > max_disk_size:
            logging.warning("(%s) Current task disk size (%s GB) exceeds the maximum disk size enforced "
                            "by the platform (%s GB). Disk size will be set to the platform maximum!")
            disk_size = max_disk_size

        logging.debug(f"({self.task.get_ID()}) Min Disk Size: {min_disk_size}. Disk Image Size: {disk_image_size}. "
                      f"Total (including input): {disk_size}")

        return disk_size

    def __check_cancelled(self):
        if self.__cancelled:
            raise RuntimeError("(%s) Task failed due to cancellation!")


class GarbageCollector(threading.Thread):
    def __init__(self, proc):
        super(GarbageCollector, self).__init__()

        # Setting node thread as daemon
        self.daemon = True

        # Setting a variable for error message that might appear
        self.proc = proc

    def run(self):
        logging.debug("GarbageCollector destroying processor: {0}".format(self.proc.get_name()))
        self.proc.destroy()
