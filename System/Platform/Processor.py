import os
import logging
import abc
from collections import OrderedDict
import subprocess as sp
import time
import threading

from System.Platform import Process

class Processor(object, metaclass=abc.ABCMeta):
    OFF         = 0  # Destroyed or not allocated on the cloud
    CREATING    = 1  # Instance is being created
    DESTROYING  = 2  # Instance is destroyed
    AVAILABLE   = 3  # Available for running processes

    STATUSES    = ["OFF", "CREATING", "DESTROYING", "AVAILABLE"]

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):
        self.name       = name
        self.nr_cpus    = nr_cpus
        self.mem        = mem
        self.disk_space = disk_space

        # Initialize the start and stop time
        self.start_time = None
        self.stop_time  = None

        # Get name of directory where logs will be written
        self.log_dir    = kwargs.pop("log_dir", None)

        # Get name of working directory
        self.wrk_dir        = kwargs.pop("wrk_dir", "/data/")
        self.wrk_out_dir    = kwargs.pop("wrk_out_dir", "/data/output")

        # Per hour price of processor
        self.price      = kwargs.pop("price",   0)

        # Default number of times to retry commands if none specified at command runtime
        self.default_num_cmd_retries = kwargs.pop("cmd_retries", 3)

        # Ordered dictionary of processing being run by processor
        self.processes  = OrderedDict()

        # Setting the instance status
        self.status_lock    = threading.Lock()
        self.status         = Processor.OFF

        # Lock for preventing further commands from being run on processor
        self.locked = False
        self.stopped = False
        self.checkpoints = []

    def create(self):
        self.set_status(Processor.AVAILABLE)

    def destroy(self, wait=True):
        self.set_status(Processor.OFF)

    def run(self, job_name, cmd, num_retries=None, docker_image=None, quiet_failure=False):

        # Throw error if attempting to run command on stopped processor
        if self.is_locked():
            logging.error("(%s) Attempt to run process'%s' on locked processor!" % (self.name, job_name))
            raise RuntimeError("Attempt to run command on locked processor!")

        if num_retries is None:
            num_retries = self.default_num_cmd_retries

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = "%s.log" % job_name
            if self.log_dir is not None:
                log_file = os.path.join(self.log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1 "
            log_cmd_stdout  = " >>%s " % log_file
            log_cmd_stderr  = " 2>>%s " % log_file
            log_cmd_all     = " >>%s 2>&1 " % log_file

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Save original command
        original_cmd = cmd

        # Run in docker image if specified
        if docker_image is not None:
            cmd = "sudo docker run --rm --user root" \
                  " -v %s:%s" \
                  " --entrypoint '/bin/bash'" \
                  " %s '-c %s'" % (self.wrk_dir, self.wrk_dir, docker_image, cmd)

        # Make any modifications to the command to allow it to be run on a specific platform
        cmd = self.adapt_cmd(cmd)

        # Run command using subprocess popen and add Popen object to self.processes
        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, original_cmd))

        # Generating process arguments
        kwargs = dict()

        # Process specific arguments
        kwargs["cmd"] = original_cmd

        # Popen specific arguments
        kwargs["shell"] = True
        kwargs["stdout"] = sp.PIPE
        kwargs["stderr"] = sp.PIPE
        kwargs["num_retries"] = num_retries
        kwargs["docker_image"] = docker_image
        kwargs["quiet_failure"] = quiet_failure
        kwargs["close_fds"] = True

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def wait(self):
        # Returns when all currently running processes have completed
        for proc_name, proc_obj in self.processes.items():
            self.wait_process(proc_name)

    def lock(self):
        # Prevent any additional processes from being run
        with threading.Lock():
            self.locked = True

    def unlock(self):
        # Allow processes to run on processor
        with threading.Lock():
            self.locked = False

    def stop(self):
        # Lock so that no new processes can be run on processor
        self.lock()

        # Kill all currently executing processes on processor
        #for proc_name, proc_obj in self.processes.iteritems():
        #    if not proc_obj.is_complete() and proc_name.lower() != "destroy":
        #        logging.debug("Killing process: %s" % proc_name)
        #        proc_obj.stop()

    ############ Getters and Setters
    def set_status(self, new_status):
        # Updates instance status with threading.lock() to prevent race conditions
        if new_status > len(Processor.STATUSES) or new_status < 0:
            logging.debug("(%s) Status level %d not available!" % (self.name, new_status))
            raise RuntimeError("Instance %s has failed!" % self.name)

        with self.status_lock:

            # Report the status change, if any
            if new_status != self.status:
                logging.info("(%s) Status has been changed to %s." % (self.name, Processor.STATUSES[new_status]))

            self.status = new_status

    def get_status(self):
        # Returns instance status with threading.lock() to prevent race conditions
        with self.status_lock:
            return self.status

    def set_log_dir(self, new_log_dir):
        self.log_dir = new_log_dir

    def set_wrk_dir(self, new_wrk_dir):
        self.wrk_dir = new_wrk_dir

    def set_wrk_out_dir(self, new_wrk_out_dir):
        if new_wrk_out_dir == "/" or new_wrk_out_dir is None:
            self.wrk_out_dir = os.path.join(self.wrk_dir, "output")
            logging.warning("(%s) Working output directory was not set or set to '/'. "
                            "Automatically setting to '%s'." % (self.name, self.wrk_out_dir))
        else:
            self.wrk_out_dir = new_wrk_out_dir

    def set_start_time(self):
        self.start_time = time.time()

    def set_stop_time(self):
        self.stop_time = time.time()

    def is_locked(self):
        with self.status_lock:
            return self.locked

    def get_name(self):
        return self.name

    def get_runtime(self):

        count = 0

        # Only try to obtain a (correct) positive runtime 5 times
        while count < 5:

            count += 1

            # Return 0 if instance hasn't started yet
            if self.start_time is None:
                runtime =  0

            # Instance is still running so register runtime since last start/restart
            elif self.stop_time is None or self.stop_time < self.start_time:
                runtime = time.time() - self.start_time

            # Instance has been stopped
            else:
                runtime = self.stop_time - self.start_time

            # If (correct) positivie runtime, return
            if runtime >= 0:
                return runtime

        else:
            # Could not obtain the runtime, so raise an exception
            logging.error("(%s) Could not obtain the processor runtime" % self.name)
            raise RuntimeError("(%s) Could not obtain the processor runtime" % self.name)

    def get_start_time(self):
        return self.start_time

    def get_nr_cpus(self):
        return self.nr_cpus

    def get_mem(self):
        return self.mem

    def get_disk_space(self):
        return self.disk_space

    def compute_cost(self):
        # Compute running cost of current task processor
        runtime = self.get_runtime()
        return self.price * runtime / 3600
    
    def add_checkpoint(self, clear_output=True):
        """ Function for setting where processor should fall back to in case of a preemption. 
            -clear_output: Flag to indicate that, in case of preemption, the task's output directory needs to be cleared.
        """
        self.checkpoints.append((next(reversed(self.processes)), clear_output))


    ############ Abstract methods
    @abc.abstractmethod
    def wait_process(self, proc_name):
        pass

    @abc.abstractmethod
    def adapt_cmd(self, cmd):
        pass
