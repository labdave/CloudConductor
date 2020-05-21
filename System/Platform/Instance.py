import os
import logging
import abc
import subprocess as sp
import time
import socket
from collections import OrderedDict

from System.Platform import Process


class Instance(object, metaclass=abc.ABCMeta):

    OFF         = 0  # Stopped or not allocated on the cloud at all
    CREATING    = 1  # Instance is being created/provisioned/allocated
    DESTROYING  = 2  # Instance is being destroyed
    AVAILABLE   = 3  # Available for running processes
    TERMINATED  = 4  # Destroyed instance

    STATUSES    = ["OFF", "CREATING", "DESTROYING", "AVAILABLE", "TERMINATED"]

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        # Initialize main instance information
        self.name       = name
        self.nr_cpus    = nr_cpus
        self.mem        = mem
        self.disk_space = disk_space

        # Obtain identify and secret from platform
        self.identity = kwargs.pop("identity")
        self.secret = kwargs.pop("secret")

        # Initialize the workspace directories
        self.wrk_dir = "/data"
        self.wrk_log_dir = f"{self.wrk_dir}/log"
        self.wrk_out_dir = f"{self.wrk_dir}/output"

        # Default number of times to retry commands if none specified at command runtime
        self.default_num_cmd_retries = kwargs.pop("cmd_retries", 3)
        self.recreation_count = 0
        self.reset_count = 0

        # Ordered dictionary of processing being run by processor
        self.processes  = OrderedDict()

        # Initialize the event history of the instance
        self.history = []

        # Initialize the checkpoints of the instance
        self.checkpoints = []

    def get_name(self):
        return self.name

    def get_runtime(self):
        return self.get_stop_time() - self.get_start_time()

    def wait(self):
        raise NotImplementedError("Make a system to wait for all processes")

    # ABSTRACT METHODS TO BE IMPLEMENTED BY INHERITING CLASSES

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def wait_process(self, proc_name):
        pass

    @abc.abstractmethod
    def finalize(self):
        pass

    @abc.abstractmethod
    def get_start_time(self):
        pass

    @abc.abstractmethod
    def get_stop_time(self):
        pass

    @abc.abstractmethod
    def get_status(self, log_status=False):
        pass

    @abc.abstractmethod
    def add_checkpoint(self, clear_output=True):
        pass

    @abc.abstractmethod
    def compute_cost(self):
        pass

    @abc.abstractmethod
    def get_compute_price(self):
        pass

    @abc.abstractmethod
    def get_storage_price(self):
        pass


class CloudInstance(Instance, metaclass=abc.ABCMeta):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        super(CloudInstance, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Obtain location specific information
        self.region = kwargs.pop("region")
        self.zone = kwargs.pop("zone")

        # Obtain the mother platform object
        self.platform = kwargs.pop("platform")

        # Obtain disk image
        self.disk_image = kwargs.pop("disk_image")

        # Obtain the CloudConductor SSH private key from platform
        self.ssh_private_key = kwargs.pop("ssh_private_key")
        self.ssh_connection_user = kwargs.pop("ssh_connection_user")

        # Initialize external IP address
        self.external_IP = None

    def create(self):

        # Allocate resources on the platform for current instance
        self.platform.allocate_resources(self.nr_cpus, self.mem, self.disk_space)

        # Create the actual instance
        self.external_IP = self.create_instance()

        # Add creation event to instance history
        self.__add_history_event("CREATE")

        # Check if external IP was set
        if self.external_IP is None:
            logging.error(f'({self.name}) No IP address was provided by the create_instance() method!')
            raise NotImplementedError(f'({self.name}) Create() method for {self.__class__.__name__} did not return '
                                      f'an IP address! Please check the documentation and method implementation.')

        # Wait until instance is ready (aka the SSH server is responsive)
        self.__wait_until_ready()

        # Run post_startup_tasks
        self.post_startup()

        # Return an instance of self
        return self

    def destroy(self):

        while True:

            # Get the current instance status
            status = self.get_status()

            # If status is OFF then the instance was destroyed
            if status == CloudInstance.OFF or status == CloudInstance.TERMINATED:
                self.__add_history_event("DESTROY")
                break

            # If status is not DESTROYING then we destroy the instance
            elif status != CloudInstance.DESTROYING:
                self.destroy_instance()

                # Allocate resources on the platform for current instance
                self.platform.deallocate_resources(self.nr_cpus, self.mem, self.disk_space)

            # Wait for 10 seconds before checking again for status
            time.sleep(10)

    def recreate(self):

        # Check if we recreated too many times already
        if self.recreation_count > self.default_num_cmd_retries:
            logging.debug("(%s) Instance successfully created but "
                          "never became available after %s resets!" %
                          (self.name, self.default_num_cmd_retries))

            raise RuntimeError("(%s) Instance successfully created but never"
                               " became available after multiple tries!" %
                               self.name)

        # Recreate instance
        self.destroy()
        self.create()

        # Increment the recreation count
        self.recreation_count += 1

    def start(self):

        # Add history event
        self.__add_history_event("START")

        # Start instance
        self.external_IP = self.start_instance()

        # Check if external IP was set
        if self.external_IP is None:
            logging.error(f'({self.name}) No IP address was provided by the start() method!')
            raise NotImplementedError(f'({self.name}) Start() method for {self.__class__.__name__} did not return '
                                      f'an IP address! Please check the documentation and method implementation.')

        # Wait until instance is ready (aka the SSH server is responsive)
        self.__wait_until_ready()

    def stop(self):

        # Stop instance
        self.stop_instance()

        # Add history event
        self.__add_history_event("STOP")

    def reset(self):
        if self.reset_count > self.default_num_cmd_retries:
            logging.debug("(%s) Instance successfully started but "
                          "never became available after %s resets!" %
                          (self.name, self.default_num_cmd_retries))

            raise RuntimeError("(%s) Instance successfully started but never"
                               " became available after multiple tries!" %
                               self.name)

        # Recreate instance
        self.stop()
        self.start()

        # Increment the recreation count
        self.reset_count += 1

    def run(self, job_name, cmd, num_retries=None, docker_image=None):

        # Checking if logging is required
        if "!LOG" in cmd:

            # Generate name of log file
            log_file = f"{job_name}.log"
            if self.wrk_log_dir is not None:
                log_file = os.path.join(self.wrk_log_dir, log_file)

            # Generating all the logging pipes
            log_cmd_null    = " >>/dev/null 2>&1 "
            log_cmd_stdout  = f" >>{log_file}"
            log_cmd_stderr  = f" 2>>{log_file}"
            log_cmd_all     = f" >>{log_file} 2>&1"

            # Replacing the placeholders with the logging pipes
            cmd = cmd.replace("!LOG0!", log_cmd_null)
            cmd = cmd.replace("!LOG1!", log_cmd_stdout)
            cmd = cmd.replace("!LOG2!", log_cmd_stderr)
            cmd = cmd.replace("!LOG3!", log_cmd_all)

        # Save original command
        original_cmd = cmd

        # Run in docker image if specified
        if docker_image is not None:
            cmd = f"sudo docker run --rm --user root -v {self.wrk_dir}:{self.wrk_dir} --entrypoint '/bin/bash' {docker_image} " \
                f"-c '{cmd}'"

        # Modify quotation marks to be able to send through SSH
        cmd = cmd.replace("'", "'\"'\"'")

        # Wrap the command around ssh
        cmd = f"ssh -i {self.ssh_private_key} " \
            f"-o CheckHostIP=no -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o TCPKeepAlive=yes " \
            f"{self.ssh_connection_user}@{self.external_IP} -- '{cmd}'"

        # Run command using subprocess popen and add Popen object to self.processes
        logging.info("(%s) Process '%s' started!" % (self.name, job_name))
        logging.debug("(%s) Process '%s' has the following command:\n    %s" % (self.name, job_name, original_cmd))

        # Generating process arguments
        kwargs = {

            # Add Popen specific arguments
            "shell": True,
            "stdout": sp.PIPE,
            "stderr": sp.PIPE,
            "close_fds": True,

            # Add CloudConductor specific arguments
            "original_cmd": original_cmd,
            "num_retries": self.default_num_cmd_retries if num_retries is None else num_retries,
            "docker_image": docker_image
        }

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def wait_process(self, proc_name):

        # Get process from process list
        proc_obj = self.processes[proc_name]

        # Wait for process to finish
        proc_obj.wait_completion()

        # If process is complete with no failure return the output
        if not proc_obj.has_failed():
            logging.info(f"({self.name}) Process '{proc_name}' complete!")
            return proc_obj.get_output()

        # Retry process if it can be retried
        if self.handle_failure(proc_name, proc_obj):
            stdout, stderr = proc_obj.get_output()
            logging.warning(f"({self.name}) Process '{proc_name}' failed but we will retry it!")
            logging.warning(f"({self.name}) Process '{proc_name}' had the following issue: {stderr}")
            cmd = proc_obj.get_command()
            # alter aws s3 cmd to try recursive vs. non-recursive
            if 'aws s3 cp' in cmd:
                if '--recursive' in cmd:
                    cmd = cmd.replace('--recursive', '')
                else:
                    cmd = cmd.replace('aws s3 cp', 'aws s3 cp --recursive')
            if 'ssh' in stderr:
                # issue with ssh connection, sleep for 10 seconds in case the server was having trouble with connections/commands
                time.sleep(10)
            self.run(job_name=proc_name,
                     cmd=cmd,
                     num_retries=proc_obj.get_num_retries()-1,
                     docker_image=proc_obj.get_docker_image())
            return self.wait_process(proc_name)

        # Process still failing and cannot be retried anymore
        logging.error(f"({self.name}) Process '{proc_name}' failed!")

        # Log the output
        stdout, stderr = proc_obj.get_output()
        logging.debug(f"({self.name}) The following output/error was received:"
                        f"\n\nSTDOUT:\n{stdout}"
                        f"\n\nSTDERR:\n{stderr}")

        # Raise an error
        raise RuntimeError(f"({self.name}) Instance failed at process '{proc_name}'!")

    def handle_failure(self, proc_name, proc_obj):
        return self.default_num_cmd_retries != 0 and proc_obj.get_num_retries() > 0

    def compute_cost(self):
        # Compute running cost of current task processor

        # Copy the instance history
        history = self.history.copy()

        # TODO: Maybe sort based on timestamp

        # Initialize total values
        total_compute_cost = 0
        compute_cost = 0
        total_storage_cost = 0
        storage_cost = 0

        # Initialize status timestamp
        instance_is_on = None
        storage_is_present = None

        while history:

            # Get first element of history
            event = history.pop(0)

            # Calculate compute cost
            if event["type"] in ["CREATE", "START"] and instance_is_on is None:

                # Mark the instance start-up and get its cost
                instance_is_on = event["timestamp"]
                compute_cost = float(event["price"]["compute"])

            elif event["type"] in ["DESTROY", "STOP"] and instance_is_on is not None:

                # Calculate time delta in hours
                time_delta = (event["timestamp"] - instance_is_on) / 3600.0
                logging.info(f"Compute Cost calc for {self.name} is {time_delta} * {compute_cost}")

                # Add cost since last start-up
                total_compute_cost += time_delta * compute_cost

                logging.info(f"Total Compute Cost for {self.name} is {total_compute_cost}")

                # Mark the instance shut down and no compute cost present
                instance_is_on = None
                compute_cost = 0

            # Calculate storage cost
            if event["type"] == "CREATE" and storage_is_present is None:

                # Mark the storage creation and get its cost
                storage_is_present = event["timestamp"]
                storage_cost = float(event["price"]["storage"])

            elif event["type"] == "DESTROY" and storage_is_present is not None:

                # Calculate time delta
                time_delta = (event["timestamp"] - storage_is_present) / 3600.0
                logging.info(f"Storage Cost calc for {self.name} is {time_delta} * {storage_cost}")

                # Add cost since last start-up
                total_storage_cost += time_delta * storage_cost

                logging.info(f"Total Storage Cost for {self.name} is {total_storage_cost}")

                # Mark the storage are removed and no storage cost present
                storage_is_present = None
                storage_cost = 0

        return total_compute_cost + total_storage_cost

    def __wait_until_ready(self):
        # Wait until instance can be SSHed

        # Initialize the SSH status to False and assume that the instance will need to be recreated
        self.ssh_ready = False
        needs_recreate = True

        # Initializing the cycle count
        cycle_count = 0

        # Waiting for 10 minutes for instance to be SSH-able
        while cycle_count < 20:

            # Increment the cycle count
            cycle_count += 1

            # Wait for 15 seconds before checking the SSH server and status again
            time.sleep(30)

            status = self.get_status(log_status=True)

            # If instance is not creating, it means it does not exist on the cloud or it's stopped
            if status not in [CloudInstance.CREATING, CloudInstance.AVAILABLE]:
                logging.debug(f'({self.name}) Instance has been shut down, removed, or preempted. Resetting instance!')
                break

            # Check if ssh server is accessible
            if self.__check_ssh():
                needs_recreate = False
                break

        # Check if it needs resetting
        if needs_recreate:
            # TODO: Should we reset here or recreate?
            self.reset()
        else:
            # If no resetting is needed, then we are all set!
            self.ssh_ready = True
            logging.debug(f'({self.name}) Instance can be accessed through SSH!')

    def __check_ssh(self):

        # If the instance is off, the ssh is definitely not ready
        if self.external_IP is None:
            return False

        # Generate the command to run
        cmd = "nc -w 1 {0} 22".format(self.external_IP)

        # Run the command
        proc = sp.Popen(cmd, stderr=sp.PIPE, stdout=sp.PIPE, shell=True)
        out, err = proc.communicate()

        # Convert to string formats
        out = out.decode("utf8")
        err = err.decode("utf8")

        # If any error occured, then the ssh is not ready
        if err:
            return False

        # Otherwise, return only if there is ssh in the received header
        return "ssh" in out.lower()

    def __add_history_event(self, _type, _timestamp=None):
        # make sure not to add duplicate events
        if len(self.history) == 0:
            self.history.append({
                "type": _type,
                "timestamp": time.time() if _timestamp is None else _timestamp,
                "price": {
                    "compute": self.get_compute_price(),
                    "storage": self.get_storage_price()
                }
            })
        elif len(self.history) > 0 and self.history[len(self.history)-1]['type'] != _type:
            self.history.append({
                "type": _type,
                "timestamp": time.time() if _timestamp is None else _timestamp,
                "price": {
                    "compute": self.get_compute_price(),
                    "storage": self.get_storage_price()
                }
            })

    def get_start_time(self):

        # Return the timestamp of the first CREATE event
        for event in self.history:
            if event["type"] == "CREATE":
                return event["timestamp"]

        return None

    def get_stop_time(self):

        # Return the timestamp of the last DESTROY event
        for event in reversed(self.history):
            if event["type"] == "DESTROY":
                return event["timestamp"]

        return time.time()

    def add_checkpoint(self, clear_output=True):
        """ Function for setting where processor should fall back to in case of a preemption.
            -clear_output: Flag to indicate that, in case of preemption, the task's output directory needs to be cleared.
        """
        self.checkpoints.append((next(reversed(self.processes)), clear_output))

    def post_startup(self):
        pass

    # ABSTRACT METHODS TO BE IMPLEMENTED BY INHERITING CLASSES

    @abc.abstractmethod
    def create_instance(self):
        pass

    @abc.abstractmethod
    def destroy_instance(self):
        pass

    @abc.abstractmethod
    def start_instance(self):
        pass

    @abc.abstractmethod
    def stop_instance(self):
        pass
