import os
import logging
import abc
import subprocess as sp
import time
from collections import OrderedDict

from System.Platform import Process


class CloudInstance(object, metaclass=abc.ABCMeta):

    OFF         = 0  # Destroyed or not allocated on the cloud at all
    CREATING    = 1  # Instance is being created/provisioned/allocated
    DESTROYING  = 2  # Instance is being destroyed
    AVAILABLE   = 3  # Available for running processes

    STATUSES    = ["OFF", "CREATING", "DESTROYING", "AVAILABLE"]

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        # Initialize main instance information
        self.name       = name
        self.nr_cpus    = nr_cpus
        self.mem        = mem
        self.disk_space = disk_space

        # Obtain the CloudConductor SSH private key from platform
        self.ssh_private_key = kwargs.pop("ssh_private_key")
        self.ssh_connection_user = kwargs.pop("ssh_connection_user")

        # Obtain identify and secret from platform
        self.identity = kwargs.pop("identity")
        self.secret = kwargs.pop("secret")

        # Obtain location specific information
        self.region = kwargs.pop("region")
        self.zone = kwargs.pop("zone")

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

        # Initialize external IP address
        self.external_IP = None


    def create(self):

        # Add creation event to instance history
        self.__add_history_event("CREATE")

        # Create the actual instance
        self.external_IP = self.create_instance()

        # Check if external IP was set
        if self.external_IP is None:
            logging.error(f'({self.name}) No IP address was provided by the create() method!')
            raise NotImplementedError(f'({self.name}) Create() method for {self.__class__.__name__} did not return '
                                      f'an IP address! Please check the documentation and method implementation.')

        # Wait until instance is ready (aka the SSH server is responsive)
        self.__wait_until_ready()

        # Return an instance of self
        return self

    def destroy(self):

        while True:

            # Get the current instance status
            status = self.get_status()

            # If status is OFF then the instance was destroyed
            if status == CloudInstance.OFF:
                self.__add_history_event("DESTROY")
                break

            # If status is not DESTROYING then we destroy the instance
            elif status != CloudInstance.DESTROYING:
                self.destroy_instance()

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
            cmd = f"sudo docker run --rm --user root -v {self.wrk_dir}:{self.wrk_dir} {docker_image} " \
                f"/bin/bash -c '{cmd}'"

        # Modify quotation marks to be able to send through SSH
        cmd = cmd.replace("'", "'\"'\"'")

        # Wrap the command around ssh
        cmd = f"ssh -i {self.ssh_private_key} " \
            f"-o CheckHostIP=no -o StrictHostKeyChecking=no " \
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
        if self.handle_failure(proc_obj):
            logging.warning(f"({self.name}) Process '{proc_name}' failed but we will retry it!")
            self.run(job_name=proc_name,
                     cmd=proc_obj.get_command(),
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

    def handle_failure(self, proc_obj):
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

                # Add cost since last start-up
                total_compute_cost += (event["timestamp"] - instance_is_on) / 3600.0 * compute_cost

                # Mark the instance shut down and no compute cost present
                instance_is_on = None
                compute_cost = 0

            # Calculate storage cost
            if event["type"] == "CREATE" and storage_is_present is None:

                # Mark the storage creation and get its cost
                storage_is_present = event["timestamp"]
                storage_cost = float(event["price"]["storage"])

            elif event["type"] == "DESTROY" and storage_is_present is not None:

                # Add cost since last start-up
                total_storage_cost += (event["timestamp"] - storage_is_present) * storage_cost

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

            # Check if ssh server is accessible
            if self.__check_ssh():
                needs_recreate = False
                break

            # If instance is not creating, it means it does not exist on the cloud or it's stopped
            if self.get_status() not in [CloudInstance.CREATING, CloudInstance.AVAILABLE]:
                logging.debug(f'({self.name}) Instance has been shut down, removed, or preempted. Resetting instance!')
                break

        # Check if it needs resetting
        if needs_recreate:
            # TODO: Should we reset here or recreate?
            self.recreate()
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
        self.history.append({
            "type": _type,
            "timestamp": time.time() if _timestamp is None else _timestamp,
            "price": {
                "compute": self.get_compute_price(),
                "storage": self.get_storage_price()
            }
        })

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

    @abc.abstractmethod
    def get_status(self):
        pass

    @abc.abstractmethod
    def get_compute_price(self):
        pass

    @abc.abstractmethod
    def get_storage_price(self):
        pass
