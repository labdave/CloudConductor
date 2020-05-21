import logging
import time
import subprocess as sp

from System.Platform import Process
from System.Platform.Instance import CloudInstance
from System.Platform.Amazon import AmazonInstance


class AmazonSpotInstance(AmazonInstance):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        super(AmazonSpotInstance, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        self.is_preemptible = True

        # Attributes for handling instance resets
        self.max_resets = kwargs.pop("max_resets", 6)
        self.reset_count = 0

    def handle_failure(self, proc_name, proc_obj):
        if not self.is_preemptible:
            return super(AmazonSpotInstance, self).handle_failure(proc_name, proc_obj)

        # Determine if command can be retried
        can_retry   = self.default_num_cmd_retries != 0 and proc_obj.get_num_retries() > 0
        needs_reset = False

        logging.warning("(%s) Handling failure for proc '%s'" % (self.name, proc_name))
        logging.debug("(%s) Error code: %s" % (self.name, proc_obj.returncode))

        # Check if we receive public key error
        if "permission denied (publickey)." in proc_obj.err.lower():
            self.reset(force_destroy=True)
            return can_retry

        if proc_obj.returncode == 255:
            logging.warning("(%s) Waiting for 60 seconds to make sure instance wasn't preempted..." % self.name)
            time.sleep(60)

            # Resolve case when SSH server resets/closes the connection
            if "connection reset by" in proc_obj.err.lower() \
                    or "connection closed by" in proc_obj.err.lower():
                self.reset(force_destroy=True)
                return can_retry

        # Get the status from the cloud
        curr_status = self.get_status(log_status=True)

        # Re-run any command (except create) if instance is up and cmd can be retried
        if curr_status == CloudInstance.AVAILABLE:
            if proc_name == "create" and "already exists" not in proc_obj.err:
                # Sometimes create works but returns a failure
                # Just need to make sure the failure wasn't due to instance already existing
                return can_retry

            # Retry command if retries are left and command isn't 'create'
            return can_retry and proc_name != "create"

        # Re-run destroy command if instance is creating and cmd has enough retries
        elif curr_status == CloudInstance.CREATING:
            return proc_name == "destroy" and can_retry

        elif curr_status == CloudInstance.DESTROYING:
            # Re-run destroy command

            # Instance is destroying itself and we know why (we killed it programmatically)
            if proc_name == "destroy":
                return can_retry

            # Reset instance and re-run command if it failed and we're not sure why the instance is destroying itself (e.g. preemption)
            elif "destroy" not in self.processes and proc_name not in ["create", "destroy"]:
                needs_reset = True

        elif curr_status == CloudInstance.OFF or curr_status == CloudInstance.TERMINATED:
            # Don't do anythying if destroy failed but instance doesn't actually exist anymore
            if proc_name == "destroy":
                logging.debug("(%s) Instance already destroyed!" % self.name)
                return False

            # Handle cases where we have no idea why the instance doesn't currently exist (e.g. preemption, manual deletion)
            # Retry if 'create' command failed and instance doesn't exist
            if "destroy" not in self.processes and proc_name == "create" and proc_obj.get_num_retries() > 0:
                return True

            # Reset instance and re-run command if command failed and not sure why instance doesn't exist (e.g. preemption, gets manually deleted)
            elif "destroy" not in self.processes:
                needs_reset = True

        logging.debug("(%s) Curr_status, can_retry, needs_reset are: %s, %s, %s" % (self.name, curr_status, can_retry, needs_reset))

        # Reset instance if its been destroyed/disappeared unexpectedly (i.e. preemption)
        if needs_reset and self.is_preemptible:
            logging.warning("(%s) Instance preempted! Resetting..." % self.name)
            self.reset()
            return can_retry

        # Check if the problem is that we cannot SSH in the instance
        elif proc_obj.returncode == 255 and not self.check_ssh():
            logging.warning("(%s) SSH connection cannot be established! Resetting..." % self.name)
            self.reset()
            return can_retry

        # Raise error if command failed, has no retries, and wasn't caused by preemption
        else:
            raise RuntimeError("(%s) Instance command has failed, has no retries, and failure was not caused by preemption" %
                               self.name)

    def reset(self, force_destroy=False):

        # Resetting takes place just for preemptible instances
        if not self.is_preemptible:
            return

        # Incrementing the reset count and checking if it reached the threshold
        self.reset_count += 1
        logging.info(f"({self.name}) This is reset attempt #{self.reset_count}. Max retries is {self.max_resets} attempts.")
        if self.reset_count > self.max_resets:
            logging.warning("(%s) Instance failed! Instance preempted and has reached the maximum number of resets (num resets: %s). "
                            "Resetting as standard instance." % (self.name, self.max_resets))
            # Switch to non-preemptible instance
            self.is_preemptible = False

        status = self.get_status()

        # Restart the instance if it is preemptible and is not required to be destroyed
        if self.is_preemptible and not force_destroy and status != CloudInstance.TERMINATED:
            try:
                # Restart the instance
                while status != CloudInstance.OFF and status != CloudInstance.TERMINATED and status != CloudInstance.AVAILABLE:
                    logging.warning("(%s) Waiting for 30 seconds for instance to stop" % self.name)
                    time.sleep(30)
                    status = self.get_status(log_status=True)

                if status == CloudInstance.AVAILABLE:
                    # Instance restart complete
                    logging.debug("(%s) Instance is running, continue running processes!" % self.name)
                if status == CloudInstance.OFF:
                    self.start()
                    # Instance restart complete
                    logging.debug("(%s) Instance restarted, continue running processes!" % self.name)
                if status == CloudInstance.TERMINATED:
                    force_destroy = True
                    logging.debug(f"({self.name}) Failed to stop instance. ResourceNotFound... recreating.")

                    # Recreate the instance
                    self.recreate()

                    # Instance recreation complete
                    logging.debug("(%s) Instance recreated, rerunning all processes!" % self.name)
            except Exception as e:
                if 'notFound' in str(e):
                    force_destroy = True
                    logging.debug(f"({self.name}) Failed to stop instance. ResourceNotFound... recreating.")

                    # Recreate the instance
                    self.recreate()

                    # Instance recreation complete
                    logging.debug("(%s) Instance recreated, rerunning all processes!" % self.name)

        else:
            # Recreate the instance
            self.recreate()

            # Instance recreation complete
            logging.debug("(%s) Instance recreated, rerunning all processes!" % self.name)

        # Rerun all commands if the instance is not preemptible or was previously destroyed
        if not self.is_preemptible or force_destroy:

            # Rerun all commands
            for proc_name, proc_obj in list(self.processes.items()):

                # Skip processes that do not need to be rerun
                if proc_name in ["create", "destroy", "start", "stop"]:
                    continue

                # Run and wait for the command to finish
                self.run(job_name=proc_name,
                         cmd=proc_obj.get_command(),
                         docker_image=proc_obj.get_docker_image())
                self.wait_process(proc_name)

            # Exit function as the rest of the code is related to an instance that was not destroyed
            return

        # Identifying which process(es) need(s) to be recalled
        commands_to_run = list()
        checkpoint_queue = list()
        add_to_checkpoint_queue = False
        fail_to_checkpoint = False
        checkpoint_commands = [i[0] for i in self.checkpoints]  # create array of just the commands
        logging.debug(f"({self.name}) CHECKPOINT COMMANDS: {str(checkpoint_commands)}")
        cleanup_output = False
        for proc_name, proc_obj in list(self.processes.items()):

            # Skip processes that do not need to be rerun
            if proc_name in ["create", "destroy", "start", "stop"]:
                continue

            # Skip processes that were successful and complete
            if not proc_obj.has_failed() and proc_obj.complete:
                continue

            # Adding to the checkpoint queue since we're past a checkpoint marker
            if add_to_checkpoint_queue:
                checkpoint_queue.append(proc_name)

                # if a process hasn't been completed, a process may have failed before the checkpoint
                # so we need to add all those to the list to be run
                fail_to_checkpoint = True

            else:
                commands_to_run.append(proc_name)

            # Hit a checkpoint marker, start adding to the checkpoint_queue after this process
            if proc_name in checkpoint_commands:
                if fail_to_checkpoint:
                    if cleanup_output:
                        self.__remove_wrk_out_dir()

                    # Add all the commands in the checkpoint queue to commands to run
                    commands_to_run.extend(checkpoint_queue)

                # Obtain the cleanup status of the current checkpoint
                cleanup_output = [d[1] for d in self.checkpoints if d[0] == proc_name][0]
                logging.debug("CLEAR OUTPUT IS: %s FOR process %s" % (str(cleanup_output), str(proc_name)))

                # Clear the list if we run into a new checkpoint command
                checkpoint_queue = list()
                add_to_checkpoint_queue = True

        # Still have processes in the checkpoint queue
        if len(checkpoint_queue) > 0:
            if fail_to_checkpoint:
                if cleanup_output:
                    self.__remove_wrk_out_dir()

                # Add all the commands in the checkpoint queue to commands to run
                commands_to_run.extend(checkpoint_queue)

        # Set commands that need to be rerun to rerun mode, so they are all being rerun
        for proc_to_rerun in commands_to_run:
            self.processes[proc_to_rerun].set_to_rerun()

        # Log which commands will be rerun
        logging.debug(f"({self.name}) Commands to be rerun: ({str([proc_name for proc_name, proc_obj in list(self.processes.items()) if proc_obj.needs_rerun()])}) ")

        # Rerunning all the commands that need to be rerun
        for proc_name, proc_obj in list(self.processes.items()):
            if proc_obj.needs_rerun():
                self.run(job_name=proc_name,
                         cmd=proc_obj.get_command(),
                         docker_image=proc_obj.get_docker_image())
                self.wait_process(proc_name)

    def __remove_wrk_out_dir(self):

        logging.debug(f"({self.name}) CLEARING OUTPUT for checkpoint cleanup, clearing {self.wrk_out_dir}.")

        # Generate the removal command. HAS to be 'sudo' to be able to remove files created by any user.
        cmd = f"sudo rm -rf {self.wrk_out_dir}*"

        # Clean the working output directory
        self.run("cleanup_work_output", cmd)
        self.wait_process("cleanup_work_output")

    def get_recent_start_time(self):
        # Return the timestamp of the most recent CREATE event
        for event in reversed(self.history):
            if event["type"] == "CREATE":
                return event["timestamp"]

        return None
