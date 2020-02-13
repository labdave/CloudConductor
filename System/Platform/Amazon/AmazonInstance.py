import logging
import os
import subprocess as sp

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from System.Platform import CloudInstance
from System.Platform import Process


class AmazonInstance(CloudInstance):

    def __init__(self, name, nr_cpus, mem, disk_space, disk_image, **kwargs):

        super(AmazonInstance, self).__init__(name, nr_cpus, mem, disk_space, disk_image, **kwargs)

        # Create libcloud driver
        driver_class = get_driver(Provider.EC2)
        self.driver = driver_class(self.identity, self.secret, region=self.region)

        # Initialize the node variable
        self.node = None

    def get_instance_size(self):
        return "t2.medium"

    def create_instance(self):

        # Generate NodeSize for instance
        size_name = self.get_instance_size()
        node_size = [size for size in self.driver.list_sizes() if size.id == size_name][0]

        # Create instance
        node = self.driver.create_node(name=self.name,
                                  image=self.disk_image,
                                  size=node_size,
                                  ex_keyname=self.platform.get_ssh_key_pair(),
                                  ex_security_groups=[self.platform.get_security_group()])

        # Get list of running nodes
        running_nodes = self.driver.wait_until_running([node])

        # Obtain our node
        self.node, external_IP = [(n, ext_IP[0]) for n, ext_IP in running_nodes if n.uuid == node.uuid][0]

        # Obtain the volume attached to the instance
        volume = self.driver.list_volumes(self.node)[0]

        # Resize to the correct disk size
        self.driver.ex_modify_volume(volume, {"Size": self.disk_space})

        # Return the external IP from node
        return external_IP

    def post_startup(self):

        # Resize by sending the correct command
        cmd = "device=$(ls /dev/sda || echo /dev/xvda) ; sudo growpart ${device} 1 ; sudo resize2fs ${device}1"
        self.run("resize_disk", cmd)
        self.wait_process("resize_disk")

    def destroy_instance(self):
        self.driver.destroy_node(self.node)

    def start_instance(self):
        self.driver.start_node(self.node)

    def stop_instance(self):
        self.driver.stop_node(self.node)

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
            f"-o SendEnv=AWS_ACCESS_KEY_ID -o SendEnv=AWS_SECRET_ACCESS_KEY " \
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
            "env":  {
                "AWS_ACCESS_KEY_ID": self.identity,
                "AWS_SECRET_ACCESS_KEY": self.secret
            },

            # Add CloudConductor specific arguments
            "original_cmd": original_cmd,
            "num_retries": self.default_num_cmd_retries if num_retries is None else num_retries,
            "docker_image": docker_image
        }

        # Add process to list of processes
        self.processes[job_name] = Process(cmd, **kwargs)

    def get_status(self):

        if self.node is None:
            return CloudInstance.OFF

        self.node = self.driver.list_nodes(ex_node_ids=[self.node.id])[0]

        # Define mapping between the cloud status and the current class status
        status_map = {
            "pending":          CloudInstance.CREATING,
            'running':          CloudInstance.AVAILABLE,
            'shutting-down':    CloudInstance.DESTROYING,
            'stopping':         CloudInstance.DESTROYING,
            'terminated':       CloudInstance.OFF
        }

        return status_map[self.node.extra["status"]]

    def get_compute_price(self):
        return 0

    def get_storage_price(self):
        return 0
