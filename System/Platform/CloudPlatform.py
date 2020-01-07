import logging
import abc
import uuid
import threading
import os
import time
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from Config import ConfigParser


class CloudPlatform(object, metaclass=abc.ABCMeta):

    CONFIG_SPEC = f"{os.path.dirname(__file__)}/Platform.validate"

    def __init__(self, name, platform_config_file, final_output_dir):

        # Platform name
        self.name = name

        # Check if CloudInstance class is set by the user
        self.CloudInstanceClass = self.get_cloud_instance_class()

        # Initialize platform config
        config_parser       = ConfigParser(platform_config_file, self.CONFIG_SPEC)
        self.config         = config_parser.get_config()

        # Only consider the specs related to the current platform
        self.config = self.config[self.__class__.__name__]

        # Obtain the constants from the platform config
        self.NR_CPUS = {
            "TOTAL" :   self.config["PLAT_MAX_NR_CPUS"],
            "MAX" :     self.config["INST_MAX_NR_CPUS"],
            "MIN" :     self.config["INST_MIN_NR_CPUS"]
        }
        self.MEM = {
            "TOTAL" :   self.config["PLAT_MAX_MEM"],
            "MAX" :     self.config["INST_MAX_MEM"],
            "MIN" :     self.config["INST_MIN_MEM"]
        }
        self.DISK_SPACE = {
            "TOTAL" :   self.config["PLAT_MAX_DISK_SPACE"],
            "MAX" :     self.config["INST_MAX_DISK_SPACE"],
            "MIN" :     self.config["INST_MIN_DISK_SPACE"]
        }

        # Obtain the identity and the secret
        self.identity = self.config["identity"]
        self.secret = self.config.get("secret", None)

        # Obtain processing locations
        self.region = self.config["region"]
        self.zone = self.config.get("zone", None)
        if self.zone is None:
            self.zone = self.get_random_zone()

        # Obtain remaining parameters from the configuration file
        self.cmd_retries = self.config["cmd_retries"]
        self.ssh_connection_user = self.config["ssh_connection_user"]

        #TODO: I still have to add this, because Datastore required a work directory
        self.wrk_dir = "/data"
        self.final_output_dir = self.standardize_dir(final_output_dir)

        # Save extra variables
        self.extra = self.config.get("extra", {})

        # Dictionary to hold instances currently managed by the platform
        self.instances = {}

        # Platform resource threading lock
        self.platform_lock = threading.Lock()

        # Boolean flag to lock instance creation upon cleanup
        self.__locked = False

        # Current resource levels
        self.cpu = 0
        self.mem = 0
        self.disk_space = 0

        # TODO: figure out the ssh_connection_user from platform_config

        # Initialize the location of the CloudConductor ssh_key
        self.ssh_private_key = None

    def init_platform(self):

        # Authenticate CloudConductor locally
        self.authenticate_cc()

        # Authenticate the current platform
        self.authenticate_platform()

        # Validate the current platform
        self.validate()

    def authenticate_cc(self):

        # Obtain the home directory of the current user
        home_dir = str(Path.home())

        # Ensure the .ssh directory is present in the home directory
        if not os.path.exists(f'{home_dir}/.ssh'):
            os.mkdir(f'{home_dir}/.ssh')

        # Check if the cloud_conductor private/public key pair exists; if not, create one pair
        private_key = f'{home_dir}/.ssh/cloud_conductor'
        public_key = f'{private_key}.pub'
        if os.path.exists(private_key) and os.path.exists(public_key):
            self.ssh_private_key = private_key
        else:
            self.__create_ssh_key(private_key)
            self.ssh_private_key = private_key

    def get_instance(self, nr_cpus, mem, disk_space, **kwargs):
        """Initialize new instance and register with platform"""

        # Obtain task_id that will be used
        task_id = kwargs.pop("task_id", None)

        # Generate a unique instance name and associate it to the current request
        while True:

            # Generate a (new) unique instance name
            if kwargs.pop("is_helper", False):
                inst_name = f'helper-{self.name[:20]}-{self.generate_unique_id()}'
            else:
                inst_name = f'inst-{self.name[:20]}-{task_id[:25]}-{self.generate_unique_id()}'

            # Standardize instance type
            inst_name, nr_cpus, mem, disk_space = self.standardize_instance(inst_name, nr_cpus, mem, disk_space)

            # Check if instance name already exists (because of randomer collission), otherwise reserve it
            with self.platform_lock:
                if inst_name not in self.instances:
                    self.instances[inst_name] = None
                    break

        # TODO: Check what happens in case of locking. Will lots of exceptions be thrown?

        # Check if platform is locked
        if self.__locked:
            logging.error(f'({inst_name}) Platform failed to initialize instance! Platform is currently locked!')
            raise RuntimeError("Cannot create instance while platform is locked!")

        # Check to see if asking for too many resources
        err_msg = self.__check_instance(inst_name, nr_cpus, mem, disk_space)
        if err_msg is not None:
            logging.error(f'{inst_name} Could not create instance!')
            raise RuntimeError(err_msg)

        # Check if the current resource pool is full to allocate the current instance
        allocated = False
        while not allocated:
            with self.platform_lock:

                # Identify any overload
                cpu_overload = self.cpu + nr_cpus > self.NR_CPUS["TOTAL"]
                mem_overload = self.mem + mem > self.MEM["TOTAL"]
                disk_overload = self.disk_space + disk_space > self.DISK_SPACE["TOTAL"]

                # Check if not overloaded
                if not cpu_overload and not mem_overload and not disk_overload:

                    # Reserve resources so that the other threads can see the new number
                    self.cpu += nr_cpus
                    self.mem += mem
                    self.disk_space += disk_space

                    # Mark as allocated and start creating
                    allocated = True
                    logging.debug(f'({inst_name}) Creating instance for task "{task_id}".')

            if not allocated:
                logging.debug(f'({inst_name}) Platform fully loaded, we will wait for one minute and check again!')
                time.sleep(60)

        # Load cloud instance kwargs with platform variables
        kwargs.update({
            "identity"              : self.identity,
            "secret"                : self.secret,
            "ssh_connection_user"   : self.ssh_connection_user,
            "ssh_private_key"       : self.ssh_private_key,
            "cmd_retries"           : self.cmd_retries,
            "region"                : self.region,
            "zone"                  : self.zone
        })

        # Also add the extra information
        kwargs.update(self.extra)

        # Initialize new instance
        try:
            self.instances[inst_name] = self.CloudInstanceClass(inst_name, nr_cpus, mem, disk_space, **kwargs).create()

            logging.info(f'({inst_name}) Instance was successfully created!')

            return self.instances[inst_name]

        except BaseException:

            # TODO: Should we destroy the instance here?

            # Deallocate resources as no instance was created
            with self.platform_lock:
                self.cpu -= nr_cpus
                self.mem -= mem
                self.disk_space -= disk_space

            # Raise the actual exception
            raise

    def get_max_nr_cpus(self):
        return self.NR_CPUS["MAX"]

    def get_max_mem(self):
        return self.MEM["MAX"]

    def get_max_disk_space(self):
        return self.DISK_SPACE["MAX"]

    def get_min_disk_space(self):
        return self.DISK_SPACE["MIN"]

    def get_final_output_dir(self):
        return self.final_output_dir

    def lock(self):
        with self.platform_lock:
            self.__locked = True

    def unlock(self):
        with self.platform_lock:
            self.__locked = False

    def __check_instance(self, inst_name, nr_cpus, mem, disk_space):
        # Check that nr_cpus, mem, disk space are under max

        if nr_cpus > self.NR_CPUS["MAX"]:
            return f'{inst_name} Cannot provision instance with {nr_cpus} vCPUs, ' \
                   f'as maximum per instance is {self.NR_CPUS["MAX"]} vCPUs.'

        elif mem > self.MEM["MAX"]:
            return f'{inst_name} Cannot provision instance with {mem} GB RAM, ' \
                   f'as maximum per instance is {self.MEM["MAX"]} GB RAM.'

        elif disk_space > self.DISK_SPACE["MAX"]:
            return f'{inst_name} Cannot provision instance with {disk_space} GB disk space, ' \
                   f'as maximum per instance is {self.DISK_SPACE["MAX"]} GB disk space.'

        else:
            return None

    # ABSTRACT METHODS TO BE IMPLEMENTED BY INHERITING CLASSES

    @abc.abstractmethod
    def get_random_zone(self):
        pass

    @abc.abstractmethod
    def get_cloud_instance_class(self):
        pass

    @abc.abstractmethod
    def authenticate_platform(self):
        pass

    @abc.abstractmethod
    def validate(self):
        pass

    @abc.abstractmethod
    @staticmethod
    def standardize_instance(inst_name, nr_cpus, meme, disk_space):
        pass

    @abc.abstractmethod
    def publish_report(self, report_path):
        pass

    @abc.abstractmethod
    def clean_up(self):
        pass

    # PRIVATE UTILITY METHODS

    @staticmethod
    def __create_ssh_key(key_prefix):

        # Generate RSA private key
        key = rsa.generate_private_key(
            backend=default_backend(),
            public_exponent=65537,
            key_size=2048
        )

        # Write the private key to file
        with open(key_prefix, "wb") as out:
            out.write(
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption()
                )
            )

        # Change permission of private key to 400
        os.chmod(key_prefix, 0o400)

        # Write the public key to file
        with open(f"{key_prefix}.pub", "wb") as out:
            out.write(
                key.public_key().public_bytes(
                    serialization.Encoding.OpenSSH,
                    serialization.PublicFormat.OpenSSH
                )
            )

    @staticmethod
    def generate_unique_id(id_len=6):
        return str(uuid.uuid4())[0:id_len]

    @staticmethod
    def standardize_dir(dir_path):
        # Makes directory names uniform to include a single '/' at the end
        return dir_path.rstrip("/") + "/"
