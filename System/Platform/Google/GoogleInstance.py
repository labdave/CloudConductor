import json

from System.Platform import CloudInstance

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver


class GoogleInstance(CloudInstance):

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        super(GoogleInstance, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Initialize the instance credentials
        self.service_account, self.project_id = self.parse_service_account_json(self.identity)

        # Create libcloud driver
        driver_class = get_driver(Provider.GCE)
        self.driver = driver_class(self.service_account, self.identity,
                                   datacenter=self.zone,
                                   project=self.project_id)

        # Initialize the extra information
        self.image = kwargs.get("disk_image")

        # Initialize the node variable
        self.node = None

    @staticmethod
    def parse_service_account_json(identity_json_file):

        # Parse service account file
        with open(identity_json_file) as json_inp:
            service_account_data = json.load(json_inp)

        # Save data locally
        service_account = service_account_data["client_email"]
        project_id = service_account_data["project_id"]

        return service_account, project_id

    def create_instance(self):

        # Generate NodeSize for instance
        size_name = f"custom-{self.nr_cpus}-{self.mem*1024}"
        node_size = self.driver.ex_get_size(size_name)

        # Read the public key content
        with open(f"{self.ssh_private_key}.pub") as inp:
            ssh_key_content = inp.read()

        # Generate service account scope
        sa_scope = [
            {
                "email": self.service_account,
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
            }
        ]

        # Generate the correct metadata that will contain the SSH key
        metadata = {
            "ssh-keys": f"{self.ssh_connection_user}: {ssh_key_content} {self.ssh_connection_user}"
        }

        # Generate the boot disk information
        disks = [
            {
                "boot": True,
                "initializeParams": {
                    "sourceImage" : f"global/images/{self.image}",
                    "diskSizeGb" : str(self.disk_space)
                }
            }
        ]

        # Create instance
        self.node = self.driver.create_node(name=self.name,
                                            image=self.image,
                                            size=node_size,
                                            ex_disks_gce_struct=disks,
                                            ex_service_accounts=sa_scope,
                                            ex_metadata=metadata)

        # Return the external IP from node
        return self.node.public_ips[0]

    def destroy_instance(self):
        self.driver.destroy_node(self.node)

    def start_instance(self):
        self.driver.ex_start_node(self.node)

    def stop_instance(self):
        self.driver.ex_stop_node(self.node)

    def get_status(self):
        self.node = self.driver.ex_get_node(self.name)

        # Define mapping between the cloud status and the current class status
        status_map = {
            "PROVISIONING": CloudInstance.CREATING,
            "STAGING":      CloudInstance.CREATING,
            "RUNNING":      CloudInstance.AVAILABLE,
            "STOPPING":     CloudInstance.DESTROYING,
            "SUSPENDED":    CloudInstance.OFF,
            "TERMINATED":   CloudInstance.OFF,
            "UNKNOWN":      CloudInstance.OFF
        }

        return status_map[self.node.extra["status"]]

    def get_compute_price(self):
        return 0

    def get_storage_price(self):
        return 0
