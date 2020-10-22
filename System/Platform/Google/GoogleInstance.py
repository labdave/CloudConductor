import logging
import requests
import time
import os

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.common.google import ResourceNotFoundError
from libcloud.common.types import LibcloudError

from System.Platform.Instance import CloudInstance
from System.Platform import Process


class GoogleInstance(CloudInstance):

    gcp_billing_api_url = "https://cloudbilling.googleapis.com/v1/services/"
    nanos_conversion_rate = .000000001  # 10^-9

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        super(GoogleInstance, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Initialize the instance credentials
        self.service_account, self.project_id = self.platform.parse_service_account_json()

        self.api_key = ''
        self.is_preemptible = False

        # Create libcloud driver
        driver_class = get_driver(Provider.GCE)
        self.driver = driver_class(self.service_account, self.identity,
                                   datacenter=self.zone,
                                   project=self.project_id)

        # Initialize the node variable
        self.node = None

        # check for force_standard
        self.force_standard = kwargs.get("force_standard", 'false')

        # Set AWS credentials as SSH options
        self.set_ssh_option("SendEnv", "AWS_ACCESS_KEY_ID")
        self.set_ssh_option("SendEnv", "AWS_SECRET_ACCESS_KEY")

    def create_instance(self):

        # Generate NodeSize for instance
        size_name = f"custom-{int(self.nr_cpus)}-{int(self.mem*1024)}"
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
                    "sourceImage" : f"global/images/{self.disk_image.name}",
                    "diskSizeGb"  : str(self.disk_space)
                },
                "autoDelete": True
            }
        ]

        # Create instance
        if self.name.startswith("helper-") or self.force_standard:
            # don't want helper instances to be preemptible
            self.is_preemptible = False

        creation_attempts = 1
        while not self.node and creation_attempts < 4:
            try:
                creation_attempts += 1
                self.node = self.driver.create_node(name=self.name,
                                                    image=self.disk_image,
                                                    size=node_size,
                                                    ex_disks_gce_struct=disks,
                                                    ex_service_accounts=sa_scope,
                                                    ex_preemptible=self.is_preemptible,
                                                    ex_metadata=metadata)
            except Exception as e:
                exception_string = str(e)
                if 'alreadyExists' in exception_string:
                    logging.warning(f"({self.name}) Instance already exists. Getting status...")
                    self.get_status(log_status=True)
                else:
                    sleep_time = self.get_api_sleep(creation_attempts-1)
                    logging.warning(f"({self.name}) Failed to create instance due to: {str(e)}. Waiting {sleep_time} seconds before retrying.")
                    time.sleep(sleep_time)

        if not self.node:
            raise RuntimeError(f"({self.name}) Failed to create instance!")
        # Return the external IP from node
        return self.node.public_ips[0]

    def destroy_instance(self):
        # for some reason destroying nodes can sometimes timeout. stopping the instance first is the suggested solution
        self.driver.ex_stop_node(self.node)
        self.driver.destroy_node(self.node)

    def post_startup(self):

        # Transfer SA key to instance
        cmd = f'scp -i {self.ssh_private_key} -o CheckHostIP=no -o StrictHostKeyChecking=no {self.identity} ' \
              f'{self.ssh_connection_user}@{self.external_IP}:GCP.json'

        Process.run_local_cmd(cmd, err_msg="Could not authenticate Google SDK on instance!", print_logs=True, error_string_check="")

        # Setup Google SA path
        os.environ["GOOGLE_SA"] = f"/home/{self.ssh_connection_user}/GCP.json"
        self.set_ssh_option('SendEnv', 'GOOGLE_SA')

    def start_instance(self):
        try:
            self.driver.ex_start_node(self.node)
        except ResourceNotFoundError:
            logging.info(f"({self.name}) Instance not found. Recreating a new instance.")
            self.recreate()
        except LibcloudError:
            logging.debug(f"({self.name}) Libcloud issue while starting the instance waiting for 30 seconds before retrying.")
            time.sleep(30)

        logging.info(f"({self.name}) Instance started. Waiting for it to become available")

        # Initializing the cycle count
        cycle_count = 0
        started = False

        # Waiting for 5 minutes for instance to be SSH-able
        while cycle_count < 30:
            if self.get_status() == CloudInstance.AVAILABLE:
                started = True
                break

            # Wait for 10 seconds before checking the status again
            time.sleep(self.get_api_sleep(cycle_count+1))

            # Increment the cycle count
            cycle_count += 1

        if not started:
            raise RuntimeError("(%s) Instance was unable to restart" % self.name)

        return self.node.public_ips[0]

    def stop_instance(self):
        try:
            self.driver.ex_stop_node(self.node)
        except Exception as e:
            exception_string = str(e)
            if 'notFound' in exception_string:
                logging.debug(f"({self.name}) Failed to stop instance. ResourceNotFound moving on.")

    def get_status(self, log_status=False):

        try:
            self.node = self.driver.ex_get_node(self.name)
        except ResourceNotFoundError:
            return CloudInstance.OFF

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

        if log_status:
            logging.debug(f"({self.name}) Current status is: {self.node.extra['status']}")

        return status_map[self.node.extra["status"]]

    def get_compute_price(self):
        price = self.gcp_compute_price_old_json()
        return price

    def get_storage_price(self):
        price = self.gcp_storage_price_old_json()
        return price

    def generate_docker_env(self):
        env_vars = [
            "RCLONE_CONFIG_GS_TYPE='google cloud storage'",
            "RCLONE_CONFIG_GS_SERVICE_ACCOUNT_FILE=$GOOGLE_SA",
            "RCLONE_CONFIG_GS_BUCKET_ACL='projectPrivate'",
            "RCLONE_CONFIG_GS_OBJECT_ACL='projectPrivate'",
            "RCLONE_CONFIG_S3_TYPE='s3'",
            "RCLONE_CONFIG_S3_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID",
            "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
        ]

        return " ".join([f"-e {e}" for e in env_vars])

    def gcp_compute_price_new_api(self):
        compute_cost = 0
        ram_cost = 0

        # get the compute engine service id
        gcp_services = requests.get(self.gcp_billing_api_url + "?key=" + self.api_key).json()
        gce_id = [x["serviceId"] for x in gcp_services["services"] if x["displayName"] == "Compute Engine"]

        if gce_id and gce_id[0]:
            # retrieve gcp sku pricing info
            gcp_skus = requests.get(self.gcp_billing_api_url + gce_id[0] + "/skus?key=" + self.api_key).json()

            # calculate cost for compute resources
            compute_skus = [x for x in gcp_skus['skus'] if any(self.region in reg for reg in x['serviceRegions'])
                            and x['category']['resourceFamily'] == 'Compute'
                            and x['category']['usageType'] == 'OnDemand'
                            and x['category']['resourceGroup'] == 'CPU'
                            and 'custom' in x['description'].lower()]

            if compute_skus and compute_skus[0]:
                compute_nanos = int(compute_skus[0]["pricingInfo"][0]["pricingExpression"]["tieredRates"][0]["unitPrice"]["nanos"])
                compute_cost = compute_nanos * self.nanos_conversion_rate

            # calculate cost for memory resources
            ram_skus = [x for x in gcp_skus['skus'] if any(self.region in reg for reg in x['serviceRegions'])
                        and x['category']['resourceFamily'] == 'Compute'
                        and x['category']['usageType'] == 'OnDemand'
                        and x['category']['resourceGroup'] == 'RAM'
                        and 'custom' in x['description'].lower()]

            if ram_skus and ram_skus[0]:
                ram_nanos = int(ram_skus[0]["pricingInfo"][0]["pricingExpression"]["tieredRates"][0]["unitPrice"]["nanos"])
                ram_cost = ram_nanos * self.nanos_conversion_rate

        # return sum of compute and ram costs
        return compute_cost + ram_cost

    def gcp_compute_price_old_json(self):
        compute_cost = 0
        try:
            price_json_url = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

            prices = requests.get(price_json_url).json()["gcp_price_list"]

            # Get price of CPUs, mem for custom instance
            cpu_price_key = "CP-COMPUTEENGINE-CUSTOM-VM-CORE"
            mem_price_key = "CP-COMPUTEENGINE-CUSTOM-VM-RAM"
            if self.is_preemptible:
                cpu_price_key += "-PREEMPTIBLE"
                mem_price_key += "-PREEMPTIBLE"

            # calculate hourly price for all CPUs and memory
            compute_cost += prices[cpu_price_key][self.region] * self.nr_cpus + prices[mem_price_key][self.region] * self.mem

        except BaseException as e:
            if str(e) != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e)
            raise

        return compute_cost

    def gcp_storage_price_new_api(self):
        storage_cost = 0

        # get the compute engine service id
        gcp_services = requests.get(self.gcp_billing_api_url + "?key=" + self.api_key).json()
        gce_id = [x["serviceId"] for x in gcp_services["services"] if x["displayName"] == "Compute Engine"]

        if gce_id and gce_id[0]:
            # retrieve gcp sku pricing info
            gcp_skus = requests.get(self.gcp_billing_api_url + gce_id[0] + "/skus?key=" + self.api_key).json()

            # calculate cost for storage resources
            storage_skus = [x for x in gcp_skus['skus'] if any(self.region in reg for reg in x['serviceRegions'])
                            and x['category']['resourceFamily'] == 'Storage'
                            and x['category']['usageType'] == 'OnDemand'
                            and x['category']['resourceGroup'] == 'PDStandard']

            if storage_skus and storage_skus[0]:
                storage_nanos = int(storage_skus[0]["pricingInfo"][0]["pricingExpression"]["tieredRates"][0]["unitPrice"]["nanos"])
                storage_cost = storage_nanos * self.nanos_conversion_rate

        return storage_cost

    def gcp_storage_price_old_json(self):
        storage_cost = 0
        try:
            price_json_url = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

            prices = requests.get(price_json_url).json()["gcp_price_list"]

            # Calculate hourly rate for all disk space
            storage_cost += (prices["CP-COMPUTEENGINE-STORAGE-PD-CAPACITY"][self.region] / 730) * self.disk_space

        except BaseException as e:
            if str(e) != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e)
            raise

        return storage_cost
