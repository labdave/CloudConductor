import json

from System.Platform import CloudInstance

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.common.google import ResourceNotFoundError


class GoogleInstance(CloudInstance):

    gcp_billing_api_url = "https://cloudbilling.googleapis.com/v1/services/"
    nanos_conversion_rate = .000000001 # 10^-9

    def __init__(self, name, nr_cpus, mem, disk_space, **kwargs):

        super(GoogleInstance, self).__init__(name, nr_cpus, mem, disk_space, **kwargs)

        # Initialize the instance credentials
        self.service_account, self.project_id = self.parse_service_account_json(self.identity)

        self.api_key = ''

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

        return status_map[self.node.extra["status"]]

    def get_compute_price(self):
        return self.gcp_compute_cost_old_json()

    def get_storage_price(self):
        return self.gcp_storage_cost_old_json()

    def gcp_compute_price_new_api(self):
        compute_cost = 0
        ram_cost = 0

        # get the compute engine service id
        gcp_services = requests.get(self.gcp_billing_api_url+"?key="+self.api_key).json()
        gce_id = [x["serviceId"] for x in gcp_services["services"] if x["displayName"]== "Compute Engine"]
        
        if gce_id and gce_id[0]:
            # retrieve gcp sku pricing info
            gcp_skus = requests.get(self.gcp_billing_api_url+gce_id[0]+"/skus?key="+self.api_key).json()
            
            # calculate cost for compute resources
            compute_skus = [x for x in gcp_skus['skus'] if any(self.region in reg for reg in x['serviceRegions']) \
                                        and x['category']['resourceFamily']=='Compute' \
                                        and x['category']['usageType']=='OnDemand' \
                                        and x['category']['resourceGroup']=='CPU' \
                                        and 'custom' in x['description'].lower() ]

            if compute_skus and compute_skus[0]:
                compute_nanos = int(compute_skus[0]["pricingInfo"][0]["pricingExpression"]["tieredRates"][0]["unitPrice"]["nanos"])
                compute_cost = compute_nanos * self.nanos_conversion_rate

            # calculate cost for memory resources
            ram_skus = [x for x in gcp_skus['skus'] if any(self.region in reg for reg in x['serviceRegions']) \
                                                        and x['category']['resourceFamily']=='Compute' \
                                                        and x['category']['usageType']=='OnDemand' \
                                                        and x['category']['resourceGroup']=='RAM' \
                                                        and 'custom' in x['description'].lower() ]
            
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
            # if is_preemptible:
            #     cpu_price_key += "-PREEMPTIBLE"
            #     mem_price_key += "-PREEMPTIBLE"
            compute_cost += prices[cpu_price_key][self.zone] + prices[mem_price_key][self.zone]


        except BaseException as e:
            if str(e) != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e)
            raise

        return compute_cost

    def gcp_storage_price_new_api(self):
        storage_cost = 0

        # get the compute engine service id
        gcp_services = requests.get(self.gcp_billing_api_url+"?key="+self.api_key).json()
        gce_id = [x["serviceId"] for x in gcp_services["services"] if x["displayName"]== "Compute Engine"]
        
        if gce_id and gce_id[0]:
            # retrieve gcp sku pricing info
            gcp_skus = requests.get(self.gcp_billing_api_url+gce_id[0]+"/skus?key="+self.api_key).json()
            
            # calculate cost for storage resources
            storage_skus = [x for x in gcp_skus['skus'] if any(self.region in reg for reg in x['serviceRegions']) \
                                        and x['category']['resourceFamily']=='Storage' \
                                        and x['category']['usageType']=='OnDemand' \
                                        and x['category']['resourceGroup']=='PDStandard' ]

            if storage_skus and storage_skus[0]:
                storage_nanos = int(storage_skus[0]["pricingInfo"][0]["pricingExpression"]["tieredRates"][0]["unitPrice"]["nanos"])
                storage_cost = storage_nanos * self.nanos_conversion_rate

        return storage_cost

    def gcp_storage_price_old_json(self):
        storage_cost = 0
        try:
            price_json_url = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

            prices = requests.get(price_json_url).json()["gcp_price_list"]

            storage_cost += prices["CP-COMPUTEENGINE-STORAGE-PD-CAPACITY"][self.zone]

        except BaseException as e:
            if str(e) != "":
                logging.error("Could not obtain instance prices. The following error appeared: %s." % e)
            raise

        return storage_cost
