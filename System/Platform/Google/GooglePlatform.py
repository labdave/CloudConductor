import math

from System.Platform import CloudPlatform
from System.Platform.Google import GoogleInstance


class GooglePlatform(CloudPlatform):

    def authenticate_platform(self):
        pass

    @staticmethod
    def standardize_instance(inst_name, nr_cpus, mem, disk_space):

        # Ensure instance name does not contain weird characters
        inst_name = inst_name.replace("_", "-").lower()

        # Ensure the memory is withing GCP range:
        if mem / nr_cpus < 0.9:
            mem = nr_cpus * 0.9
        elif mem / nr_cpus > 6.5:
            nr_cpus = math.ceil(mem / 6.5)

        # Ensure number of CPUs is an even number or 1
        if nr_cpus != 1 and nr_cpus % 2 == 1:
            nr_cpus += 1

        return inst_name, nr_cpus, mem, disk_space

    def publish_report(self, report):
        pass

    def validate(self):
        pass

    def clean_up(self):
        pass

    def get_random_zone(self):
        return "us-east1-c"

    def get_cloud_instance_class(self):
        return GoogleInstance
