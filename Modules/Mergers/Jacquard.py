import os
import logging

from Modules import Merger

class Jacquard(Merger):
    def __init__(self, module_id, is_docker = True):
        super(Jacquard, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf"]


    def define_input(self):
        self.add_argument("vcf",            is_required=True)
        self.add_argument("jacquard",       is_required=True, is_resource=True)

        self.add_argument("nr_cpus",        is_required=True, default_value=16)
        self.add_argument("mem",            is_required=True, default_value=104)

        
    def define_output(self):

        # Declare merged-VCF filename
        vcf = self.generate_unique_file_name(extension=".Merged.vcf")
        self.add_output("vcf",vcf)


    def define_command(self):

        # Get program options
        jacquard            = self.get_argument("jacquard")
        input_dir           = os.path.dirname(self.output_dir.rstrip("/"))
        output_file         = self.get_output("vcf")

        # COMMAND:
        # sudo docker run -v "$PWD":"/TheFiles" davelabhub/jacquard:v1.1.2 bash -c "jacquard merge --include_format_tags=GT,AD,DP,DPI --include_rows=all --include_cells=all /TheFiles/ /TheFiles/Merged.vcf"

        cmd = "{0} merge --include_format_tags=GT,AD,DP,DPI --include_rows=all --include_cells=all {1}/ {2}".format(jacquard, input_dir, output_file)


        return cmd
