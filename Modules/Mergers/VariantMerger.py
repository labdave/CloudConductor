import os
from Modules import Merger

class VariantMerger(Merger):

    def __init__(self, module_id, is_docker=True):
        super(VariantMerger, self).__init__(module_id, is_docker)
        self.output_keys    = ["merged_filt_long", "merged_filt_wide", "merged_wl_long", "merged_wl_wide"]

    def define_input(self):
        self.add_argument("nr_cpus",         is_required=True, default_value=8)
        self.add_argument("mem",             is_required=True, default_value=48)



    def define_output(self):
        # Declare name of merged VCF output file
        merged_filt_long = self.generate_unique_file_name(extension="_merged_filtered_long.txt")
        merged_filt_wide = self.generate_unique_file_name(extension="_merged_filtered_wide.txt")
        merged_wl_long = self.generate_unique_file_name(extension="_merged_whitelist_long.txt")
        merged_wl_wide = self.generate_unique_file_name(extension="_merged_whitelist_wide.txt")
        self.add_output("merged_filt_long", merged_filt_long)
        self.add_output("merged_filt_wide", merged_filt_wide)
        self.add_output("merged_wl_long", merged_wl_long)
        self.add_output("merged_wl_wide", merged_wl_wide)

    def define_command(self):
        # Get input arguments
        input_dir = os.path.dirname(self.output_dir.rstrip("/"))

        merged_filt_long     = self.get_output("merged_filt_long")
        merged_filt_wide = self.get_output("merged_filt_wide")
        merged_wl_long       = self.get_output("merged_wl_long")
        merged_wl_wide = self.get_output("merged_wl_wide")

        # Generating command
        cmd = "Rscript multisample_merge.R {0} {1} {2} {3} {4}".format(input_dir, merged_filt_long, merged_filt_wide, merged_wl_long, merged_wl_wide)

        cmd += "!LOG3!"

        return cmd

