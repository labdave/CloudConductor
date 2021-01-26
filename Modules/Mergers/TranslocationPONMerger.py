from Modules import Merger

class CreateTranslocationPanelOfNormals(Merger):
    def __init__(self, module_id, is_docker=False):
        super(CreateTranslocationPanelOfNormals, self).__init__(module_id, is_docker)
        
        # Add output keys here if needed
        self.output_keys = ["pon", "full_pon"]


    def define_input(self):
        self.add_argument("anno_vcf",               is_required=True)
        self.add_argument("merge_distance",         default_value=10)
        self.add_argument("min_reads",              default_value=10)
        self.add_argument("min_samples",            default_value=5)
        self.add_argument("nr_cpus",                default_value=2)
        self.add_argument("mem",                    default_value=10.0)

    def define_output(self):
        # FILE NAME IN ALL MERGERS DEPENDS ON THIS: CHANGE WITH CAUTION
        pon      = self.generate_unique_file_name("filtered.pon.tsv")
        full_pon = self.generate_unique_file_name("unfiltered.pon.tsv")
        self.add_output("pon",      pon)
        self.add_output("full_pon", full_pon)


    def define_command(self):
        # Get input
        vcf_list                = self.get_argument("anno_vcf")
        merge_distance          = self.get_argument("merge_distance")
        min_reads               = self.get_argument("min_reads")
        min_samples             = self.get_argument("min_samples")

        # Get output
        filtered_pon            = self.get_output("pon")
        unfiltered_pon          = self.get_output("full_pon")
        
        
        # Start creating command
        cmd = "Rscript create_translocation_pon.R"

        # Create a comma-separated list of input files
        if isinstance(vcf_list, list):
            cmd += " -i {0}".format(','.join(vcf_list))
        else:
            cmd += " -i {0}".format(vcf_list)

        # Add arguments and outputs
        cmd += " -u {0} -f {1} -d {2} -n {3} -r {4}".format(unfiltered_pon,
            filtered_pon, merge_distance, min_samples, min_reads)

        # Add logging
        cmd += " !LOG3!"

        return cmd

