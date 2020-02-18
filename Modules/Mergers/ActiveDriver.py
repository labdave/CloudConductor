from Modules import Merger

class ActiveDriver(Merger):
    def __init__(self, module_id, is_docker=False):
        super(ActiveDriver, self).__init__(module_id, is_docker)
        self.output_keys = ["activedriver_in","activedriver_fdr","activedriver_mr","activedriver_rds"]


    def define_input(self):
        self.add_argument("activedriver",   is_resource=True, is_required=True)
        self.add_argument("ref",            is_resource=True, is_required=True)
        self.add_argument("recoded_vcf",    is_required=True)
        self.add_argument("out_file",       default_value="activedriver_file")
        self.add_argument("nr_cpus",        default_value=2)
        self.add_argument("mem",            default_value=8)


    def define_output(self):
        # Combine output prefix with output directory
        out_file        = self.get_argument("out_file")
        output_pefix    = "{0}/{1}".format(self.output_dir,out_file)

        name_in = "{0}.input.tsv".format(output_pefix)
        name_fdr = "{0}.fdr.tsv".format(output_pefix)
        name_mr = "{0}.merged_report.tsv".format(output_pefix)
        name_rds = "{0}.RDS".format(output_pefix)

        self.add_output("activedriver_in",name_in)
        self.add_output("activedriver_fdr",name_fdr)
        self.add_output("activedriver_mr",name_mr)
        self.add_output("activedriver_rds",name_rds)

    def define_command(self):
        activedriver = self.get_argument("activedriver")
        ref          = self.get_argument("ref")
        file_list    = self.get_argument("recoded_vcf")
        out_file     = self.get_argument("out_file")

        output_prefix = "{0}/{1}".format(self.output_dir,out_file)

        # Change recodec vcf files into string
        file_string = ",".join(file_list)

        cmd = "Rscript {0} -o {1} -r {2} -f {3}".format(activedriver, output_prefix, ref, file_string)

        return cmd
