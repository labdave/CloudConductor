from Modules import Module

class Peddy(Module):
    def __init__(self, module_id, is_docker=False):
        super(Peddy, self).__init__(module_id, is_docker)
        self.output_keys = ["peddy_html", "sex_csv", "sex_png", "pca_png",
                            "het_csv", "het_png", "ped_csv", "ped_png", "ped_csv_rel_diff"]

    def define_input(self):
        self.add_argument("peddy",      is_resource=True, is_required=True)
        self.add_argument("vcf_gz",     is_required=True)
        self.add_argument("vcf_tbi",    is_required=True)
        self.add_argument("ped",        is_required=True)
        self.add_argument("nr_cpus",    default_value=4)
        self.add_argument("mem",        default_value=15)

    def define_output(self):
        # Generate output prefix
        self.output_prefix = self.generate_unique_file_name(extension="").rstrip(".")

        # Generate all output files
        self.add_output("peddy_html",       "{0}.html".format(self.output_prefix))

        self.add_output("sex_csv",          "{0}.sex_check.csv".format(self.output_prefix))
        self.add_output("sex_png",          "{0}.sex_check.png".format(self.output_prefix))

        self.add_output("pca_png",          "{0}.pca_check.png".format(self.output_prefix))

        self.add_output("het_csv",          "{0}.het_check.csv".format(self.output_prefix))
        self.add_output("het_png",          "{0}.het_check.png".format(self.output_prefix))

        self.add_output("ped_csv",          "{0}.ped_check.csv".format(self.output_prefix))
        self.add_output("ped_png",          "{0}.ped_check.png".format(self.output_prefix))
        self.add_output("ped_csv_rel_diff", "{0}.ped_check.rel-difference.csv".format(self.output_prefix))

    def define_command(self):
        peddy          = self.get_argument("peddy")
        vcf_gz         = self.get_argument("vcf_gz")
        ped            = self.get_argument("ped")
        nr_cpus        = self.get_argument("nr_cpus")

        # Generate command
        cmd = "python -m {0} -p {1} --plot --prefix {2} --sites hg38 {3} {4} !LOG3!".format(
            peddy, nr_cpus, self.output_prefix, vcf_gz, ped)

        return cmd
