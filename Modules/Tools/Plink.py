from Modules import Module

class Plink(Module):
    def __init__(self, module_id, is_docker=False):
        super(Plink, self).__init__(module_id, is_docker)
        self.output_keys = ["ped"]

    def define_input(self):
        self.add_argument("plink",          is_resource=True, is_required=True)
        self.add_argument("vcf_gz",         is_required=True)
        self.add_argument("vcf_tbi",        is_required=True)
        self.add_argument("nr_cpus",        default_value=1)
        self.add_argument("mem",            default_value=1)

    def define_output(self):
        # Generate output filename
        ped_file = self.generate_unique_file_name(extension=".ped")

        self.add_output("ped", ped_file)

    def define_command(self):
        plink             = self.get_argument("plink")
        vcf_gz            = self.get_argument("vcf_gz")
        ped_file          = self.get_output("ped")

        # Create output file prefix
        out_prefix = ped_file.get_path().rsplit(".", 1)[0]

        # Generate command
        cmd = "{0} --vcf {1} --recode --double-id --allow-extra-chr --out {2} !LOG3!".format(plink, vcf_gz, out_prefix)

        return cmd
