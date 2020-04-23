from Modules import Module

class Delly(Module):
    def __init__(self, module_id, is_docker = False):
        super(Delly, self).__init__(module_id, is_docker)
        self.output_keys = ["delly_vcf"]


    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("delly",          is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("exclude_list",   is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=13)


    def define_output(self):

        # Declare unique file name for bcf file
        vcf_file        = self.generate_unique_file_name("delly.vcf")
        self.add_output("delly_vcf", vcf_file)


    def define_command(self):

        # Get arguments to run Delly
        bam             = self.get_argument("bam")
        ref             = self.get_argument("ref")
        exclude_list    = self.get_argument("exclude_list")
        delly           = self.get_argument("delly")

        # Get output paths
        vcf             = self.get_output("delly_vcf")

        # Generate unique file name for intermediate bcf
        bcf             = "/temp.bcf"

        # Generate command
        # cmd = 'touch {0}; touch {1}; ls -l /usr/bin/ !LOG3!'.format(vcf, bcf)
        if exclude_list:
            cmd = "{0} call -x {1} -g {2} -o {3} {4};".format(delly, exclude_list, ref, bcf, bam)
        else:
            cmd = "{0} call -g {1} -o {2} {3} !LOG3!;".format(delly, ref, bcf, bam)
        cmd += "/usr/bin/bcftools view {0} > {1} !LOG3!".format(bcf, vcf)

        return cmd
