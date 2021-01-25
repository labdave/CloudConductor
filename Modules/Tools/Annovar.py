from Modules import Module

class Annovar(Module):

    def __init__(self, module_id, is_docker = False):
        super(Annovar, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)
        self.add_argument("annovar",            is_required=True, is_resource=True)
        self.add_argument("perl",               is_required=True, is_resource=True)
        self.add_argument("operations",         is_required=True)
        self.add_argument("protocol",           is_required=True)
        self.add_argument("buildver",           is_required=True)
        self.add_argument("nastring",           is_required=True, default_value=".")
        self.add_argument("dbdir",              is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=32)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 6.5")

    def define_output(self):

        # Get genome build version
        buildver = self.get_argument("buildver")

        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".{0}_multianno.vcf".format(buildver))
        self.add_output("vcf", vcf)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        annovar     = self.get_argument("annovar")
        perl        = self.get_argument("perl")
        operation   = self.get_argument("operations")
        protocol    = self.get_argument("protocol")
        nastring    = self.get_argument("nastring")
        buildver    = self.get_argument("buildver")
        dbdir       = self.get_argument("dbdir")
        nr_cpus     = self.get_argument("nr_cpus")

        # Generate prefix for final VCF output file
        vcf_out = str(self.get_output("vcf")).rsplit(".{0}_multianno.vcf".format(buildver), 1)[0]

        cmd = "{0} {1} {2} {3} --vcfinput --remove --buildver {4} --outfile {5} --protocol {6} --operation {7} --nastring {8} --thread {9} !LOG3!".format\
                (perl, annovar, vcf_in, dbdir, buildver, vcf_out, protocol, operation, nastring, nr_cpus)
        return cmd
