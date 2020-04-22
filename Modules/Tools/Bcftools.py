from Modules import Module
from System.Platform import Platform


class View(Module):

    def __init__(self, module_id, is_docker = False):
        super(View, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_gz"]

    def define_input(self):
        self.add_argument("vcf",        is_required=True)                       # Input VCF file
        self.add_argument("bcftools",   is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=1)
        self.add_argument("mem",        is_required=True,   default_value=2)

    def define_output(self):

        randomer = Platform.generate_unique_id()
        vcf_gz = self.generate_unique_file_name(extension="{0}.vcf.gz".format(randomer))
        self.add_output("vcf_gz", vcf_gz)

    def define_command(self):
        # Get input arguments
        vcf         = self.get_argument("vcf")
        bcftools    = self.get_argument("bcftools")
        threads     = self.get_argument("nr_cpus")

        # Get output file for compressed VCF file
        vcf_gz     = self.get_output("vcf_gz")

        cmd = "{0} view -O z --threads {1} {2} > {3} !LOG2!".format(bcftools, threads, vcf, vcf_gz)

        return cmd

class BcftoolsIndex(Module):

    def __init__(self, module_id, is_docker = False):
        super(BcftoolsIndex, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_csi"]

    def define_input(self):
        self.add_argument("vcf_gz",             is_required=True)                       # Input VCF file
        self.add_argument("bcftools",           is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=2)

    def define_output(self):

        vcf_gz = self.get_argument("vcf_gz")
        self.add_output("vcf_csi", "{0}.csi".format(vcf_gz))

    def define_command(self):
        # Get input arguments
        vcf_gz      = self.get_argument("vcf_gz")
        bcftools    = self.get_argument("bcftools")
        threads     = self.get_argument("nr_cpus")

        # Get output file for VCF index file
        vcf_csi     = self.get_output("vcf_csi")

        cmd = "{0} index --threads {3} -f -c -o {2} {1} !LOG3!".format(bcftools, vcf_gz, vcf_csi, threads)

        return cmd

class BCFToolsNorm(Module):

    def __init__(self, module_id, is_docker = False):
        super(BCFToolsNorm, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("bcftools",           is_required=True,   is_resource=True)
        self.add_argument("ref",                is_required=True,   is_resource=True)   # Path to Fasta formatted genome reference
        self.add_argument("split_multiallelic", is_required=True,   default_value=True) # Whether to split multiallelic sites into mutliple sites
        self.add_argument("nr_cpus",            is_required=True,   default_value=4)
        self.add_argument("mem",                is_required=True,   default_value="nr_cpus*4")

    def define_output(self):
        # Declare recoded VCF output filename
        normalized_vcf = self.generate_unique_file_name(extension=".normalized.vcf")
        self.add_output("vcf", normalized_vcf)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        bcftools            = self.get_argument("bcftools")
        ref                 = self.get_argument("ref")
        split_multiallelic  = self.get_argument("split_multiallelic")

        # Get final normalized VCF output file path
        vcf_out = self.get_output("vcf")

        cmd = "{0} norm".format(bcftools)
        if split_multiallelic:
            # Optionally specify to split multiallelic into two lines
            cmd += " -m-both"
        cmd += " -f {0} -o {1} {2}".format(ref, vcf_out, vcf_in)

        # Capture stderr
        cmd += " !LOG3!"

        # Return cmd
        return cmd
