from Modules import Module


class Vcf2Maf(Module):
    def __init__(self, module_id, is_docker = True):
        super(Vcf2Maf, self).__init__(module_id, is_docker)
        self.output_keys = ["maf"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("vcf",            is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("exac",           is_required=True, is_resource=True)
        self.add_argument("vep",            is_required=True, is_resource=True)
        self.add_argument("vcf2maf",        is_required=True, is_resource=True)
        self.add_argument("ncbi_build",     default_value="GRCh38")

        self.add_argument("nr_cpus",        default_value=4)
        self.add_argument("mem",            default_value=15)

    def define_output(self):
        #
        sample_name = self.get_argument("sample_name")
        output_file = self.generate_unique_file_name(extension="{0}.maf".format(sample_name))

        self.add_output("maf",output_file)


    def define_command(self):

        # Get program options
        vcf2maf             = self.get_argument("vcf2maf")
        input_vcf           = self.get_argument("vcf")
        output_maf          = self.get_output("maf")
        ref_fasta           = self.get_argument("ref")
        vep_data            = self.get_argument("vep")
        filter_vcf          = self.get_argument("exac")
        ncbi_build          = self.get_argument("ncbi_build")


        cmd = "{0} --input-vcf {1} --output-maf {2} --vep-path /usr/local/bin/ --vep-data {3} --ref-fasta {4} " \
              "--filter-vcf {5} --ncbi-build {6}".format(
            vcf2maf, input_vcf, output_maf, vep_data, ref_fasta, filter_vcf, ncbi_build)

        return cmd