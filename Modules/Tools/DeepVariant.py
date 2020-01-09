from Modules import Module


class DeepVariant(Module):
    def __init__(self, module_id, is_docker = True):
        super(DeepVariant, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf_gz", "vcf_tbi", "gvcf_gz", "gvcf_tbi"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("bed",            is_required=True, is_resource=True)
        self.add_argument("deepvariant",    is_required=True, is_resource=True)

        self.add_argument("modeltype",      is_required=True, default_value="WES")
        self.add_argument("nr_cpus",        is_required=True, default_value=32)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus*2")

    def define_output(self):

        sample_name = self.get_argument("sample_name")

        # Make unique filename
        vcf_file = self.generate_unique_file_name(extension="{0}.vcf.gz".format(sample_name))
        vcf_tbi_file = "{0}.tbi".format(vcf_file)
        gvcf_file = self.generate_unique_file_name(extension="{0}.g.vcf.gz".format(sample_name))
        gvcf_tbi_file = "{0}.tbi".format(gvcf_file)

        self.add_output("vcf_gz",   vcf_file)
        self.add_output("vcf_tbi",  vcf_tbi_file)
        self.add_output("gvcf_gz",  gvcf_file)
        self.add_output("gvcf_tbi", gvcf_tbi_file)

    def define_command(self):

        # Get program options
        modeltype           = self.get_argument("modeltype")
        bamfile             = self.get_argument("bam")
        refgenome           = self.get_argument("ref")
        regionfile          = self.get_argument("bed")
        deepvariant         = self.get_argument("deepvariant")
        nr_cpus             = self.get_argument("nr_cpus")

        # Get output keys
        vcf_file            = self.get_output("vcf_gz")
        gvcf_file           = self.get_output("gvcf_gz")

        cmd = "{0} --model_type={1} --ref={2} --reads={3} --regions={4} --output_vcf={5} --output_gvcf={6} " \
              "--num_shards={7}".format(deepvariant, modeltype, refgenome, bamfile, regionfile, vcf_file,
                                        gvcf_file, nr_cpus)

        return cmd
