from Modules import Module


class Strelka2(Module):
    def __init__(self, module_id, is_docker = True):
        super(Strelka2, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf_gz", "vcf_tbi"]

        # Initialze Strelka2's run directory
        self.run_directory = None

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("bed_gz",         is_required=True)
        self.add_argument("bed_tbi",        is_required=True)
        self.add_argument("strelka2",       is_required=True, is_resource=True)

        self.add_argument("nr_cpus",        is_required=True, default_value=32)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus*2")

    def define_output(self):

        # Everything Strelka does, it does inside this directory
        self.run_directory = "{0}/RunDir".format(self.output_dir)

        # Make unique filename
        vcf_file = "{0}/results/variants/variants.vcf.gz".format(self.run_directory)
        vcf_tbi_file = "{0}.tbi".format(vcf_file)

        self.add_output("vcf_gz",   vcf_file)
        self.add_output("vcf_tbi",  vcf_tbi_file)

    def define_command(self):

        # Get program options
        bamlist             = self.get_argument("bam")
        refgenome           = self.get_argument("ref")
        regionfile          = self.get_argument("bed_gz")
        strelka2            = self.get_argument("strelka2")
        nr_cpus             = self.get_argument("nr_cpus")

        # Convert bam list into string
        if isinstance(bamlist, list):
            bam_string = " ".join(["--bam {0}".format(_bam) for _bam in bamlist])
        else:
            bam_string = "--bam {0}".format(bamlist)

        cmd1 = "{0} {1} --referenceFasta {2} --callRegions={3} --runDir {4}".format(
               strelka2, bam_string, refgenome, regionfile, self.run_directory)
        cmd2 = "{0}/runWorkflow.py -m local -j {1}".format(self.run_directory, nr_cpus)

        return [cmd1, cmd2]
