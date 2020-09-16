from Modules import Merger


# Module created using CC_module_helper.py
class SNPFingerprint(Merger):
    def __init__(self, module_id, is_docker=False):
        super(SNPFingerprint, self).__init__(module_id, is_docker)
        # Add output keys here if needed
        self.output_keys = ["snp_fingerprint", "snp_fingerprint_r", "snp_fingerprint_data"]

    def define_input(self):
        # Module creator needs to define which arguments have is_resource=True
        # Module creator needs to rename arguments as required by CC
        self.add_argument("bam", is_required=True)
        self.add_argument("bam_idx", is_required=True)
        self.add_argument("nr_cpus", default_value=8)
        self.add_argument("mem", default_value=48)
        self.add_argument("sample_id", is_required=True)
        self.add_argument("ref", is_required=True)
        self.add_argument("ref_idx", is_required=True)

    def define_output(self):
        # Module creator needs to define what the outputs are
        # based on the output keys provided during module creation
        snp_fingerprint = self.generate_unique_file_name("snp_fingerprint.txt")
        snp_fingerprint_r = self.generate_unique_file_name("snp_fingerprint.RData")
        snp_fingerprint_data = self.generate_unique_file_name("snp_fingerprint_data.txt")


        self.add_output("snp_fingerprint", snp_fingerprint)
        self.add_output("snp_fingerprint_r", snp_fingerprint_r)
        self.add_output("snp_fingerprint_data", snp_fingerprint_data)


    def define_command(self):
        # Module creator needs to use renamed arguments as required by CC
        sample_id = self.get_argument("sample_id")
        bam = self.get_argument("bam")
        ref = self.get_argument("ref")
        ref_idx = self.get_argument("ref_idx")

        # get output
        snp_fingerprint = self.get_output("snp_fingerprint")
        snp_fingerprint_r = self.get_output("snp_fingerprint_r")
        snp_fingerprint_data = self.get_output("snp_fingerprint_data")


        # add arguments
        cmd = " Rscript fp.R {0} {1} {2} {3} {4} {5} {6}".format( bam,ref, ref_idx, sample_id, snp_fingerprint_r, snp_fingerprint, snp_fingerprint_data)

        # add logging
        cmd += " !LOG3!"

        return cmd
