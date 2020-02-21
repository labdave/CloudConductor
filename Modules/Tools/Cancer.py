import os
from Modules import Module


class DemuxTNA(Module):
    def __init__(self, module_id, is_docker=True):
        super().__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2", "demux_stats", "assay_type"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("barcodes",       is_required=True)
        r1 = self.get_argument("R1")
        if isinstance(r1, list):
            nr_cpus = len(r1)
        else:
            nr_cpus = 1
        self.add_argument("nr_cpus",        is_required=True, default_value=nr_cpus)
        self.add_argument("mem",            is_required=True, default_value=4)

        # Mismatch percentage
        self.add_argument("max_error_rate", is_required=False, default_value=0.2)

    def output_paths(self):
        sample_name = self.get_argument("sample_name")

        # Create list of R1 files
        r1 = [
            os.path.join(self.get_output_dir(), "{0}.RNA.R1.fastq.gz".format(sample_name)),
            os.path.join(self.get_output_dir(), "{0}.non-RNA.R1.fastq.gz".format(sample_name))
        ]

        # Create list of R2 files
        r2 = [
            os.path.join(self.get_output_dir(), "{0}.RNA.R2.fastq.gz".format(sample_name)),
            os.path.join(self.get_output_dir(), "{0}.non-RNA.R2.fastq.gz".format(sample_name))
        ]
        return r1, r2

    def define_output(self):
        r1, r2 = self.output_paths()
        self.add_output("R1", r1)
        self.add_output("R2", r2)

        # Create name for barcode stat file
        demux_stats = os.path.join(self.get_output_dir(), "all_barcode_stats.csv")
        self.add_output("demux_stats", demux_stats)

        # Create list of assay types
        assay_type = ["rna", "dna"]
        self.add_output("assay_type", assay_type, is_path=False)

    def define_command(self):
        r1 = self.get_argument("R1")
        if isinstance(r1, list):
            r1 = " ".join(r1)
        r2 = self.get_argument("R2")
        if isinstance(r2, list):
            r2 = " ".join(r2)
        barcodes = " ".join(self.get_argument("barcodes"))
        max_error_rate = self.get_argument("max_error_rate")

        out_r1, out_r2 = self.output_paths()
        cmd_list = [
            "python -m Cancer.main demux_inline --r1 %s --r2 %s " % (r1, r2) +
            "--barcode %s --output %s --error_rate %s --name %s !LOG3!" % (
                barcodes, self.get_output_dir(), max_error_rate, self.get_argument("sample_name")
            ),
            "cd %s" % self.get_output_dir(),
            "mv %sR1_matched.fastq.gz %s" % (self.get_output_dir(), out_r1[0]),
            "mv %sR2_matched.fastq.gz %s" % (self.get_output_dir(), out_r2[0]),
            "mv %sR1_unmatched.fastq.gz %s" % (self.get_output_dir(), out_r1[1]),
            "mv %sR2_unmatched.fastq.gz %s" % (self.get_output_dir(), out_r2[1])
        ]

        cmd = " && ".join(cmd_list)
        return cmd
