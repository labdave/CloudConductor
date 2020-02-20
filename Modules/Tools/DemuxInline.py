import os

from Modules import Module

class DemuxTNA (Module):
    def __init__(self, module_id, is_docker = False):
        super(DemuxTNA, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2", "demux_stats", "assay_type"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("barcodes",       is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=8)

        # Optional cutadapt arguments
        self.add_argument("max_error_rate", is_required=True, default_value=0.2)

        self.add_argument("tna_demux_script",   is_resource=True, is_required=True)


    def define_output(self):

        sample_name = self.get_argument("sample_name")

        # Create list of R1 files
        R1 = [
            os.path.join(self.get_output_dir(), "{0}.RNA.R1.fastq.gz".format(sample_name)),
            os.path.join(self.get_output_dir(), "{0}.non-RNA.R1.fastq.gz".format(sample_name))
        ]
        self.add_output("R1", R1)

        # Create list of R2 files
        R2 = [
            os.path.join(self.get_output_dir(), "{0}.RNA.R2.fastq.gz".format(sample_name)),
            os.path.join(self.get_output_dir(), "{0}.non-RNA.R2.fastq.gz".format(sample_name))
        ]
        self.add_output("R2", R2)

        # Create name for barcode stat file
        demux_stats = os.path.join(self.get_output_dir(), "all_barcode_stats.csv")
        self.add_output("demux_stats", demux_stats)

        # Create list of assay types
        assay_type = ["rna", "dna"]
        self.add_output("assay_type", assay_type, is_path=False)

    def define_command(self):

        # Get program options
        sample_name     = self.get_argument("sample_name")
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        barcodes        = self.get_argument("barcodes")
        max_error_rate  = self.get_argument("max_error_rate")
        demux_script    = self.get_argument("tna_demux_script")

        #convert the barcode list into a string delimited by comma
        barcodes = ",".join(barcodes)

        cmd = "{0} -s {1} -b {2} -e {3} -f {4} -r {5} -o {6}".format(demux_script, sample_name, barcodes,
                                                                     max_error_rate, R1, R2, self.get_output_dir())

        return cmd