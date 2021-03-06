import os.path
from Modules import Module
import logging


class DiscoWave(Module):
    def __init__(self, module_id, is_docker=True):
        super(DiscoWave, self).__init__(module_id, is_docker)
        self.output_keys = ["translocation_table", "non_split_bam", "non_split_bam_idx", "figure_dir"]

    def define_input(self):
        # Path to execution wrapper script
        self.add_argument("disco_wave",                 is_required=True, is_resource=True)

        # CPU and memory requirements
        self.add_argument("nr_cpus",                    is_required=True, default_value=8)
        self.add_argument("mem",                        is_required=True, default_value="nr_cpus * 2")

        # Inputs to disco-wave
        self.add_argument("sample_name",                  is_required=True)
        self.add_argument("bam",                        is_required=True)
        self.add_argument("bam_idx",                    is_required=True)

        # Options for disco-wave, set to disco-wave defaults
        self.add_argument("tiling_bed",                 default_value=None)
        self.add_argument("min_read_pairs",             default_value=5)
        self.add_argument("min_mapping_threshold",      default_value=20)
        
        
    def define_output(self):

        # Get the sample name
        sample_name = self.get_argument("sample_name")
        
        # We may get more than one sample name if this is being run on a merged BAM. Check that it's all the same sample
        # name, and raise an error if that's not the case.
        if isinstance(sample_name, list):
            sample_name = set(sample_name)
            if len(sample_name) != 1:
                logging.error("More than one unique sample provided. Please only run one sample at a time.")
                raise RuntimeError("More than one unique sample provided. Please only run one sample at a time!")

            # If we get here, we know that sample_name is a set with 1 element 
            sample_name = sample_name.pop()

        # Declare output file names
        translocation_table = self.generate_unique_file_name("{}.candidate_translocations.tsv".format(sample_name))
        bam                 = self.generate_unique_file_name("discordant_reads.diff_chrom.bam")
        bam_idx             = bam + ".bai"

        # Create figure directory
        # Initializing this way because unclear if generate_unique_file_name will copy a directory over
        figure_dir          = os.path.join(self.get_output_dir(), "{}.supporting_figures".format(sample_name))

        self.add_output("translocation_table", translocation_table)
        self.add_output("non_split_bam", bam)
        self.add_output("non_split_bam_idx", bam_idx)
        self.add_output("figure_dir", figure_dir)


    def define_command(self):

        # Get inputs
        input_bam               = self.get_argument("bam")
        disco_wave              = self.get_argument("disco_wave")
        nr_cpus                 = self.get_argument("nr_cpus")
        sample_name               = self.get_argument("sample_name")
        tiling_bed              = self.get_argument("tiling_bed")
        min_read_pairs          = self.get_argument("min_read_pairs")
        min_mapping_threshold   = self.get_argument("min_mapping_threshold")

        # Get output paths
        translocation_table     = self.get_output("translocation_table")
        output_bam              = self.get_output("non_split_bam")
        figure_dir              = self.get_output("figure_dir")


        cmd = "{0} {1} --sample_name {2}  --out_translocation_table {3} --out_discordant_bam {4} " \
        "--figure_output_dir {5} --min_read_pairs {6} --min_mapping_quality {7} --nr_cpus {8}".format(
            disco_wave, input_bam, sample_name, translocation_table, output_bam, figure_dir, min_read_pairs, 
            min_mapping_threshold, nr_cpus)

        # Add tiling BED if it was somehow defined. Default in tool is MYC/BCL2/BCL6.
        if tiling_bed != None:
            cmd += "--tiling_BED {0}".format(tiling_bed)

        cmd += " !LOG3!"

        if not self.is_docker:
            cmd = "sudo python3 " + cmd

        return cmd
