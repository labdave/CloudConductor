import os.path
from Modules import Module
import logging


class DiscordantReadsPileup(Module):
    def __init__(self, module_id, is_docker=True):
        super(DiscordantReadsPileup, self).__init__(module_id, is_docker)
        self.output_keys = ["raw_translocation_table", "annotated_translocation_table"]

    def define_input(self):
        self.add_argument("sample_name",                is_required=True)
        self.add_argument("bam",                        is_required=True)
        self.add_argument("bam_idx",                    is_required=True)

        # Path to execution wrapper script
        self.add_argument("discordant_reads_pileup",    is_required=True, is_resource=True)

        # BED files
        # If filter_bed is set to -1, the reads will not be filtered and translocations are called from whole genome
        self.add_argument("filter_bed",                 is_required=False, is_resource=True, default_value=-1)
        self.add_argument("gene_bed",                   is_required=False, is_resource=True)
        self.add_argument("promoter_bed",               is_required=False, is_resource=True)
        self.add_argument("bed",                        is_required=False, is_resource=True)
        self.add_argument("literature_bed",             is_required=False, is_resource=True)

        self.add_argument("nr_cpus",                    is_required=True, default_value=2)
        self.add_argument("mem",                        is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Get the sample name
        sample_name = self.get_argument("sample_name")

        # generate the output file
        output_dir = self.get_output_dir()

        # We may get more than one sample name if this is being run on a merged BAM. Check that it's all the same sample
        # name, and raise an error if that's not the case.
        if isinstance(sample_name, list):
            sample_name = set(sample_name)
            if len(sample_name) != 1:
                logging.error("More than one unique sample provided. Please only run one sample at a time.")
                raise RuntimeError("More than one unique sample provided. Please only run one sample at a time!")

            sample_name = list(sample_name)[0]

        output_prefix = os.path.join(output_dir, sample_name)
        
        # Declare vanilla translocations and annotated output file names
        self.add_output("raw_translocation_table", "{0}_raw_translocations.tsv".format(output_prefix))
        self.add_output("annotated_translocation_table", "{0}_annotated_translocations.tsv".format(output_prefix))

    def define_command(self):

        # Get inputs
        input_bam               = self.get_argument("bam")
        discordant_reads_pileup = self.get_argument("discordant_reads_pileup")
        nr_cpus                 = self.get_argument("nr_cpus")
        filter_bed              = self.get_argument("filter_bed")
        gene_bed                = self.get_argument("gene_bed")
        promoter_bed            = self.get_argument("promoter_bed")
        target_bed              = self.get_argument("bed")
        literature_bed          = self.get_argument("literature_bed")

        # Get output path
        raw_output_path         = self.get_output("raw_translocation_table")
        annotated_output_path   = self.get_output("annotated_translocation_table")

        cmd = "{0} {1} {2} {3} --nr_cpus {4}".format(discordant_reads_pileup, input_bam, raw_output_path,
                                                     annotated_output_path, nr_cpus)

        # Add on BED files if defined. Otherwise, the program will use its defaults.
        if filter_bed:
            cmd += " --BED_filter {0}".format(filter_bed)
        if gene_bed:
            cmd += " --gene_bed {0}".format(gene_bed)
        if promoter_bed:
            cmd += " --promoter_bed {0}".format(promoter_bed)
        if target_bed:
            cmd += " --target_bed {0}".format(target_bed)
        if literature_bed:
            cmd += " --literature_bed {0}".format(literature_bed)

        cmd += " !LOG3!"

        if not self.is_docker:
            cmd = "sudo " + cmd

        return cmd
