import os
from Modules import Module

class Star(Module):
    def __init__(self, module_id, is_docker = False):
        super(Star, self).__init__(module_id, is_docker)

        self.output_keys = ["bam", "transcriptome_mapped_bam", "raw_read_counts",
                            "splice_junction_out", "final_log"]

    def define_input(self):
        self.add_argument("R1",                         is_required=True)
        self.add_argument("R2")
        self.add_argument("star",                       is_required=True, is_resource=True)
        self.add_argument("ref",                        is_required=True, is_resource=True)
        self.add_argument("quant_mode",                 default_value="TranscriptomeSAM GeneCounts")
        self.add_argument("out_unmapped_within_sam",    default_value="Within")
        self.add_argument("output_file_type",           default_value="BAM SortedByCoordinate")
        self.add_argument("twopass_mode",               default_value="None")
        self.add_argument("read_group",                 is_required=True)
        self.add_argument("out_sam_mapq_unique",        is_required=False, default_value=60)
        self.add_argument("quantTranscriptomeBan",      is_required=False, default_value="Singleend")
        self.add_argument("nr_cpus",                    is_required=True, default_value=12)
        self.add_argument("mem",                        is_required=True, default_value=75)

    def define_output(self):
        # Declare unique file name
        output_prefix = self.generate_unique_file_name(extension=".txt").split(".")[0]

        self.add_output("bam", "{0}.Aligned.sortedByCoord.out.bam".format(output_prefix))
        self.add_output("transcriptome_mapped_bam", "{0}.Aligned.toTranscriptome.out.bam".format(output_prefix))
        self.add_output("raw_read_counts", "{0}.ReadsPerGene.out.tab".format(output_prefix))
        self.add_output("splice_junction_out", "{0}.SJ.out.tab".format(output_prefix))
        self.add_output("final_log", "{0}.Log.final.out".format(output_prefix))

    def define_command(self):

        # Get arguments to run STAR aligner
        R1                          = self.get_argument("R1")
        R2                          = self.get_argument("R2")
        star                        = self.get_argument("star")
        ref                         = self.get_argument("ref")
        quant_mod                   = self.get_argument("quant_mode")
        out_unmapped_within_sam     = self.get_argument("out_unmapped_within_sam")
        output_file_type            = self.get_argument("output_file_type")
        twopass_mode                = self.get_argument("twopass_mode")
        read_group                  = self.get_argument("read_group")
        out_sam_mapq_unique         = self.get_argument("out_sam_mapq_unique")
        quant_transcriptome_ban     = self.get_argument("quantTranscriptomeBan")
        nr_cpus                     = self.get_argument("nr_cpus")
        bam                         = self.get_output("bam").get_path()

        # Generate output file name prefix for STAR
        output_file_name_prefix = bam.split(".")[0] + "."

        # Check the input FASTQ format
        if R1.endswith(".gz"):
            read_file_command = "zcat"
        else:
            read_file_command = "-"

        # remove @RG text from the read group string as STAR adds it automatically and
        # required read group line to starts with ID
        read_group = read_group.replace("@RG\\t", "")
        read_group = read_group.replace("\\t", " ")

        # Design command line based on read type (i.e. paired-end or single-end)
        if self.get_argument("R2") is not None:
            cmd = "{0} --runThreadN {1} --genomeDir {2} --readFilesIn {3} {4} --outFileNamePrefix {5} --readFilesCommand {6} " \
                  "--quantMode {7} --outSAMunmapped {8} --outSAMtype {9} --twopassMode {10} --outSAMattrRGline {11} " \
                  "--outSAMmapqUnique {12} --quantTranscriptomeBan {13} !LOG3!".format(star, nr_cpus, ref, R1, R2,
                                                                                       output_file_name_prefix,
                                                                                       read_file_command, quant_mod,
                                                                                       out_unmapped_within_sam,
                                                                                       output_file_type, twopass_mode,
                                                                                       read_group, out_sam_mapq_unique,
                                                                                       quant_transcriptome_ban)
        else:
            cmd = "{0} --runThreadN {1} --genomeDir {2} --readFilesIn {3} --outFileNamePrefix {4} --readFilesCommand {5} " \
                  "--quantMode {6} --outSAMunmapped {7} --outSAMtype {8} --twopassMode {9} --outSAMattrRGline {10} " \
                  "--outSAMmapqUnique {11} --quantTranscriptomeBan {12} !LOG3!".format(star,nr_cpus, ref, R1,
                                                                                       output_file_name_prefix,
                                                                                       read_file_command, quant_mod,
                                                                                       out_unmapped_within_sam,
                                                                                       output_file_type, twopass_mode,
                                                                                       read_group, out_sam_mapq_unique,
                                                                                       quant_transcriptome_ban)

        return cmd
