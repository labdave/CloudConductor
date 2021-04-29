import copy

from Modules import Merger

class MergeTranscriptomeBams(Merger):

    def __init__(self, module_id, is_docker=False):
        super(MergeTranscriptomeBams, self).__init__(module_id, is_docker)
        self.output_keys  = ["transcriptome_mapped_bam"]

    def define_input(self):
        self.add_argument("spliced_rna_transcriptome_bam")
        self.add_argument("short_insert_rna_transcriptome_bam")
        self.add_argument("long_insert_rna_transcriptome_bam")
        self.add_argument("spliced_dna_transcriptome_bam")
        self.add_argument("long_insert_dna_transcriptome_bam")
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 4")

    def define_output(self):
        # Declare merged bam output

        bam_out = self.generate_unique_file_name(extension=".bam")
        # bam_idx = "%s.bai" % bam_out

        self.add_output("transcriptome_mapped_bam", bam_out)
        # self.add_output("transcriptome_bam_idx",    bam_idx)

    def define_command(self):
        # Obtaining the arguments
        spliced_rna_transcriptome_bam       = self.get_argument("spliced_rna_transcriptome_bam")
        short_insert_rna_transcriptome_bam  = self.get_argument("short_insert_rna_transcriptome_bam")
        long_insert_rna_transcriptome_bam   = self.get_argument("long_insert_rna_transcriptome_bam")
        spliced_dna_transcriptome_bam       = self.get_argument("spliced_dna_transcriptome_bam")
        long_insert_dna_transcriptome_bam   = self.get_argument("long_insert_dna_transcriptome_bam")
        samtools                            = self.get_argument("samtools")
        nr_cpus                             = self.get_argument("nr_cpus")

        transcriptome_mapped_bam            = self.get_output("transcriptome_mapped_bam")

        # list of all available transcriptome bams
        bams = [spliced_rna_transcriptome_bam, short_insert_rna_transcriptome_bam, long_insert_rna_transcriptome_bam,
                spliced_dna_transcriptome_bam, long_insert_dna_transcriptome_bam]

        if all(bam is None for bam in bams):
            raise Exception("No transcriptome bam is provided.")

        # remove all the missing bams
        bams = [bam for bam in bams if bam]

        # generating the merging command
        merge_cmd = f'{samtools} merge -@ {nr_cpus} {transcriptome_mapped_bam} {" ".join(bams)} !LOG3!'

        # generating the index command
        # sort_merge_cmd = f'{samtools} index -@ {nr_cpus} {transcriptome_mapped_bam}'

        # final cmd to return
        # cmd = f'{";".join([merge_cmd, sort_merge_cmd])} !LOG2!'

        return merge_cmd
