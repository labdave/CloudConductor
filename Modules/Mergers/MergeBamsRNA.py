import copy

from Modules import Merger

class MergeBamsRNA(Merger):

    def __init__(self, module_id, is_docker=False):
        super(MergeBams, self).__init__(module_id, is_docker)
        self.output_keys  = ["rna_bam", "rna_bam_idx"]

    def define_input(self):
        self.add_argument("rna_bam",            is_required=True)
        self.add_argument("rna_bam_idx",        is_required=True)
        self.add_argument("bam_sorted",     is_required=True, default_value=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 4")

    def define_output(self):
        # Declare merged bam output

        # Single bam input. No merging to do.
        if not isinstance(self.get_argument("rna_bam"), list):
            rna_bam_out = copy.deepcopy(self.arguments["rna_bam"].get_value())
            rna_bam_idx = copy.deepcopy(self.arguments["rna_bam_idx"].get_value())

        # Multiple bam input. Merge into single bam.
        else:
            rna_bam_out = self.generate_unique_file_name(extension=".bam")
            rna_bam_idx = "%s.bai" % rna_bam_out

        self.add_output("rna_bam",      rna_bam_out)
        self.add_output("rna_bam_idx",  rna_bam_idx)

    def define_command(self):
        # Obtaining the arguments
        rna_bam_list        = self.get_argument("rna_bam")
        rna_bam_idx_list    = self.get_argument("rna_bam_idx")
        samtools        = self.get_argument("samtools")
        nr_cpus         = self.get_argument("nr_cpus")
        sorted_input    = self.get_argument("bam_sorted")
        rna_output_bam      = self.get_output("rna_bam")
        rna_output_bam_idx  = self.get_output("rna_bam_idx")

        needs_merging   = rna_bam_list != rna_output_bam.get_path()

        # Don't do anything if single bam that already has index
        if not needs_merging:
            return None

        # Generating the merging command
        if sorted_input:
            merge_cmd = "%s merge -f -c -@%d %s %s" % (samtools,
                                                       nr_cpus,
                                                       rna_output_bam,
                                                       " ".join(rna_bam_list))
        else:
            merge_cmd = "%s cat -o %s %s" % (samtools,
                                             rna_output_bam,
                                             " ".join(rna_bam_list))

        # Generate command to make index
        index_cmd = "%s index %s %s" % (samtools, rna_output_bam, rna_output_bam_idx)

        # Return command for
        return "%s !LOG2! && %s !LOG2!" % (merge_cmd, index_cmd)

