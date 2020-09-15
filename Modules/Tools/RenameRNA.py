import os
from Modules import Module

class RenameRNA(Module):
    def __init__(self, module_id, is_docker = False):
        super(RenameRNA, self).__init__(module_id, is_docker)
        self.output_keys    = ["rna_bam", "rna_bam_idx"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("nr_cpus",    default_value=1)
        self.add_argument("mem",        default_value=5)

    def define_output(self):
        # get bam file names from the sample sheet
        bam                 = self.get_argument("bam")
        self.add_output("rna_bam",      bam)

        rna_bam_idx         = self.get_output("rna_bam")+".bai"
        self.add_output("rna_bam_idx",  rna_bam_idx)

    def define_command(self):
        rna_bam             = self.get_output("rna_bam")

        cmd = "samtools index {} !LOG3!".format(rna_bam)
        return cmd