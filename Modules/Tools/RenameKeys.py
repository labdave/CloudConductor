import os
from Modules import Module

class RenameRNA(Module):
    def __init__(self, module_id, is_docker=False):
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

        rna_bam_idx         = str(self.get_output("rna_bam"))+".bai"
        self.add_output("rna_bam_idx",  rna_bam_idx)

    def define_command(self):
        rna_bam             = self.get_output("rna_bam")

        cmd = "samtools index {} !LOG3!".format(rna_bam)
        return cmd


class RenameUMI(Module):
    def __init__(self, module_id, is_docker=False):
        super(RenameUMI, self).__init__(module_id, is_docker)
        self.output_keys = ["umi_bam"]

    def define_input(self):
        self.add_argument("bam", is_required=True)
        self.add_argument("nr_cpus", default_value=1)
        self.add_argument("mem", default_value=5)

    def define_output(self):
        # get bam file names from the sample sheet
        bam = self.get_argument("bam")
        self.add_output("umi_bam", bam)

    def define_command(self):
        cmd = "echo 'Wrapping input bam as umi_bam key...' !LOG3!"
        return cmd

class RenameTranscriptomeBamToBam(Module):
    def __init__(self, module_id, is_docker=False):
        super(RenameTranscriptomeBamToBam, self).__init__(module_id, is_docker)
        self.output_keys = ["bam"]

    def define_input(self):
        self.add_argument("transcriptome_mapped_bam", is_required=True)
        self.add_argument("nr_cpus", default_value=1)
        self.add_argument("mem", default_value=5)

    def define_output(self):
        # get bam file names from the sample sheet
        bam = self.get_argument("transcriptome_mapped_bam")
        self.add_output("bam", bam)

    def define_command(self):
        cmd = "echo 'Wrapping input transcriptome_mapped_bam as bam key...' !LOG3!"
        return cmd

class RenameBamToTranscriptomeBam(Module):
    def __init__(self, module_id, is_docker=False):
        super(RenameBamToTranscriptomeBam, self).__init__(module_id, is_docker)
        self.output_keys = ["transcriptome_mapped_bam"]

    def define_input(self):
        self.add_argument("bam", is_required=True)
        self.add_argument("nr_cpus", default_value=1)
        self.add_argument("mem", default_value=5)

    def define_output(self):
        # get bam file names from the sample sheet
        bam = self.get_argument("bam")
        self.add_output("transcriptome_mapped_bam", bam)

    def define_command(self):
        cmd = "echo 'Wrapping input bam as transcriptome_mapped_bam key...' !LOG3!"
        return cmd
