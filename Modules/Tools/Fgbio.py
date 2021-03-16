from Modules import Module
import logging

class ClipBam(Module):
    def __init__(self, module_id, is_docker = True):
        super(ClipBam, self).__init__(module_id, is_docker)
        self.output_keys = ["bam", "bam_idx"]

    def define_input(self):
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx", is_required=True)
        self.add_argument("ref", is_required=True, is_resource=True)
        self.add_argument("nr_cpus", is_required=True, default_value=3)
        self.add_argument("mem", is_required=True, default_value=10)

    def define_output(self):

        #generate unique file names
        bam = self.generate_unique_file_name(".hardclipped.bam")
        bam_idx = self.generate_unique_file_name(".hardclipped.bam.idx")

        self.add_output("bam", bam)
        self.add_output("bam_idx", bam_idx)

    def define_command(self):

        input_bam = self.get_argument("bam")
        ref=self.get_argument("ref")

        output_bam = self.get_output("bam")

        cmd = "fgbio ClipBam -i {0} -o {1} -r {2} --upgrade-clipping".format(input_bam, output_bam, ref)
        cmd += " !LOG3!"

        return cmd