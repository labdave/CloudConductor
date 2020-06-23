from os import path

from Modules import Module

class AlignmentBasedQuant(Module):
    def __init__(self, module_id, is_docker = False):
        super(AlignmentBasedQuant, self).__init__(module_id, is_docker)
        self.output_keys = ["quant_file"]

    def define_input(self):
        self.add_argument("transcriptome_mapped_bam",       is_required=True)
        self.add_argument("salmon",                         is_required=True, is_resource=True)
        self.add_argument("ref",                            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                        is_required=True, default_value=8)
        self.add_argument("mem",                            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare unique file name
        quant_file = path.join(self.get_output_dir(), "quant.sf")

        self.add_output("quant_file", quant_file)

    def define_command(self):

        # Get arguments
        bam             = self.get_argument("transcriptome_mapped_bam")
        salmon          = self.get_argument("salmon")
        ref             = self.get_argument("ref")
        nr_cpus         = self.get_argument("nr_cpus")

        # get the output dir
        output_dir = self.get_output_dir()

        # Generate command line for Salmon
        cmd = "{0} quant -t {1} -l A -a {2} -o {3} -p {4} !LOG3!".format(salmon, ref, bam, output_dir, nr_cpus)

        return cmd
