from os import path
from Modules import Module

class Tximport(Module):
    def __init__(self, module_id, is_docker = False):
        super(Tximport, self).__init__(module_id, is_docker)
        self.output_keys = ["quant_gene_counts", "quant_gene_tpm"]

    def define_input(self):
        self.add_argument("quant_file",         is_required=True)
        self.add_argument("tx2gene",            is_required=True, is_resource=True)
        self.add_argument("tximport_script",    is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=4)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare unique file names
        # quant_gene_counts   = self.generate_unique_file_name(extension=".quant.gene.counts")
        # quant_gene_tpm      = self.generate_unique_file_name(extension=".quant.gene.tpm")

        quant_gene_counts   = path.join(self.get_output_dir(), "quant.gene.counts")
        quant_gene_tpm      = path.join(self.get_output_dir(), "quant.gene.tpm")

        self.add_output("quant_gene_counts", quant_gene_counts)
        self.add_output("quant_gene_tpm", quant_gene_tpm)

    def define_command(self):

        # Get arguments
        tximport_script = self.get_argument("tximport_script")
        quant_file      = self.get_argument("quant_file")
        tx2gene         = self.get_argument("tx2gene")

        output_dir = self.get_output_dir()

        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} {1} {2} {3} !LOG3!".format(tximport_script,quant_file,output_dir,tx2gene)
        else:
            cmd = "Rscript --vanilla {0} {1} {2} {3} !LOG3!".format(tximport_script,quant_file,output_dir,tx2gene)

        return cmd
