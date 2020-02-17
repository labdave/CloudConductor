import os
from Modules import Module

class ABCGCBPlot(Module):
    def __init__(self, module_id, is_docker = False):
        super(ABCGCBPlot, self).__init__(module_id, is_docker)
        self.output_keys = ["abc_gcb_plots"]

    def define_input(self):
        self.add_argument("sample_name",                            is_required=True)
        self.add_argument("aggregated_normalized_gene_counts_long", is_required=True)
        self.add_argument("sample_disease",                         is_required=True)
        self.add_argument("abc_gcb_plot_script",                    is_required=True, is_resource=True)
        self.add_argument("generic_plot_script",                    is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                                is_required=True, default_value=4)
        self.add_argument("mem",                                    is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare output folder
        self.add_output("abc_gcb_plots", self.get_output_dir())

    def define_command(self):

        # Get arguments
        aggregated_normalized_gene_counts_long  = self.get_argument("aggregated_normalized_gene_counts_long")
        sample_disease                          = self.get_argument("sample_disease")
        abc_gcb_plot_script                     = self.get_argument("abc_gcb_plot_script")
        generic_plot_script                     = self.get_argument("generic_plot_script")

        output_file_prefix = self.generate_unique_file_name(extension=".txt")

        output_file_prefix = output_file_prefix.split('.')[0]

        output_file_prefix = os.path.join(self.get_output_dir(), output_file_prefix)

        cmds = []

        if not self.is_docker:
            cmds.append("sudo Rscript --vanilla {0} {1} {2} {3} !LOG3!".format(abc_gcb_plot_script,
                                                                               aggregated_normalized_gene_counts_long,
                                                                               sample_disease, output_file_prefix))
            cmds.append("sudo Rscript --vanilla {0} {1} {2} !LOG3!".format(generic_plot_script,
                                                                           aggregated_normalized_gene_counts_long,
                                                                           output_file_prefix))
        else:
            cmds.append("Rscript --vanilla {0} {1} {2} {3} !LOG3!".format(abc_gcb_plot_script,
                                                                          aggregated_normalized_gene_counts_long,
                                                                          sample_disease, output_file_prefix))
            cmds.append("Rscript --vanilla {0} {1} {2} !LOG3!".format(generic_plot_script,
                                                                      aggregated_normalized_gene_counts_long,
                                                                      output_file_prefix))

        cmd = " ; ".join(cmds)

        return cmd
