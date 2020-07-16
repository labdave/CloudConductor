import os
from Modules import Module

class ABCGCBScoreSalmon(Module):
    def __init__(self, module_id, is_docker = False):
        super(ABCGCBScoreSalmon, self).__init__(module_id, is_docker)
        self.output_keys = ["abc_gcb_score", "normalized_gene_counts"]

    def define_input(self):
        self.add_argument("sample_name",            is_required=True)
        self.add_argument("ref",                    is_required=True, is_resource=True)
        self.add_argument("capture_gene_id",        is_required=True, is_resource=True)
        self.add_argument("quant_gene_counts",      is_required=True)
        self.add_argument("abc_gcb_score_script",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                is_required=True, default_value=2)
        self.add_argument("mem",                    is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare output file name
        self.add_output("abc_gcb_score", "{0}/abc_gcb_onesample_score.{1}.txt".format(self.get_output_dir(),
                                                                                      self.get_argument("sample_name")))
        self.add_output("normalized_gene_counts", "{0}/normalized_gene_counts.{1}.txt".format(self.get_output_dir(),
                                                                                              self.get_argument("sample_name")))

    def define_command(self):

        # Get arguments
        ref                 = self.get_argument("ref")
        capture_gene_id     = self.get_argument("capture_gene_id")
        sample_name         = self.get_argument("sample_name")
        quant_gene_counts   = self.get_argument("quant_gene_counts")
        script              = self.get_argument("abc_gcb_score_script")

        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} {1} {2} {3} {4} {5} !LOG3!".format(script, quant_gene_counts, ref,
                                                                                 capture_gene_id, sample_name,
                                                                                 self.get_output_dir())
        else:
            cmd = "Rscript --vanilla {0} {1} {2} {3} {4} {5} !LOG3!".format(script, quant_gene_counts, ref, capture_gene_id,
                                                                            sample_name, self.get_output_dir())

        return cmd
