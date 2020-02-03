import os
from Modules import Module

class ABCGCBScore(Module):
    def __init__(self, module_id, is_docker = False):
        super(ABCGCBScore, self).__init__(module_id, is_docker)
        self.output_keys = ["abc_gcb_score", "normalized_gene_counts"]

    def define_input(self):
        self.add_argument("sample_name",            is_required=True)
        self.add_argument("sample_type",            is_required=True)
        self.add_argument("ref",                    is_required=True, is_resource=True)
        self.add_argument("read_counts",            is_required=True)
        self.add_argument("abc_gcb_score_script",   is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                is_required=True, default_value=2)
        self.add_argument("mem",                    is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare output file name
        self.add_output("abc_gcb_score", "{0}/abc_gcb_onesample_score.{1}.txt".format(self.get_output_dir(),
                                                                                      self.get_argument("sample_type")))
        self.add_output("normalized_gene_counts", "{0}/normalized_gene_counts.{1}.txt".format(self.get_output_dir(),
                                                                                              self.get_argument("sample_type")))

    def define_command(self):

        # Get arguments
        ref             = self.get_argument("ref")
        sample_type     = self.get_argument("sample_type")
        read_counts     = self.get_argument("read_counts")
        script          = self.get_argument("abc_gcb_score_script")

        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} {1} {2} {3} {4} !LOG3!".format(script, read_counts, ref, sample_type,
                                                                             self.get_output_dir())
        else:
            cmd = "Rscript --vanilla {0} {1} {2} {3} {4} !LOG3!".format(script, read_counts, ref, sample_type,
                                                                        self.get_output_dir())

        return cmd
