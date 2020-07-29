import os.path
from Modules import Module
import logging


class AnnotateSVRepeats(Module):
    def __init__(self, module_id, is_docker=True):
        super(AnnotateSVRepeats, self).__init__(module_id, is_docker)
        self.output_keys = ["anno_vcf"]

    def define_input(self):
        # CPU and memory requirements
        self.add_argument("nr_cpus",                    is_required=True, default_value=2)
        self.add_argument("mem",                        is_required=True, default_value=10)

        # Inputs
        self.add_argument("anno_vcf",                   is_required=True)
        self.add_argument("repeat_blacklist",           is_required=True, is_resource=True)
        self.add_argument("segmental_blacklist",        is_required=True, is_resource=True)
                
    def define_output(self):
        anno_vcf = self.generate_unique_file_name("anno.vcf")
        self.add_output("anno_vcf", anno_vcf)

    def define_command(self):

        # Get inputs
        input_table              = self.get_argument("anno_vcf")
        repeat_blacklist         = self.get_argument("repeat_blacklist")
        segmental_blacklist      = self.get_argument("segmental_blacklist")

        # Get output path
        output_table             = self.get_output("anno_vcf")

        # Add module and arguments
        cmd = "python annotate_sv_repeats.py {0} {1} {2} {3}".format(input_table, output_table, 
            repeat_blacklist, segmental_blacklist)

        # Add logging
        cmd += " !LOG3!"

        if not self.is_docker:
            cmd = "sudo " + cmd

        return cmd


class AnnotateSVBCellGenes(Module):
    def __init__(self, module_id, is_docker=True):
        super(AnnotateSVBCellGenes, self).__init__(module_id, is_docker)
        self.output_keys = ["anno_vcf"]

    def define_input(self):
        # CPU and memory requirements
        self.add_argument("nr_cpus",                    is_required=True, default_value=2)
        self.add_argument("mem",                        is_required=True, default_value=10)

        # Inputs
        self.add_argument("anno_vcf",                   is_required=True)
                
    def define_output(self):
        anno_vcf = self.generate_unique_file_name("anno.vcf")
        self.add_output("anno_vcf", anno_vcf)

    def define_command(self):

        # Get input and output paths
        input_table              = self.get_argument("anno_vcf")
        output_table             = self.get_output("anno_vcf")

        # Add module and arguments
        cmd = "python annotate_sv_b_cell_genes.py {0} {1}".format(input_table, output_table)

        # Add logging
        cmd += " !LOG3!"

        if not self.is_docker:
            cmd = "sudo " + cmd

        return cmd


class AnnotateSVPON(Module):
    def __init__(self, module_id, is_docker=True):
        super(AnnotateSVPON, self).__init__(module_id, is_docker)
        self.output_keys = ["anno_vcf"]

    def define_input(self):
        # CPU and memory requirements
        self.add_argument("nr_cpus",                    is_required=True, default_value=2)
        self.add_argument("mem",                        is_required=True, default_value=10)

        # Inputs
        self.add_argument("anno_vcf",                   is_required=True)
        self.add_argument("pon_tsv",                   is_required=True, is_resource=True)
                
    def define_output(self):
        anno_vcf = self.generate_unique_file_name("anno.vcf")
        self.add_output("anno_vcf", anno_vcf)

    def define_command(self):

        # Get inputs
        input_table              = self.get_argument("anno_vcf")
        pon_tsv                  = self.get_argument("pon_tsv")

        # Get output path
        output_table             = self.get_output("anno_vcf")

        # Add module and arguments
        cmd = "python annotate_sv_pon.py {0} {1} {2}".format(input_table, output_table, pon_tsv)

        # Add logging
        cmd += " !LOG3!"

        if not self.is_docker:
            cmd = "sudo " + cmd

        return cmd
