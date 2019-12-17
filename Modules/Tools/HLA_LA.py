import os

from Modules import Module

# Module created using CC_module_helper.py
class HLA_LA(Module):
    def __init__(self, module_id, is_docker=False):
        super(HLA_LA, self).__init__(module_id, is_docker)
        # Add output keys here if needed
        self.output_keys = ["hla_report"]

    def define_input(self):
        # Module creator needs to define which arguments have is_resource=True
        # Module creator needs to rename arguments as required by CC
        self.add_argument("hla-la",           is_resource=True, is_required=True)
        self.add_argument("nr_cpus",          default_value=16)
        self.add_argument("mem",              default_value=60)
        self.add_argument("workingDir")
        self.add_argument("bam",              is_required=True)
        self.add_argument("bam_idx",          is_required=True)
        self.add_argument("graph",            is_resource=True, is_required=True)
        self.add_argument("sample_name",      is_required=True)
        self.add_argument("ref",              is_resource=True, is_required=True)
        self.add_argument("additional_ref",   is_resource=True)

    def define_output(self):
        # Module creator needs to define what the outputs are
        # based on the output keys provided during module creation
        workingDir        = self.get_argument("workingDir")
        sampleID          = self.get_argument("sample_name")
        if workingDir:
            report_path = os.path.join(workingDir, sampleID, "hla/R1_bestguess_G.txt")
        else:
            report_path = os.path.join(self.output_dir, sampleID, "hla/R1_bestguess_G.txt")
        self.add_output("hla_report", report_path)


    def define_command(self):
        # Module creator needs to use renamed arguments as required by CC
        hla_la              = self.get_argument("hla-la")
        workingDir          = self.get_argument("workingDir")
        bam_file            = self.get_argument("bam")
        graph               = self.get_argument("graph")
        additional_ref      = self.get_argument("additional_ref")
        sampleID            = self.get_argument("sample_name")
        reference_genome    = self.get_argument("ref")
        nr_cpus             = self.get_argument("nr_cpus")

        # Add the main graph to the correct path where HLA-LA is looking for it
        cmd = "ln -s {0} /usr/local/bin/HLA-LA/graphs ; ".format(graph)

        # Add the additional ref if provided
        if additional_ref:
            cmd += "ln -s {0} /usr/local/bin/HLA-LA/src/additionalReferences/PRG_MHC_GRCh38_withIMGT ; ".format(additional_ref)

        # add module
        cmd += hla_la

        # add required non-positional arguments
        cmd += " --bam {}".format(bam_file)
        cmd += " --graph {}".format(os.path.basename(graph))
        cmd += " --sampleID {}".format(sampleID)
        cmd += " --samtools_T {}".format(reference_genome)
        cmd += " --maxThreads {}".format(nr_cpus)

        # add optional arguments
        if workingDir:
            cmd += " --workingDir {}".format(workingDir)
        else:
            cmd += " --workingDir {}".format(self.output_dir)

        cmd += " !LOG3!"

        return cmd
