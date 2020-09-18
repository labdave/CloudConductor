import collections
import os
import logging

from Modules import Module

class CellRanger(Module):
    def __init__(self, module_id, is_docker = False):
        super(CellRanger, self).__init__(module_id, is_docker)
        self.output_keys = ["cellranger_output_dir"]


    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value="MAX")
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 6.5")


    def define_output(self):
        cellranger_output_dir = os.path.join(self.output_dir, "outs")
        self.add_output("cellranger_output_dir", self.output_dir, is_path=True)


    def define_command(self):
        # Generate command for running Cell Ranger
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")
        sample_name     = self.get_argument("sample_name")

        # use first one if sample name is a list
        if isinstance(sample_name, list):
            sample_name = sample_name[0]

        source_path     = "cellranger-4.0.0/sourceme.bash"
        transcriptome   = "refdata-gex-GRCh38-2020-A/"

        cmd = "source {0}; " \
              "cellranger-4.0.0/cellranger count --id={1} --fastqs=/data/ " \
              "--transcriptome={2} --localcores={3} --localmem={4} !LOG3! ".format(
            source_path, sample_name, transcriptome, nr_cpus, mem)

        return cmd
