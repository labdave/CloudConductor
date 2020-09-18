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
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")
        sample_name     = self.get_argument("sample_name")

        # use first one if sample name is a list
        if isinstance(sample_name, list):
            sample_name = sample_name[0]

        source_path     = "cellranger-4.0.0/sourceme.bash"
        transcriptome   = "refdata-gex-GRCh38-2020-A/"

        def __flatten(l):
            for el in l:
                if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
                    yield from __flatten(el)
                else:
                    yield el

        # if R1 and R2 are not a list, make it a list. else, flatten it
        if isinstance(R1, list):
            R1 = [str(i).split("/")[-1] for i in __flatten(R1)]
        else:
            R1 = [R1]
        if isinstance(R2, list):
            R2 = [str(i).split("/")[-1] for i in __flatten(R2)]
        else:
            R2 = [R2]

        # cellranger needs filename wrangling
        mv_R1_cmd = ""
        mv_R2_cmd = ""
        for i in range(len(R1)):
            # TEMPORARY HARDCODING
            lane_R1 = R1[i].split("-")[-2][-1]
            lane_R2 = R2[i].split("-")[-2][-1]

            new_R1 = "/data/fastqs/sample_s0_L00{0}_R1_00{1}.fastq.gz".format(lane_R1, i)
            new_R2 = "/data/fastqs/sample_s0_L00{0}_R2_00{1}.fastq.gz".format(lane_R2, i)
            mv_R1_cmd += "mv -u /data/{0} {1};".format(R1[i], new_R1)
            mv_R2_cmd += "mv -u /data/{0} {1};".format(R2[i], new_R2)

        cmd = "source {0} !LOG3!; mkdir /data/fastqs; {1}{2} ls /data/fastqs/ !LOG3!;" \
              "cellranger-4.0.0/cellranger count --id={3} --fastqs=/data/fastqs --sample=sample_s0_L000 " \
              "--transcriptome={4} --localcores={5} --localmem={6} !LOG3! ".format(
            source_path, mv_R1_cmd, mv_R2_cmd, sample_name, transcriptome, nr_cpus, mem)

        return cmd
