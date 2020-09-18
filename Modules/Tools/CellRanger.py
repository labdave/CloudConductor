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
        self.add_argument("cellranger",     is_required=True, is_resource=True)
        self.add_argument("transcriptome",  is_required=True, is_resource=True)
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value="MAX")
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 6.5")


    def define_output(self):
        sample_name     = self.get_argument("sample_name")
        # use first one if sample name is a list
        if isinstance(sample_name, list):
            sample_name = sample_name[0]
        cellranger_dir  = sample_name+"/outs/"
        self.add_output("cellranger_output_dir", cellranger_dir, is_path=True)


    def define_command(self):
        # Generate command for running Cell Ranger
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")
        sample_name     = self.get_argument("sample_name")
        cellranger      = self.get_argument("cellranger")
        transcriptome   = self.get_argument("transcriptome")

        # use first one if sample name is a list
        if isinstance(sample_name, list):
            sample_name = sample_name[0]

        source_path     = "cellranger-4.0.0/sourceme.bash"

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
            samp_R1 = R1[i].split("_")[1][-1]
            samp_R2 = R2[i].split("_")[1][-1]

            new_R1 = "/data/fastqs/sample_S{0}_L00{1}_R1_001.fastq.gz".format(samp_R1, lane_R1)
            new_R2 = "/data/fastqs/sample_S{0}_L00{1}_R2_001.fastq.gz".format(samp_R2, lane_R2)
            mv_R1_cmd += "mv -u /data/{0} {1};".format(R1[i], new_R1)
            mv_R2_cmd += "mv -u /data/{0} {1};".format(R2[i], new_R2)

        cmd = ""
        cmd += "tar -zxvf {0};".format(cellranger)
        cmd += "sed -i \"/CTYPE/d\" cellranger-4:0.0/sourceme.bash;"
        cmd += "tar -zxvf {0};".format(transcriptome)
        cmd += "ls -l !LOG3!; ls -l /data/ !LOG3!;"
        cmd += "source {0} !LOG3!; mkdir /data/fastqs;".format(source_path)
        cmd += mv_R1_cmd
        cmd += mv_R2_cmd
        cmd += "ls /data/fastqs/ !LOG3!;"
        cmd += "cellranger-4.0.0/cellranger count --id {0} --fastqs /data/fastqs/ --transcriptome {1} " \
              "--localcores {2} --localmem {3} !LOG3!".format(sample_name, transcriptome, nr_cpus, mem)

        return cmd
