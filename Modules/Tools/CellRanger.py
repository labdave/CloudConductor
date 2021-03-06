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
        cellranger_dir  = os.path.join(self.get_output_dir(), sample_name)
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
        samples = []
        i = 1
        for i in range(len(R1)):
            # HUDSON ALPHA NAMING TEMPLATE
            lane_R1 = R1[i].split("-")[-2][-1]
            lane_R2 = R2[i].split("-")[-2][-1]
            
            id_R1 = "L00{0}_R1".format(lane_R1)
            id_R2 = "L00{0}_R2".format(lane_R2)
            if id_R1 not in samples:
                index = 1
                samples.append(id_R1)
            else:
                index = 2
            if id_R2 not in samples:
                index = 1
                samples.append(id_R2)
            else:
                index = 2
            new_R1 = "/data/fastqs/sample_S{0}_L00{1}_R1_001.fastq.gz".format(index, lane_R1)
            new_R2 = "/data/fastqs/sample_S{0}_L00{1}_R2_001.fastq.gz".format(index, lane_R2)
            mv_R1_cmd += "mv /data/{0} {1}; ".format(R1[i], new_R1)
            mv_R2_cmd += "mv /data/{0} {1}; ".format(R2[i], new_R2)

        # tar with z wasn't working so gunzip followed by tar xvf
        # next, remove the offending line in sourceme.bash and source it
        # next, rename fastqs according to 
        cmd = ""
        cmd += "gunzip {0}; tar -xvf {1}; ".format(cellranger, str(cellranger).rstrip(".gz"))
        cmd += "sed -i \"/CTYPE/d\" cellranger-4.0.0/sourceme.bash !LOG3!; "
        cmd += "gunzip {0}; tar -xvf {1}; ".format(transcriptome, str(transcriptome).rstrip(".gz"))
        cmd += "ls -l !LOG3!; ls -l /data/ !LOG3!; "
        cmd += "source {0} !LOG3!; mkdir /data/fastqs; ".format(source_path)
        cmd += mv_R1_cmd
        cmd += mv_R2_cmd
        cmd += "ls -l /data/fastqs/ !LOG3!; "
        cmd += "cellranger-4.0.0/cellranger count --id {0} --fastqs /data/fastqs/ --transcriptome refdata-gex-GRCh38-2020-A " \
              "--localcores {1} --localmem {2} !LOG3!; ".format(sample_name, nr_cpus, mem)
        cmd += "mv {0} {1}; ".format(sample_name, os.path.join(self.get_output_dir(), sample_name))
        cmd += "ls -l {0} !LOG3!; ls -l /data/output !LOG3! ".format(os.path.join(self.get_output_dir(), sample_name))

        return cmd
