import os
import logging

from Modules import Module

class CellRanger(Module):
    def __init__(self, module_id, is_docker = False):
        super(CellRanger, self).__init__(module_id, is_docker, is_resumable=True)
        self.output_keys = ["cellranger_output_dir"]

    def define_input(self):
        self.add_argument("sample_name",            is_required=True)
        self.add_argument("R1",                     is_required=True)
        self.add_argument("R2",                     is_required=True)
        self.add_argument("cellranger",             is_required=True, is_resource=True)
        self.add_argument("singlecell_rnaseq_ref",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                is_required=True, default_value="MAX")
        self.add_argument("mem",                    is_required=True, default_value="nr_cpus * 6.5")

    def define_output(self):
        # Declare cell ranger output dir
        sample_name = self.get_argument("sample_name")

        # Sample "names" are now the sample_id + the submission id, so we could have multiple sample "names"
        # If so, just use the first one as the sample name for all of them
        # Could parse out the sample ID to make this cleaner
        if isinstance(sample_name, list) and (len(sample_name) > 0):
            sample_name = sample_name[0]

        cellranger_output_dir = os.path.join(self.output_dir, sample_name)
        self.add_output("cellranger_output_dir", cellranger_output_dir, is_path=True)

    def define_command(self):
        # Generate command for running Cell Ranger
        cellranger      = self.get_argument("cellranger")
        sample_name     = self.get_argument("sample_name")
        transcriptome   = self.get_argument("singlecell_rnaseq_ref")
        nr_cpus         = self.get_argument("nr_cpus")
        mem             = self.get_argument("mem")
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")

        cellranger_dir  = os.path.dirname(cellranger)
        source_path     = os.path.join(cellranger_dir, "sourceme.bash")
        wrk_dir         = self.get_output_dir()

        # Sample "names" are now the sample_id + the submission id, so we could have multiple sample "names"
        # If so, just use the first one as the sample name for all of them
        # Could parse out the sample ID to make this cleaner
        if isinstance(sample_name, list) and (len(sample_name) > 0):
            sample_name = sample_name[0]

        # We accommodate two idiosynchroses of Cell Ranger:
        # 1. CR accepts a folder of fastqs, not a list of files,
        #    so we move the fastqs to fastq_dir before calling Cell Ranger
        # 2. CR does not recognize HudsonAlpha fastq filenames, so we rename
        #    the fastq files when moving

        # In this case, the fastqs are formatted by HudsonAlpha demultiplex
        #   conventions:
        #   [Flowcell]_s[Lane Number]_[Read Type]_[Barcode]_[Sequencing Library ID].fastq.gz
        # We need to coerce these into bcl2fastq standards so that
        #   Cell Ranger recognizes them:
        #   [Sample Name]_S1_L00[Lane Number]_[Read Type]_001.fastq.gz

        # Check inputs
        if not (R1 and R2):
            logging.error("CellRanger module: incorrect sample inputs"
                          "for HudsonAlpha demux platform")
            raise Exception("CellRanger module: incorrect sample inputs")

        fastq_dir = os.path.join(wrk_dir, "fastqs")

        # Make sure that the fastq directory is formatted like a directory
        if not fastq_dir.endswith("/"):
            fastq_dir += "/"

        # Coerce R1, R2 to lists even if they're single files
        # This means R1, R2 can be list of fastq files or a single fastq
        if not isinstance(R1, list):
            R1 = [R1]
            R2 = [R2]

        mv_R1_cmd = ""
        mv_R2_cmd = ""
        for i in range(len(R1)):
            new_R1 = os.path.join(fastq_dir,
                                  "sample_s0_L000_R1_00{0}.fastq.gz".format(i))
            new_R2 = os.path.join(fastq_dir,
                                  "sample_s0_L000_R2_00{0}.fastq.gz".format(i))
            mv_R1_cmd += "mv -u {0} {1};".format(R1[i], new_R1)
            mv_R2_cmd += "mv -u {0} {1};".format(R2[i], new_R2)

        # If interrupted, the lock file needs to be removed before restarting,
        # so we remove the lock file just in case it exists
        cmd = "cd {0}; source {1}; rm -f {0}{2}/_lock; mkdir -p {3}; {4} {5} " \
              "cellranger count --id={2} --fastqs={3} " \
              "--transcriptome={6} --localcores={7} --localmem={8} !LOG3! ".format(
            wrk_dir, source_path, sample_name, fastq_dir, mv_R1_cmd, mv_R2_cmd,
            transcriptome, nr_cpus, mem)

        return cmd
