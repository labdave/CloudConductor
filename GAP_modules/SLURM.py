import time
import os
import subprocess as sp
import random
from GAP_interfaces import Main

class SLURM(Main):

    def __init__(self, config):
        Main.__init__(self, config)
        self.nodes     = config.cluster.nodes
        self.mincpus   = config.cluster.mincpus
        self.partition = config.cluster.partition
        self.mem       = config.cluster.mem

        self.temp_dir  = config.general.temp_dir

        self.job_name  = "GAP_%d" % random.randint(0, 10**6)

        self.mainID    = 0
        self.allocate()

    def __del__(self): 
        self.message("Deallocating resources.")
 
        if self.mainID != 0:
            sp.Popen(["scancel %d" % self.mainID], shell=True).wait()

    def allocate(self):       
        self.message("Allocating resources on cluster.")
  
        devnull = open(os.devnull, "w")
        # Allocating resources
        sp.Popen(["salloc --nodes=%d --partition=%s --mem=%s --no-shell --job-name=%s --mincpus=%d" % (self.nodes, self.partition, self.mem, self.job_name, self.mincpus)], stdout=devnull, stderr=devnull, shell=True).wait()

        # Obtaining main job id
        count = 0
        while self.mainID == 0:
            self.mainID = int(self.getJobID(self.job_name))           
            count += 1
            if count == 100:
                 self.error("Cluster resources could not be allocated!")

        # Making sure the resources were allocated
        count = 0
        self.message('Waiting for allocation.')
        while sp.Popen(["srun --quiet --jobid=%d echo" % self.mainID], stdout=devnull, stderr=devnull, shell=True).wait():
            time.sleep(1)
            count += 1
            if count == 10:
                self.error("Cluster resources could not be allocated!")
               
        self.message("Resources allocated successfully.")

    def getJobID(self, jobname):
        cluster_cmd = "sacct -oJobid,jobname --parsable2 -n"     
        
        if self.mainID != 0:
            cluster_cmd += " --jobs=%s" % self.mainID
        
        p = sp.Popen([cluster_cmd], stdout=sp.PIPE, shell=True)
        output = p.communicate()[0]

        data = output.strip("\n").split("\n")
        for pair in data:
            pair = pair.split("|")
            if pair[1] == jobname:
                return pair[0]

        return False

    def runCommand(self, job_name, cmd, nodes=1, mincpus=1):

        self.message("Running the following command:\n  %s" % cmd)

        # Creating a bash file and changing its permissions
        aux_sh = "%s/%s.sh" % (self.temp_dir, job_name)
        with open(aux_sh, "w") as f:
            f.write("#!/usr/bin/env bash\n")
            f.write(cmd)
        os.chmod(aux_sh, 0755)

        # Generating the command to be run
        cluster_cmd = "srun --job-name=%s --output=%s/%s.out --nodes=%d --mem=%s --partition=%s --mincpus=%d --jobid=%d %s" % (job_name, self.temp_dir, job_name, nodes, self.mem, self.partition, mincpus, self.mainID, aux_sh)

        # Launching the process
        proc = sp.Popen(cluster_cmd, shell=True)

        return proc

    def checkStatus(self, job_id):
        p = sp.Popen(["sacct -oState%%5 --noheader --jobs=%s" % job_id], stdout=sp.PIPE, shell=True)
        output = p.communicate()[0]
        
        if output.startswith("CANC"):
            return -1
        elif output.startswith("FAIL"):
            return 1
        elif output.startswith("COMP"):
            return 0
        else:
            return 2

    def validate(self):
        pass
