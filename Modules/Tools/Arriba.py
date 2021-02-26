from Modules import Module

class Arriba(Module):
    def __init__(self, module_id, is_docker = False):
        super(Arriba, self).__init__(module_id, is_docker)
        self.output_keys = ["fusions"]


    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("sample_id",      is_required=True)
        self.add_argument("fusion_ref",     is_resource=True)
        self.add_argument("nr_cpus",        default_value=2)
        self.add_argument("mem",            default_value=13)


    def define_output(self):

        # Declare unique file name for bcf file
        sample_id       = self.get_argument("sample_id")
        fusions         = self.generate_unique_file_name(f"{sample_id}.fusions.tsv")
        self.add_output("fusions",          fusions)


    def define_command(self):

        # Get arguments to run Arriba
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        fusion_ref      = self.get_argument("fusion_ref")
        nr_cpus         = self.get_argument("nr_cpus")

        # Get output paths
        fusions         = self.get_output("fusions")

        # Generate command
        cmd = ""
        cmd += f"docker run --rm --user root -v \"${PWD}/output:/output\" -v \"${PWD}/../data/{fusion_ref}:/references:ro\""
        cmd += f" -v \"${PWD}/../data/{R1}:/read1.fastq.gz:ro\" -v \"${PWD}/../data/{R1}:/read2.fastq.gz:ro\""
        cmd += f" uhrigs/arriba:2.0.0 /bin/bash -c \"sed -i 's/$THREADS/{nr_cpus}/g' arriba*/run_arriba.sh; arriba.sh\""
        cmd += f" !LOG3!"
        return cmd
