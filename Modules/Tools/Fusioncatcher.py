from Modules import Module

class Fusioncatcher(Module):
    def __init__(self, module_id, is_docker = False):
        super(Fusioncatcher, self).__init__(module_id, is_docker)
        self.output_keys = ["fusion_genes"]


    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("sample_id",      is_required=True)
        self.add_argument("fusioncatcher",  is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value=30)


    def define_output(self):

        # Declare unique file name for bcf file
        sample_id       = self.get_argument("sample_id")
        fusion_genes    = self.generate_unique_file_name("{}.fusion_genes.txt".format(sample_id))
        self.add_output("fusion_genes", fusion_genes)


    def define_command(self):

        # Get arguments to run Delly
        input_folder    = "/data/fusioncatcher/{}/".format(self.get_argument("sample_id"))
        output_folder   = "/fusioncatcher_temp/"

        # Generate command
        cmd = "bash fusion_catcher.sh {0} {1} !LOG3!".format(input_folder, output_folder)

        fusion_genes = self.get_output("fusion_genes")
        cmd += ";cp /fusioncatcher_temp/final-list_candidates-fusion-genes.txt {}".format(fusion_genes)

        return cmd
