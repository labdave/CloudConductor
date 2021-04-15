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
        self.add_argument("nr_cpus",        default_value=16)
        self.add_argument("mem",            default_value=60)


    def define_output(self):

        # Declare unique file name for bcf file
        sample_id       = self.get_argument("sample_id")
        fusions         = self.generate_unique_file_name(f"{sample_id}.fusions.tsv")
        self.add_output("fusions",          fusions)


    def define_command(self):

        # Get arguments to run Arriba
        R1              = self.get_argument("R1")
        R2              = self.get_argument("R2")
        fusion_ref      = self.get_argument("fusion_ref").split('/')[-1]
        nr_cpus         = self.get_argument("nr_cpus")

        # Get output paths
        fusions         = self.get_output("fusions")

        # Generate command
        cmd = "arriba_v2.1.0/run_arriba.sh "
        cmd += f"/data/{fusion_ref}/STAR_index_GRCh38_GENCODE28/ "
        cmd += f"/data/{fusion_ref}/GENCODE28.gtf "
        cmd += f"/data/{fusion_ref}/GRCh38.p12.genome.plus.ERCC.maskPAR.fa "
        cmd += f"/data/{fusion_ref}/blacklist_hg38_GRCh38_v2.0.0.tsv.gz "
        cmd += f"/data/{fusion_ref}/known_fusions_hg38_GRCh38_v2.0.0.tsv.gz "
        cmd += f"/data/{fusion_ref}/protein_domains_hg38_GRCh38_v2.0.0.gff3.gz "
        cmd += f"{nr_cpus} {R1} {R2} !LOG3!; mv fusions.tsv {fusions}"

        return cmd

class FusionAggregator(Module):
    def __init__(self, module_id, is_docker = False):
        super(FusionAggregator, self).__init__(module_id, is_docker)
        self.output_keys    = ["cleaned_fusions"]


    def define_input(self):
        self.add_argument("sample_id",      is_required=True)
        self.add_argument("fusions",        is_required=True)
        self.add_argument("merge_thresh",   default_value=10000)
        self.add_argument("nr_cpus",        default_value=2)
        self.add_argument("mem",            default_value=8)


    def define_output(self):

        # Declare unique file name for bcf file
        sample_id           = self.get_argument("sample_id")
        cleaned_fusions     = self.generate_unique_file_name(f"{sample_id}.fusions.clean.tsv")
        self.add_output("cleaned_fusions",  cleaned_fusions)


    def define_command(self):

        # Get arguments to run Arriba cleaner
        fusions             = self.get_argument("fusions")
        merge_thresh        = self.get_argument("merge_thresh")

        # Get output paths
        cleaned_fusions     = self.get_output("cleaned_fusions")

        # Generate command
        cmd = f"python3 Merge_fusions.py {fusions} {cleaned_fusions} {merge_thresh} !LOG3!"

        return cmd

class FusionWhitelist(Module):
    def __init__(self, module_id, is_docker = False):
        super(FusionWhitelist, self).__init__(module_id, is_docker)
        self.output_keys    = ["wl_fusions"]


    def define_input(self):
        self.add_argument("sample_id",          is_required=True)
        self.add_argument("cleaned_fusions",    is_required=True)
        self.add_argument("whitelist",          is_resource=True, is_required=True)
        self.add_argument("nr_cpus",            default_value=2)
        self.add_argument("mem",                default_value=8)


    def define_output(self):

        # Declare unique file name for bcf file
        sample_id           = self.get_argument("sample_id")
        wl_fusions          = self.generate_unique_file_name(f"{sample_id}.fusions.wl.tsv")
        wl_fusions_details  = self.generate_unique_file_name(f"{sample_id}.fusions.wl.detailed.tsv")
        self.add_output("wl_fusions",           wl_fusions)


    def define_command(self):

        # Get arguments to run Arriba whitelist
        cleaned_fusions     = self.get_argument("cleaned_fusions")
        whitelist           = self.get_argument("whitelist")

        # Get output paths
        wl_fusions          = self.get_output("wl_fusions")
        

        # Generate command
        cmd = f"python3 Whitelist_fusions.py {cleaned_fusions} {whitelist} {wl_fusions} !LOG3!"

        return cmd