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
