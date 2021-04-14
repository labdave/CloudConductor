from Modules import Merger

class AggregateTranslocations(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateTranslocations, self).__init__(module_id, is_docker)
        self.output_keys = ["merged_all_trls", "merged_filt_trls", "merged_fish_trls"]

    def define_input(self):
        self.add_argument("all_translocations",  is_required=True)
        self.add_argument("filt_translocations", is_required=True)
        self.add_argument("fish_translocations", is_required=True)
        self.add_argument("mem",                 default_value=10)
        self.add_argument("nr_cpus",             default_value=2)

    def define_output(self):
        merged_all_trls     = self.generate_unique_file_name(extension=".merged.all.tsv")
        self.add_output("merged_all_trls",  merged_all_trls)
        merged_filt_trls     = self.generate_unique_file_name(extension=".merged.filtered.tsv")
        self.add_output("merged_filt_trls",  merged_filt_trls)
        merged_fish_trls      = self.generate_unique_file_name(extension=".merged.FISH.tsv")
        self.add_output("merged_fish_trls",   merged_fish_trls)

    def define_command(self):
        all_translocations  = self.get_argument("all_translocations")
        filt_translocations = self.get_argument("filt_translocations")
        fish_translocations = self.get_argument("fish_translocations")

        # Turn all inputs into lists if they're not already for the later .join
        if not isinstance(all_translocations, list):
            all_translocations = [all_translocations]
        if not isinstance(filt_translocations, list):
            filt_translocations = [filt_translocations]
        if not isinstance(fish_translocations, list):
            fish_translocations = [fish_translocations]

        merged_all_trls  = self.get_output("merged_all_trls")
        merged_filt_trls = self.get_output("merged_filt_trls")
        merged_fish_trls = self.get_output("merged_fish_trls")

        cmd = ""
        cmd += f"python merge_translocations.py {merged_all_trls} {' '.join(all_translocations)} !LOG3!; "
        cmd += f"python merge_translocations.py {merged_filt_trls} {' '.join(filt_translocations)} !LOG3!; "
        cmd += f"python merge_translocations.py {merged_fish_trls} {' '.join(fish_translocations)} !LOG3!; "

        return cmd

