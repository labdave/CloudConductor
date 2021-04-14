from Modules import Merger

class AggregateTranslocations(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateTranslocations, self).__init__(module_id, is_docker)
        self.output_keys = ["concatenated_all_trls", "concatenated_filt_trls", 
        "concatenated_fish_trls", "collapsed_filt_trls", "collapsed_fish_trls"]

    def define_input(self):
        self.add_argument("all_translocations",  is_required=True)
        self.add_argument("filt_translocations", is_required=True)
        self.add_argument("fish_translocations", is_required=True)
        self.add_argument("merge_distance",      default_value=10)
        self.add_argument("mem",                 default_value=10)
        self.add_argument("nr_cpus",             default_value=2)

    def define_output(self):
        concatenated_all_trls     = self.generate_unique_file_name(extension=".concat.all.tsv")
        self.add_output("concatenated_all_trls",  concatenated_all_trls)
        concatenated_filt_trls     = self.generate_unique_file_name(extension=".concat.filtered.tsv")
        self.add_output("concatenated_filt_trls",  concatenated_filt_trls)
        concatenated_fish_trls      = self.generate_unique_file_name(extension=".concat.FISH.tsv")
        self.add_output("concatenated_fish_trls",   concatenated_fish_trls)
        collapsed_filt_trls      = self.generate_unique_file_name(extension=".collapsed.filtered.tsv")
        self.add_output("collapsed_filt_trls",   collapsed_filt_trls)
        collapsed_fish_trls      = self.generate_unique_file_name(extension=".collapsed.FISH.tsv")
        self.add_output("collapsed_fish_trls",   collapsed_fish_trls)
        

    def define_command(self):
        all_translocations  = self.get_argument("all_translocations")
        filt_translocations = self.get_argument("filt_translocations")
        fish_translocations = self.get_argument("fish_translocations")
        merge_distance      = self.get_argument("merge_distance")

        # Turn all inputs into lists if they're not already for the later .join
        if not isinstance(all_translocations, list):
            all_translocations = [all_translocations]
        if not isinstance(filt_translocations, list):
            filt_translocations = [filt_translocations]
        if not isinstance(fish_translocations, list):
            fish_translocations = [fish_translocations]

        concatenated_all_trls  = self.get_output("concatenated_all_trls")
        concatenated_filt_trls = self.get_output("concatenated_filt_trls")
        concatenated_fish_trls = self.get_output("concatenated_fish_trls")

        collapsed_filt_trls = self.get_output("collapsed_filt_trls")
        collapsed_fish_trls = self.get_output("collapsed_fish_trls")

        cmd = ""
        cmd += f"python concatenate_translocations.py {concatenated_all_trls} {' '.join(all_translocations)} !LOG3!; "
        cmd += f"python concatenate_translocations.py {concatenated_filt_trls} {' '.join(filt_translocations)} !LOG3!; "
        cmd += f"python concatenate_translocations.py {concatenated_fish_trls} {' '.join(fish_translocations)} !LOG3!; "
        cmd += f"Rscript collapse_translocations.R -o {collapsed_filt_trls} -i {','.join(filt_translocations)} -d {merge_distance} !LOG3!; "
        cmd += f"Rscript collapse_translocations.R -o {collapsed_fish_trls} -i {','.join(fish_translocations)} -d {merge_distance} !LOG3!; "

        return cmd

