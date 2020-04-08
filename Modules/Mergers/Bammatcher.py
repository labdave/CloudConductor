from Modules import PseudoMerger, Merger

# Module created using CC_module_helper.py
class Bammatcher(PseudoMerger):
    def __init__(self, module_id, is_docker=False):
        super(Bammatcher, self).__init__(module_id, is_docker)
        self.output_keys = ["bammatcher_report"]

    def define_input(self):
        self.add_argument("bammatcher",     is_resource=True, is_required=True)
        self.add_argument("config",         is_resource=True, is_required=True)
        self.add_argument("vcf",            is_resource=True, is_required=True)
        self.add_argument("ref",            is_resource=True, is_required=True)
        self.add_argument("ref_idx",        is_resource=True, is_required=True)
        self.add_argument("ref_dict",       is_resource=True, is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("sample_id",      is_required=True)
        self.add_argument("dp-threshold",   default_value=15)
        self.add_argument("cache-dir",      default_value="/data")
        self.add_argument("nr_cpus",        default_value=2)
        self.add_argument("mem",            default_value=10)

    def define_output(self):
        sample_id = self.get_argument("sample_id")

        # Generate report extension
        ext = ".{0}.{1}.report".format(sample_id[0], sample_id[1])

        # Generate output filename
        output_filepath = self.generate_unique_file_name(extension=ext)

        self.add_output("bammatcher_report", output_filepath)

    def define_command(self):
        bammatcher             = self.get_argument("bammatcher")
        config                 = self.get_argument("config")
        vcf                    = self.get_argument("vcf")
        dp_threshold           = self.get_argument("dp-threshold")
        ref                    = self.get_argument("ref")
        cache_dir              = self.get_argument("cache-dir")
        bams                   = self.get_argument("bam")
        output_report          = self.get_output("bammatcher_report")

        # add module
        cmd = bammatcher

        # add required non-positional arguments
        cmd += " --bam1 {}".format(bams[0])
        cmd += " --bam2 {}".format(bams[1])

        # add additional arguments
        cmd += " --config {}".format(config)
        cmd += " --output {}".format(output_report)
        cmd += " --vcf {}".format(vcf)
        cmd += " --dp-threshold {}".format(dp_threshold)
        cmd += " --reference {}".format(ref)
        cmd += " --cache-dir {}".format(cache_dir)
        cmd += " !LOG2!"

        return cmd


class BammatcherReporter(Merger):
    def __init__(self, module_id, is_docker=False):
        super(BammatcherReporter, self).__init__(module_id, is_docker)
        self.output_keys = ["relatedness_report"]

    def define_input(self):
        self.add_argument("bammatcher_report",      is_required=True)
        self.add_argument("bammatcher_reporter",    is_resource=True, is_required=True)
        self.add_argument("nr_cpus",                default_value=0)
        self.add_argument("mem",                    default_value=0)

    def define_output(self):
        # Generate output filename
        output_filepath = self.generate_unique_file_name(extension=".csv")

        self.add_output("relatedness_report", output_filepath)

    def define_command(self):
        bammatcher_reporter     = self.get_argument("bammatcher_reporter")
        reports                 = self.get_argument("bammatcher_report")
        output_report           = self.get_output("relatedness_report")

        return "{0} {1} > {2} !LOG2!".format(bammatcher_reporter, " ".join(reports), output_report)
