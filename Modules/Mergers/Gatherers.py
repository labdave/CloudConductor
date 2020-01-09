from Modules import Merger

class _BaseGatherer(Merger):
    # Gatherers are basically pass-throughs that don't do anything to files but are conceptually mergers
    # Basically it means they can stop splitting and take multiple inputs and pass those inputs to subsequent modules
    TYPES_TO_GATHER = []
    def __init__(self, module_id, is_docker=False):
        super(_BaseGatherer, self).__init__(module_id, is_docker)
        self.output_keys = self.TYPES_TO_GATHER

    def define_input(self):
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)
        # Add required arguments
        for input_type in self.TYPES_TO_GATHER:
            self.add_argument(input_type)

    def define_output(self):
        # Output is literally just the same exact inputs
        args = self.get_arguments()
        for input_type in self.TYPES_TO_GATHER:
            self.add_output(input_type, args[input_type].get_value(), is_path=False)

    def define_command(self):
        return None


class GatherBams(_BaseGatherer):
    # Null module meant to re-collect a group of Bams after splitting
    TYPES_TO_GATHER = ["bam", "bam_idx"]
    def __init__(self, module_id, is_docker=False):
        super(GatherBams, self).__init__(module_id, is_docker)


class GatherVCFs(_BaseGatherer):
    # Null module meant to re-collect a group of VCFs after splitting
    TYPES_TO_GATHER = ["vcf", "vcf_idx", "vcf_gz", "vcf_tbi"]
    def __init__(self, module_id, is_docker=False):
        super(GatherVCFs, self).__init__(module_id, is_docker)


class GatherGVCFs(_BaseGatherer):
    # Null module meant to re-collect a group of gVCFs after splitting
    TYPES_TO_GATHER = ["gvcf", "gvcf_idx"]
    def __init__(self, module_id, is_docker=False):
        super(GatherGVCFs, self).__init__(module_id, is_docker)
