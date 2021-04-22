from Modules import Module


class BamToFastq(Module):
    def __init__(self, module_id, is_docker = False):
        super(BamToFastq, self).__init__(module_id, is_docker)
        self.output_keys 	= ["R1", "R2"]

    def define_input(self):
        self.add_argument("bam",		is_required=True)
        self.add_argument("mem",        default_value=5)
        self.add_argument("nr_cpus",    default_value=1)

    def define_output(self):
        r1 					= self.generate_unique_file_name(".r1.fq")
        r2 					= self.generate_unique_file_name(".r2.fq")
        self.add_output("R1", 			r1)
        self.add_output("R2",			r2)

    def define_command(self):
        # Define command for running bedtools coverage from a platform
        bam 				= self.get_argument("bam")
        sorted_bam          = str(bam).replace(".bam", ".sorted.bam")

        r1 					= self.get_output("R1")
        r2 					= self.get_output("R2")

        cmd = "samtools index {0} !LOG3!;".format(bam)
        cmd += "samtools sort -n {0} -o {1} !LOG3!;".format(bam, sorted_bam)
        cmd += "bedtools bamtofastq -i {0} -fq {1} -fq2 {2} !LOG3!".format(sorted_bam, r1, r2)

        return cmd

class Coverage(Module):
    def __init__(self, module_id, is_docker = False):
        super(Coverage, self).__init__(module_id, is_docker)
        self.output_keys = ["coverage_report", "read_counts"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref_idx",        is_required=True, is_resource=True)
        self.add_argument("bed",            is_required=True, is_resource=True)
        self.add_argument("bedtools",       is_required=True, is_resource=True)
        self.add_argument("count_reads",    is_required=False)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=8)

    def define_output(self):

        if self.get_argument("count_reads"):
            # Declare reads count output filename
            read_counts = self.generate_unique_file_name(".read.counts.txt")
            self.add_output("read_counts", read_counts)
        else:
            # Declare coverage report output filename
            coverage_report = self.generate_unique_file_name(".coverage.txt")
            self.add_output("coverage_report", coverage_report)

    def define_command(self):
        # Define command for running bedtools coverage from a platform
        bam         = self.get_argument("bam")
        ref_idx     = self.get_argument("ref_idx")
        bed         = self.get_argument("bed")
        bedtools    = self.get_argument("bedtools")
        count_reads = self.get_argument("count_reads")

        if count_reads:
            # get the output file name to store coverage stats
            read_counts = self.get_output("read_counts")

            # Generating coverage command for read counts
            cmd = "{0} coverage -counts -a {1} -b {2} -sorted -g {4} > {3} !LOG2!".format(bedtools, bed, bam,
                    read_counts, ref_idx)

            return cmd


        # get the output file name to store coverage stats
        coverage_report = self.get_output("coverage_report")

        # Generating coverage command
        cmd = "{0} coverage -a {1} -b {2} -sorted -g {4} > {3} !LOG2!".format(bedtools, bed, bam, coverage_report,
                ref_idx)

        return cmd
