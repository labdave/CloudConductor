from Modules import Module

class Fastp(Module):
    def __init__(self, module_id, is_docker=False):
        super(Fastp, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2", "unpaired_r1", "unpaired_r2", "failed_reads", "json_report",
                            "html_report", "trim_report"]

    def define_input(self):
        self.add_argument("sample_name",                    is_required=True)
        self.add_argument("R1",                             is_required=True)
        self.add_argument("R2",                             is_required=False)
        self.add_argument("fastp",                          is_required=True, is_resource=True)
        self.add_argument("adapters",                       is_required=True, is_resource=True)
        self.add_argument("phred64",                        is_required=False)
        self.add_argument("min_phred_score",                is_required=True, default_value=30)
        self.add_argument("min_read_length",                is_required=True, default_value=36)
        self.add_argument("cut_window_size",                is_required=True, default_value=5)
        self.add_argument("cut_min_qual",                   is_required=True, default_value=10)
        self.add_argument("cut_front",                      is_required=True, default_value=True)
        self.add_argument("cut_tail",                       is_required=True, default_value=True)
        self.add_argument("cut_min_qual",                   is_required=True, default_value=10)
        self.add_argument("base_correction",                is_required=False)
        self.add_argument("over_representation_analysis",   is_required=False)
        self.add_argument("trim_ploy_g",                    is_required=False, default_value=True)
        self.add_argument("trim_ploy_x",                    is_required=False, default_value=True)
        self.add_argument("report_title",                   is_required=True, default_value="FASTP report")
        self.add_argument("nr_cpus",                        is_required=True, default_value=2)
        self.add_argument("thread",                         is_required=True, default_value=2)
        self.add_argument("mem",                            is_required=True, default_value=5)

    def define_output(self):

        # Declare R1 fastqc output filename
        r1 = self.get_argument("R1")
        r1_out = r1.replace(".fastq.gz",".trimmed.fastq.gz")
        unpaired_r1 = r1.replace(".fastq.gz", ".unpaired.fastq.gz")

        self.add_output("R1", r1_out)
        self.add_output("unpaired_r1", unpaired_r1)

        # Conditionally declare R2 fastqc output filename
        r2 = self.get_argument("R2")
        if r2 is not None:
            r2_out = r2.replace(".fastq.gz", ".trimmed.fastq.gz")
            unpaired_r2 = r2.replace(".fastq.gz", ".unpaired.fastq.gz")

            self.add_output("R2", r2_out)
            self.add_output("unpaired_r2", unpaired_r2)

        else:

            self.add_output("R2", None, is_path=False)
            self.add_output("unpaired_r2", None, is_path=False)

        failed_reads = self.generate_unique_file_name(extension=".failed.reads.fastq.gz")
        json_report = self.generate_unique_file_name(extension=".fastp.report.json")
        html_report = self.generate_unique_file_name(extension=".fastp.report.html")
        trim_report = self.generate_unique_file_name(extension=".trim_report.txt")

        self.add_output("failed_reads", failed_reads)
        self.add_output("json_report", json_report)
        self.add_output("html_report", html_report)
        self.add_output("trim_report", trim_report)

    def define_command(self):
        # ge tthe arguments to run Fastp
        sample_name                     = self.get_argument("sample_name")
        r1                              = self.get_argument("R1")
        r2                              = self.get_argument("R2")
        fastp                           = self.get_argument("fastp")
        adapters                        = self.get_argument("adapters")
        phred64                         = self.get_argument("phred64")
        min_phred_score                 = self.get_argument("min_phred_score")
        min_read_length                 = self.get_argument("min_read_length")
        cut_window_size                 = self.get_argument("cut_window_size")
        cut_min_qual                    = self.get_argument("cut_min_qual")
        cut_front                       = self.get_argument("cut_front")
        cut_tail                        = self.get_argument("cut_tail")
        base_correction                 = self.get_argument("base_correction")
        over_representation_analysis    = self.get_argument("over_representation_analysis")
        trim_ploy_g                     = self.get_argument("trim_ploy_g")
        trim_ploy_x                     = self.get_argument("trim_ploy_x")
        report_title                    = self.get_argument("report_title")
        thread                          = self.get_argument("thread")

        # add sample name in the given report title
        report_title = "{0} for {1}".format(report_title, sample_name)

        # get the ouptut file names
        r1_out          = self.get_output("R1")
        unpaired1       = self.get_output("unpaired_r1")
        failed_reads    = self.get_output("failed_reads")
        json_report     = self.get_output("json_report")
        html_report     = self.get_output("html_report")
        trim_report     = self.get_output("trim_report")

        # generate initial command for Fastp
        cmd = "{0} -i {1} -o {2} --unpaired1 {3} --failed_out {4} --thread {5} --adapter_fasta {6}".format(fastp, r1,
                                                                                                           r1_out,
                                                                                                           unpaired1,
                                                                                                           failed_reads,
                                                                                                           thread,
                                                                                                           adapters)

        # Optionally analyze R2 if available
        if r2 is not None:

            r2_out      = self.get_output("R2")
            unpaired2   = self.get_output("unpaired_r2")

            # Run Fastqc on R1 and R2
            cmd = "{0} -I {1} -O {2} --unpaired2 {3}".format(cmd, r2, r2_out, unpaired2)

        # add parameters for read quality and min read length
        cmd = "{0} -q {1} -l {2}".format(cmd, min_phred_score, min_read_length)

        # add window size parameters
        cmd = "{0} --cut_window_size {1} --cut_mean_quality {2}".format(cmd, cut_window_size, cut_min_qual)

        # add trimming parameters
        cmd = "{0} --trim_poly_g --trim_poly_x --cut_front --cut_tail".format(cmd, trim_ploy_g, trim_ploy_x,
                                                                              cut_front, cut_tail)

        # add over representation analysis
        if over_representation_analysis:
            cmd = "{0} --overrepresentation_analysis".format(cmd)

        # add base correction analysis
        if base_correction:
            cmd = "{0} --correction".format(cmd)

        # if the scoring system is phread64
        if phred64:
            cmd = "{0} --phred64".format(cmd)

        # add report file parameters
        cmd = "{0} -j {1} -h {2}".format(cmd, json_report, html_report)

        # add other required parameters
        cmd = "{0} -R \"{1}\" --thread {2} > {3} 2>&1".format(cmd, report_title, thread, trim_report)

        # finalize the command
        if not self.is_docker:
            cmd = "sudo {0}".format(cmd)
        else:
            cmd = "{0}".format(cmd)

        return cmd
