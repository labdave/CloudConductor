from Modules import Merger, PseudoMerger
from System.Platform import CloudPlatform


class _GATKBase(Merger):

    def __init__(self, module_id, is_docker=False):
        super(_GATKBase, self).__init__(module_id, is_docker)

    def define_base_args(self):

        # Set GATK executable arguments
        self.add_argument("java",           is_required=True, is_resource=True)
        self.add_argument("gatk",           is_required=True, is_resource=True)
        self.add_argument("gatk_version",   is_required=True)

        # Set reference specific arguments
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("ref_idx",        is_required=True, is_resource=True)
        self.add_argument("ref_dict",       is_required=True, is_resource=True)

        # Set chromosome interval specific arguments
        self.add_argument("location")
        self.add_argument("excluded_location")

    def get_gatk_command(self):
        # Get input arguments
        gatk        = self.get_argument("gatk")
        mem         = self.get_argument("mem")
        java        = self.get_argument("java")
        jvm_options = "-Xmx{0}G -Djava.io.tmpdir={1}".format(mem * 4//5, "/tmp/")

        # Determine numeric version of GATK
        gatk_version = self.get_argument("gatk_version")
        gatk_version = str(gatk_version).lower().replace("gatk","")
        gatk_version = gatk_version.strip()
        gatk_version = int(gatk_version.split(".")[0])

        if gatk_version < 4:
            return "{0} {1} -jar {2} -T".format(java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            return "{0} {1} -jar {2}".format(java, jvm_options, gatk)

    def get_output_file_flag(self):
        """
        Function returns an appropriate output file flag for GATK tools based on GATK version
        Returns: Output file flag in Str format

        """

        # Determine numeric version of GATK
        gatk_version = self.get_argument("gatk_version")
        gatk_version = str(gatk_version).lower().replace("gatk", "")
        gatk_version = gatk_version.strip()
        gatk_version = int(gatk_version.split(".")[0])

        if gatk_version < 4:
            return "-o"

        return "-O"

class GenotypeGVCFs(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(GenotypeGVCFs, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("gvcf",                is_required=True)
        self.add_argument("gvcf_idx",            is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=6)
        self.add_argument("mem",                is_required=True, default_value=35)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=".vcf.idx")
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Get input arguments
        gvcf_in     = self.get_argument("gvcf")
        ref         = self.get_argument("ref")
        L           = self.get_argument("location")
        XL          = self.get_argument("excluded_location")
        vcf         = self.get_output("vcf")
        gatk_cmd    = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the haplotype caller options
        opts = list()

        if isinstance(gvcf_in, list):
            for gvcf in gvcf_in:
                opts.append("--variant %s" % gvcf)
        else:
            opts.append("--variant %s" % gvcf_in)
        opts.append("{0} {1}".format(output_file_flag, vcf))
        opts.append("-R %s" % ref)

        # Limit the locations to be processes
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    if included != "unmapped":
                        opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for base recalibration
        return "touch {0}/*.idx; {1} GenotypeGVCFs {2} !LOG3!".format(self.get_output_dir(), gatk_cmd, " ".join(opts))

class MergeBQSRs(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(MergeBQSRs, self).__init__(module_id, is_docker)
        self.output_keys  = ["BQSR_report"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("BQSR_report",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Get the BQSR input
        bqsrs_in = self.get_argument("BQSR_report")

        # Check if the output is one file or multiple files
        if isinstance(bqsrs_in, list):

            # Declare merged bam output file
            bqsr_out = self.generate_unique_file_name(extension=".merged.grp")
            self.add_output("BQSR_report", bqsr_out)

        else:
            self.add_output("BQSR_report", bqsrs_in)

    def define_command(self):
        # Obtaining the arguments
        bqsrs_in    = self.get_argument("BQSR_report")
        bqsr_out    = self.get_output("BQSR_report")
        gatk_cmd    = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Making sure that the input is a list of files
        if isinstance(bqsrs_in, list):
            return "{0} GatherBQSRReports --input {1} {3} {2} !LOG3!".format(gatk_cmd, " --input ".join(bqsrs_in),
                                                                             bqsr_out, output_file_flag)
        else:
            return None

class CatVariants(_GATKBase):
    # Merger module intended to merge gVCF files within samples (i.e. re-combine chromosomes)

    def __init__(self, module_id, is_docker=False):
        super(CatVariants, self).__init__(module_id, is_docker)
        self.output_keys  = ["gvcf", "gvcf_idx", "vcf", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("gvcf",       is_required=False)
        self.add_argument("gvcf_idx",   is_required=False)
        self.add_argument("vcf",        is_required=False)
        self.add_argument("vcf_idx",    is_required=False)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=13)

    def define_output(self):
        # Declare GVCF output filename
        randomer = CloudPlatform.generate_unique_id()

        if self.get_argument("gvcf"):
            gvcf = self.generate_unique_file_name(extension="{0}.g.vcf".format(randomer))
            self.add_output("gvcf", gvcf)
            # Declare GVCF index output filename
            gvcf_idx = self.generate_unique_file_name(extension="{0}.g.vcf.idx".format(randomer))
            self.add_output("gvcf_idx", gvcf_idx)

        if self.get_argument("vcf"):
            vcf = self.generate_unique_file_name(extension="{0}.vcf".format(randomer))
            self.add_output("vcf", vcf)
            # Declare GVCF index output filename
            vcf_idx = self.generate_unique_file_name(extension="{0}.vcf.idx".format(randomer))
            self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Obtaining the arguments
        gvcf_in     = self.get_argument("gvcf")
        vcf_in      = self.get_argument("vcf")
        gatk        = self.get_argument("gatk")
        java        = self.get_argument("java")
        ref         = self.get_argument("ref")
        mem         = self.get_argument("mem")

        if not gvcf_in and not vcf_in:
            raise Exception("Please provide either GVCF or VCF.")

        if gvcf_in and vcf_in:
            raise Exception("GVCF and VCF both are provided. Please provide either of them.")

        # Generating JVM options
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=/tmp/" % (mem * 9 // 10)

        # Generating the CatVariants options
        opts = list()
        opts.append("-R %s" % ref)

        if gvcf_in:
            gvcf_out = self.get_output("gvcf")
            opts.append("-out {0}".format(gvcf_out))
            if isinstance(gvcf_in, list):
                for gvcf_input in gvcf_in:
                    opts.append("-V %s" % gvcf_input)
            else:
                opts.append("-V %s" % gvcf_in)

        if vcf_in:
            vcf_out = self.get_output("vcf")
            opts.append("-out {0}".format(vcf_out))
            if isinstance(vcf_in, list):
                for vcf_input in vcf_in:
                    opts.append("-V %s" % vcf_input)
            else:
                opts.append("-V %s" % vcf_in)

        # Generating the combine command
        cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants %s !LOG3!" % (java,
                                                                                    jvm_options,
                                                                                    gatk,
                                                                                    " ".join(opts))
        return cmd

class CombineGVCF(_GATKBase):
    # Merger module intended to merge gVCF files across multiple samples
    def __init__(self, module_id, is_docker=False):
        super(CombineGVCF, self).__init__(module_id, is_docker)
        self.output_keys  = ["gvcf", "gvcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("gvcf",               is_required=True)
        self.add_argument("gvcf_idx",           is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value=16)

    def define_output(self):
        # Declare GVCF output filename
        randomer = CloudPlatform.generate_unique_id()
        gvcf = self.generate_unique_file_name(extension="{0}.g.vcf".format(randomer))
        self.add_output("gvcf", gvcf)
        # Declare GVCF index output filename
        gvcf_idx = self.generate_unique_file_name(extension="{0}.g.vcf.idx".format(randomer))
        self.add_output("gvcf_idx", gvcf_idx)

    def define_command(self):

        # Obtaining the arguments
        gvcf_list    = self.get_argument("gvcf")
        ref          = self.get_argument("ref")
        L            = self.get_argument("location")
        XL           = self.get_argument("excluded_location")
        gvcf_out     = self.get_output("gvcf")
        gatk_version = self.get_argument("gatk_version")

        gatk_version = str(gatk_version).lower().replace("gatk", "")
        gatk_version = gatk_version.strip()
        gatk_version = int(gatk_version.split(".")[0])

        gatk_cmd = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the combine options
        opts = list()
        opts.append("{0} {1}".format(output_file_flag, gvcf_out))
        opts.append("-R %s" % ref)

        # Option that allows dictionary incompatibility
        # Unsafe options are not permitted in GATK4, so only add if using GATK3
        if gatk_version < 4:
            opts.append("-U ALLOW_SEQ_DICT_INCOMPATIBILITY")

        for gvcf_input in gvcf_list:
            opts.append("-V %s" % gvcf_input)

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    if included != "unmapped":
                        opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating the combine command
        return "{0} CombineGVCFs {1} !LOG3!".format(gatk_cmd, " ".join(opts))

class GenomicsDBImport(PseudoMerger):
    # Merger module intended to merge gVCF files across multiple samples
    def __init__(self, module_id, is_docker=False):
        super(GenomicsDBImport, self).__init__(module_id, is_docker)
        self.output_keys  = ["genomicsDB"]

    def define_input(self):
        self.add_argument("java",                   is_required=True, is_resource=True)
        self.add_argument("gatk",                   is_required=True, is_resource=True)
        self.add_argument("gvcf",                   is_required=False)
        self.add_argument("gvcf_idx",               is_required=False)
        self.add_argument("vcf_gz",                 is_required=False)
        self.add_argument("vcf_tbi",                is_required=False)
        self.add_argument("interval_list",          is_required=True)
        self.add_argument("batch_size",             is_required=True,   default_value=50)
        self.add_argument("interval_pad",           is_required=False,  default_value=None)
        self.add_argument("merge_input_intervals",  is_required=False,  default_value=False)
        self.add_argument("nr_cpus",                is_required=True,   default_value=5)
        self.add_argument("mem",                    is_required=True,   default_value=15)
        self.add_argument("location")
        self.add_argument("excluded_location")

    def define_output(self):
        # Declare merged GVCF output filename
        genomicsDB = self.generate_unique_file_name(extension="genomicsDB")
        self.add_output("genomicsDB", genomicsDB)

    def define_command(self):

        # Obtaining the arguments
        gatk                    = self.get_argument("gatk")
        mem                     = self.get_argument("mem")
        java                    = self.get_argument("java")
        gvcf_list               = self.get_argument("gvcf")
        gvcf_idx                = self.get_argument("gvcf_idx")
        vcf_list                = self.get_argument("vcf_gz")
        vcf_tbi                 = self.get_argument("vcf_tbi")
        nr_cpus                 = self.get_argument("nr_cpus")
        batch_size              = self.get_argument("batch_size")
        interval_pad            = self.get_argument("interval_pad")
        L                       = self.get_argument("location")
        interval_list           = self.get_argument("interval_list")
        merge_input_intervals   = self.get_argument("merge_input_intervals")
        genomicsDB              = self.get_output("genomicsDB")


        if not vcf_list and not gvcf_list:
            raise Exception("Neither of gVCF or VCF files are provided. Please provide only one of them.")

        if vcf_list and gvcf_list:
            raise Exception("A list of both gVCF and VCF are provided. You can only provide either of them based on "
                            "your need.")

        if (gvcf_list and not gvcf_idx) or (vcf_list and not vcf_tbi):
            raise Exception("VCF files require index files.")

        # Make JVM options and GATK command
        jvm_options = "-Xmx{0}G -Xms{0}G -Djava.io.tmpdir={1}".format(mem * 4 // 5, "/tmp/")
        gatk_cmd    = "{0} {1} -jar {2}".format(java, jvm_options, gatk)

        # Generating the combine options
        opts = list()

        # merge all the input intervals to avoid storage drive crash
        if merge_input_intervals:
            opts.append("--merge-input-intervals True")

        # Add input gvcfs
        if gvcf_list:
            if isinstance(gvcf_list, list):
                for gvcf_input in gvcf_list:
                    opts.append("-V {0}".format(gvcf_input))
            else:
                opts.append("-V {0}".format(gvcf_list))

        # Add input vcfs
        if vcf_list:
            if isinstance(vcf_list, list):
                for vcf_input in vcf_list:
                    opts.append("-V {0}".format(vcf_input))
            else:
                opts.append("-V {0}".format(vcf_list))

        # Add threads option
        opts.append("--reader-threads {0}".format(nr_cpus))
        opts.append("--batch-size {0}".format(batch_size))
        opts.append("--genomicsdb-workspace-path {0}".format(genomicsDB))

        # Add interval pad if necessary
        if interval_pad is not None:
            opts.append("-ip {0}".format(interval_pad))

        # Limit the locations to be processes
        if L is not None:
            if isinstance(L, list):
                for included in L:
                        opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)

        opts.append("-L {0}".format(interval_list))

        # Generate command to make genomicsDB directory and run job
        return "rm -rf {0} ; {1} GenomicsDBImport {2} !LOG3!".format(genomicsDB, gatk_cmd, " ".join(opts))

class CreateReadCountPanelOfNormals(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CreateReadCountPanelOfNormals, self).__init__(module_id, is_docker)
        self.output_keys = ["pon"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("read_count_out",                         is_required=True)
        self.add_argument("annotated_intervals",                    is_required=False, default_value=None)
        self.add_argument("do_impute_zeros",                        is_required=False, default_value=True)
        self.add_argument("extreme_outlier_truncation_percentile",  is_required=False, default_value=0.1)
        self.add_argument("extreme_sample_median_percentile",       is_required=False, default_value=2.5)
        self.add_argument("maximum_zeros_in_interval_percentage",   is_required=False, default_value=5.0)
        self.add_argument("maximum_zeros_in_sample_percentage",     is_required=False, default_value=5.0)
        self.add_argument("minimum_interval_median_percentile",     is_required=False, default_value=10.0)
        self.add_argument("number_of_eigensamples",                 is_required=False, default_value=20)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=2)

    def define_output(self):
        # Declare PoN output filename
        pon = self.generate_unique_file_name(extension=".pon.hdf5")
        self.add_output("pon", pon)

    def define_command(self):
        # Get input arguments
        read_count_out                          = self.get_argument("read_count_out")
        annotated_intervals                     = self.get_argument("annotated_intervals")
        do_impute_zeros                         = self.get_argument("do_impute_zeros")
        extreme_outlier_truncation_percentile   = self.get_argument("extreme_outlier_truncation_percentile")
        extreme_sample_median_percentile        = self.get_argument("extreme_sample_median_percentile")
        max_zeros_in_interval_percentage        = self.get_argument("maximum_zeros_in_interval_percentage")
        max_zeros_in_sample_percentage          = self.get_argument("maximum_zeros_in_sample_percentage")
        min_interval_median_percentile          = self.get_argument("minimum_interval_median_percentile")
        eigensamples                            = self.get_argument("number_of_eigensamples")

        # Get output arguments
        pon = self.get_output("pon")

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Generate the command line for CreateReadCountPanelOfNormals
        cmd = "{0} CreateReadCountPanelOfNormals".format(gatk_cmd)

        # If the read count is a list create a list of input files
        if isinstance(read_count_out, list):
            cmd = "{0} -I {1}".format(cmd, ' -I '.join(read_count_out))
        else:
            cmd = "{0} -I {1}".format(cmd, read_count_out)

        # If annotated interval file is provided, use it in the cmd line
        if annotated_intervals is not None:
            cmd = "{0} --annotated-intervals {1}".format(cmd, annotated_intervals)

        # If impute zero-coverage false, set the flag to false
        if not do_impute_zeros:
            cmd = "{0} --do-impute-zeros {1}".format(cmd, do_impute_zeros)

        cmd = "{0} --extreme-outlier-truncation-percentile {1} --extreme-sample-median-percentile {2} " \
              "--maximum-zeros-in-interval-percentage {3} --maximum-zeros-in-sample-percentage {4} " \
              "--minimum-interval-median-percentile {5} --number-of-eigensamples {6} {7} {8}".\
            format(cmd, extreme_outlier_truncation_percentile, extreme_sample_median_percentile,
                   max_zeros_in_interval_percentage, max_zeros_in_sample_percentage, min_interval_median_percentile,
                   eigensamples, output_file_flag, pon)

        return "{0} !LOG3!".format(cmd)