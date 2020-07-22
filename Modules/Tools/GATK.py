import logging
import os

from Modules import Module
from System.Platform import Platform


class _GATKBase(Module):

    def __init__(self, module_id, is_docker=False):
        super(_GATKBase, self).__init__(module_id, is_docker)

        # Initialize the gatk version
        self.gatk_version = None

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
        self.add_argument("interval_list")
        self.add_argument("bed", is_resource=True)

    def get_gatk_version(self):

        # Generate the gatk version if it was not generated yet
        if self.gatk_version is None:

            gatk_version = self.get_argument("gatk_version")
            gatk_version = str(gatk_version).lower().replace("gatk", "")
            gatk_version = gatk_version.strip()
            self.gatk_version = int(gatk_version.split(".")[0])

        return self.gatk_version

    def get_gatk_command(self):
        # Get input arguments
        gatk    = self.get_argument("gatk")
        mem     = self.get_argument("mem")
        java = self.get_argument("java")
        jvm_options = "-Xmx{0}G -Djava.io.tmpdir={1}".format(mem * 4 // 5, "/tmp/")

        if self.get_gatk_version() < 4:
            return "{0} {1} -jar {2} -T".format(java, jvm_options, gatk)

        # Generate base command with endpoint provided by docker
        else:
            return "{0} {1} -jar {2}".format(java, jvm_options, gatk)

    def get_output_file_flag(self):
        """
        Function returns an appropriate output file flag for GATK tools based on GATK version
        Returns: Output file flag in Str format

        """

        if self.get_gatk_version() < 4:
            return "-o"

        return "-O"


class HaplotypeCaller(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(HaplotypeCaller, self).__init__(module_id, is_docker)
        self.output_keys = ["gvcf_gz", "gvcf_idx", "vcf_gz", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                    is_required=True)
        self.add_argument("bam_idx",                is_required=True)
        self.add_argument("BQSR_report",            is_required=False)
        self.add_argument("use_bqsr",               is_required=True, default_value=True)
        self.add_argument("output_type",            is_required=True, default_value="gvcf")
        self.add_argument("nr_cpus",                is_required=True, default_value=8)
        self.add_argument("mem",                    is_required=True, default_value=48)
        self.add_argument("use_soft_clipped_bases", is_required=True, default_value=True)

    def define_output(self):
        # Declare GVCF output filename
        randomer = Platform.generate_unique_id()
        # generate uniques file name based on the output mode set for the Haplotypecaller
        if self.get_argument("output_type") == "gvcf":
            gvcf = self.generate_unique_file_name(extension="{0}.g.vcf.gz".format(randomer))
            self.add_output("gvcf_gz", gvcf)
            # Declare GVCF index output filename
            gvcf_idx = self.generate_unique_file_name(extension="{0}.g.vcf.gz.tbi".format(randomer))
            self.add_output("gvcf_idx", gvcf_idx)
        else:
            vcf = self.generate_unique_file_name(extension="{0}.vcf.gz".format(randomer))
            self.add_output("vcf_gz", vcf)
            # Declare VCF index output filename
            vcf_idx = self.generate_unique_file_name(extension="{0}.vcf.gz.tbi".format(randomer))
            self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Get input arguments
        bam                    = self.get_argument("bam")
        BQSR                   = self.get_argument("BQSR_report")
        output_type            = self.get_argument("output_type")
        ref                    = self.get_argument("ref")
        L                      = self.get_argument("location")
        XL                     = self.get_argument("excluded_location")
        interval               = self.get_argument("interval_list")
        bed                    = self.get_argument("bed")

        gatk_cmd               = self.get_gatk_command()
        use_bqsr               = self.get_argument("use_bqsr")
        use_soft_clipped_bases = self.get_argument("use_soft_clipped_bases")
        nr_cpus                = self.get_argument("nr_cpus")

        output_file_flag = self.get_output_file_flag()

        # Generating the haplotype caller options
        opts = list()
        opts.append("-I {0}".format(bam))
        opts.append("-R {0}".format(ref))

        # Setting the output file based on the output mode
        if output_type == "gvcf":
            opts.append("{0} {1}".format(output_file_flag, self.get_output("gvcf_gz")))
        else:
            opts.append("{0} {1}".format(output_file_flag, self.get_output("vcf_gz")))

        # Setting the output mode
        if output_type == "gvcf":
            opts.append("-ERC GVCF")

        # Adding the BQSR for lower version of the GATK
        if BQSR is not None and self.get_gatk_version() < 4 and use_bqsr:
            opts.append("-BQSR {0}".format(BQSR))

        # Set the parallelism method
        if self.get_gatk_version() < 4:
            opts.append("-nct {0}".format(nr_cpus))
        else:
            opts.append("--native-pair-hmm-threads {0}".format(nr_cpus))

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    if included != "unmapped":
                        opts.append("-L \"{0}\"".format(included))
            else:
                opts.append("-L \"{0}\"".format(L))
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"{0}\"".format(excluded))
            else:
                opts.append("-XL \"{0}\"".format(XL))

        # Check if an interval list was provided and if yes, place it
        if interval is not None:
            opts.append("-L {0}".format(interval))

        # Check if a BED file was provided and if yes, place it
        if bed is not None:
            opts.append("-L {0}".format(bed))

        # Add on flag for not using soft clipped bases if defined (used in RNAseq variant calling)
        if use_soft_clipped_bases == "False":
            opts.append("--dont-use-soft-clipped-bases")

        # Generating command for HaplotypeCaller
        return "{0} HaplotypeCaller {1} !LOG3!".format(gatk_cmd, " ".join(opts))


class PrintReads(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(PrintReads, self).__init__(module_id, is_docker)
        self.output_keys            = ["bam"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("BQSR_report",        is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2.5")

    def define_output(self):
        # Declare bam output filename
        bam = self.generate_unique_file_name(extension=".recalibrated.bam")
        self.add_output("bam", bam)

    def define_command(self):
        # Obtaining the arguments
        bam     = self.get_argument("bam")
        BQSR    = self.get_argument("BQSR_report")
        ref     = self.get_argument("ref")
        L       = self.get_argument("location")
        XL      = self.get_argument("excluded_location")
        nr_cpus = self.get_argument("nr_cpus")
        output_bam = self.get_output("bam")
        gatk_cmd = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the PrintReads caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("{0} {1}".format(output_file_flag, output_bam))
        opts.append("-nct %d" % nr_cpus)
        opts.append("-R %s" % ref)
        opts.append("-BQSR %s" % BQSR)

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for GATK PrintReads
        return "%s PrintReads %s !LOG3!" % (gatk_cmd, " ".join(opts))


class ApplyBQSR(_GATKBase):
    # GATK 4 replacement for PrintReads
    def __init__(self, module_id, is_docker=False):
        super(ApplyBQSR, self).__init__(module_id, is_docker)
        self.output_keys            = ["bam", "bam_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("BQSR_report",        is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2.5")

    def define_output(self):
        # Declare bam output filename
        bam = self.generate_unique_file_name(extension=".recalibrated.bam")
        bam_idx = "{0}.bai".format(bam)
        self.add_output("bam", bam)
        self.add_output("bam_idx", bam_idx)

    def define_command(self):
        # Obtaining the arguments
        bam     = self.get_argument("bam")
        BQSR    = self.get_argument("BQSR_report")
        ref     = self.get_argument("ref")
        L       = self.get_argument("location")
        XL      = self.get_argument("excluded_location")
        output_bam      = self.get_output("bam")
        output_bam_idx  = self.get_output("bam_idx")
        tmp_bam_idx     = str(output_bam_idx).replace(".bam.bai", ".bai")
        gatk_cmd        = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the ApplyBQSR caller options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("{0} {1}".format(output_file_flag, output_bam))
        opts.append("-R %s" % ref)
        opts.append("--bqsr-recal-file %s" % BQSR)

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)
        if XL is not None:
            if isinstance(XL, list):
                for excluded in XL:
                    opts.append("-XL \"%s\"" % excluded)
            else:
                opts.append("-XL \"%s\"" % XL)

        # Generating command for GATK PrintReads
        bqsr_cmd = "%s ApplyBQSR %s !LOG3!" % (gatk_cmd, " ".join(opts))
        mv_cmd = "mv %s %s !LOG2!" % (tmp_bam_idx, output_bam_idx)
        return "%s ; %s" % (bqsr_cmd, mv_cmd)


class BaseRecalibrator(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(BaseRecalibrator, self).__init__(module_id, is_docker)
        self.output_keys    = ["BQSR_report"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("dbsnp",              is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=4)
        self.add_argument("mem",                is_required=True, default_value=12)

    def define_output(self):
        # Declare BQSR report file
        bqsr_report = self.generate_unique_file_name(extension=".grp")
        self.add_output("BQSR_report", bqsr_report)

    def define_command(self):
        # Get arguments needed to generate GATK BQSR command
        bam             = self.get_argument("bam")
        ref             = self.get_argument("ref")
        dbsnp           = self.get_argument("dbsnp")
        L               = self.get_argument("location")
        XL              = self.get_argument("excluded_location")
        nr_cpus         = self.get_argument("nr_cpus")
        bqsr_report     = self.get_output("BQSR_report")

        gatk_cmd        = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the base recalibration options
        opts = list()
        opts.append("-I %s" % bam)
        opts.append("{0} {1}".format(output_file_flag, bqsr_report))
        opts.append("-R %s" % ref)

        if self.get_gatk_version() >= 4:
            opts.append("--known-sites %s" % dbsnp)
        else:
            opts.append("-knownSites %s" % dbsnp)
            opts.append("-nct %d" % nr_cpus)
            opts.append("-cov ReadGroupCovariate")
            opts.append("-cov QualityScoreCovariate")
            opts.append("-cov CycleCovariate")
            opts.append("-cov ContextCovariate")

        # Limit the locations to be processed
        if L is not None:
            if isinstance(L, list):
                for included in L:
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
        return "{0} BaseRecalibrator {1} !LOG3!".format(gatk_cmd, " ".join(opts))


class IndexVCF(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(IndexVCF, self).__init__(module_id, is_docker)
        self.output_keys  = ["vcf", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("vcf",               is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=13)

    def define_output(self):
        # Declare merged GVCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)
        # Declare GVCF index output filename
        vcf_idx = vcf + ".idx"
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Obtaining the arguments
        vcf_in  = self.get_argument("vcf")
        gatk    = self.get_argument("gatk")
        ref     = self.get_argument("ref")
        mem     = self.get_argument("mem")
        vcf_out = self.get_output("vcf")

        # Generate command with java if not running on docker
        java = self.get_argument("java")
        jvm_options = "-Xmx%dG -Djava.io.tmpdir=%s" % (mem * 4 // 5, "/tmp/")
        cmd = "%s %s -cp %s org.broadinstitute.gatk.tools.CatVariants" % (java, jvm_options, gatk)

        # Generating the CatVariants options
        opts = list()
        opts.append("-out {0}".format(vcf_out))
        opts.append("-R %s" % ref)
        opts.append("-V %s" % vcf_in)

        # Generating the IndexVCF cmd
        return "%s %s !LOG3!" % (cmd, " ".join(opts))


class FilterMutectCalls(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(FilterMutectCalls, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_gz", "vcf_tbi"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("vcf_gz",         is_required=True)
        self.add_argument("vcf_tbi",        is_required=True)
        self.add_argument("stats_table",    is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=2)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf.gz")
        self.add_output("vcf_gz", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=".vcf.gz.tbi")
        self.add_output("vcf_tbi", vcf_idx)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf_gz")
        gatk_cmd    = self.get_gatk_command()
        vcf_out     = self.get_output("vcf_gz")
        ref         = self.get_argument("ref")
        stats_table = self.get_argument("stats_table")

        output_file_flag = self.get_output_file_flag()

        return "{0} FilterMutectCalls -V {1} -R {4} --stats {5} {3} {2} !LOG3!".format(gatk_cmd, vcf_in, vcf_out,
                                                                                       output_file_flag, ref,
                                                                                       stats_table)


class CollectReadCounts(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CollectReadCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["read_count_out"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        read_count_out = self.generate_unique_file_name(extension=".read_count.txt")
        self.add_output("read_count_out", read_count_out)

    def define_command(self):
        # Get input arguments
        bam             = self.get_argument("bam")
        gatk_cmd        = self.get_gatk_command()
        read_count_out  = self.get_output("read_count_out")
        interval_list   = self.get_argument("interval_list")

        output_file_flag = self.get_output_file_flag()

        cmd = "{0} CollectReadCounts -I {1} {3} {2} --format TSV -DF MappingQualityReadFilter ".format(gatk_cmd, bam, read_count_out, output_file_flag)

        if interval_list is not None:
            cmd = "{0} -L {1} --interval-merging-rule OVERLAPPING_ONLY".format(cmd, interval_list)

        return "{0} !LOG3!".format(cmd)


class BedToIntervalList(_GATKBase):
    def __init__(self, module_id, is_docker=False):
        super(BedToIntervalList, self).__init__(module_id, is_docker)
        self.output_keys = ["interval_list"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bed",        is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        interval_list = self.generate_unique_file_name(extension=".interval.list")
        self.add_output("interval_list", interval_list)

    def define_command(self):

        # Get input arguments
        bed             = self.get_argument("bed")
        dict_file       = self.get_argument("ref_dict")
        gatk_cmd        = self.get_gatk_command()
        interval_list   = self.get_output("interval_list")

        output_file_flag = self.get_output_file_flag()

        return "{0} BedToIntervalList -I {1} {4} {2} -SD {3} !LOG3!".format(gatk_cmd, bed, interval_list, dict_file,
                                                                            output_file_flag)


class GenotypeGenomicsDB(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(GenotypeGenomicsDB, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("genomicsDB", is_required=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=4)
        self.add_argument("mem",        is_required=True, default_value=16)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf")
        self.add_output("vcf", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=".vcf.idx")
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Get input arguments
        genomics_db = self.get_argument("genomicsDB")
        ref         = self.get_argument("ref")
        L           = self.get_argument("location")
        vcf         = self.get_output("vcf")

        # Make JVM options and GATK command
        gatk_cmd = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the haplotype caller options
        opts = list()

        if isinstance(genomics_db, list):
            for gdb in genomics_db:
                opts.append("-V gendb://{0}".format(gdb))
        else:
            opts.append("-V gendb://{0}".format(genomics_db))

        opts.append("{1} {0}".format(vcf, output_file_flag))
        opts.append("-R {0}".format(ref))

        # Limit the locations to be processes
        if L is not None:
            if isinstance(L, list):
                for included in L:
                    opts.append("-L \"%s\"" % included)
            else:
                opts.append("-L \"%s\"" % L)

            # Add option to restrict vcf to intervals
            opts.append("--only-output-calls-starting-in-intervals")

        # Generating command for base recalibration
        return "{0} GenotypeGVCFs {1} !LOG3!".format(gatk_cmd, " ".join(opts))


class SplitNCigarReads(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(SplitNCigarReads, self).__init__(module_id, is_docker)
        self.output_keys  = ["bam"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2.5")

    def define_output(self):
        # Declare BAM output filename
        bam = self.generate_unique_file_name(extension=".split.bam")
        self.add_output("bam", bam)

    def define_command(self):
        # Get arguments needed to generated a SplitNCigarReads command
        bam        = self.get_argument("bam")
        output_bam = self.get_output("bam")
        ref        = self.get_argument("ref")

        # Get JVM options and GATK command
        gatk_cmd = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Generating the options for splitting reads with N in cigar string
        opts = list()
        opts.append("-R {0}".format(ref))
        opts.append("-I {0}".format(bam))
        opts.append("{0} {1}".format(output_file_flag, output_bam))

        if self.get_gatk_version() < 4:
            opts.append("-rf ReassignOneMappingQuality")
            opts.append("-RMQF 255")
            opts.append("-RMQT 60")
            opts.append("-U ALLOW_N_CIGAR_READS")

        # Generate command for splitting reads
        return "{0} SplitNCigarReads {1} !LOG3!".format(gatk_cmd, " ".join(opts))


class Mutect2(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(Mutect2, self).__init__(module_id, is_docker)
        self.output_keys = ["vcf_gz", "vcf_tbi", "stats_table"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("is_tumor",           is_required=True)
        self.add_argument("pon_vcf_gz",         is_required=False)
        self.add_argument("pon_vcf_tbi",        is_required=False)
        self.add_argument("max_mnp_distance",   is_required=False)
        self.add_argument("germline_vcf",       is_required=False,  is_resource=True)
        self.add_argument("nr_cpus",            is_required=True,   default_value=8)
        self.add_argument("mem",                is_required=True,   default_value=30)

    def define_output(self):
        # Declare VCF output filename
        vcf = self.generate_unique_file_name(extension=".vcf.gz")
        self.add_output("vcf_gz", vcf)
        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=".vcf.gz.tbi")
        self.add_output("vcf_tbi", vcf_idx)
        # Declare stats table output filename
        stats_table = self.generate_unique_file_name(extension=".vcf.gz.stats")
        self.add_output("stats_table", stats_table)

    def define_command(self):
        # Get input arguments
        bams                = self.get_argument("bam")
        ref                 = self.get_argument("ref")
        germline_vcf        = self.get_argument("germline_vcf")
        L                   = self.get_argument("location")
        XL                  = self.get_argument("excluded_location")
        nr_cpus             = self.get_argument("nr_cpus")
        interval            = self.get_argument("interval_list")
        bed                 = self.get_argument("bed")
        pon_vcf_gz          = self.get_argument("pon_vcf_gz")
        max_mnp_distance    = self.get_argument("max_mnp_distance")

        vcf = self.get_output("vcf_gz")

        gatk_cmd        = self.get_gatk_command()

        output_file_flag = self.get_output_file_flag()

        # Get sample names and tumor status
        sample_names, is_tumor = self.__process_samples()

        # Generating the Mutect2 options
        opts = list()

        # Add Tumor/Normal sample names
        for _sample_name, _is_tumor in zip(sample_names, is_tumor):
            if not _is_tumor and pon_vcf_gz:
                opts.append("-normal {0}".format(_sample_name))

        def flatten(lis):
            """Given a list, possibly nested to any level, return it flattened."""
            new_lis = []
            for item in lis:
                if type(item) == type([]):
                    new_lis.extend(flatten(item))
                else:
                    new_lis.append(item)
            return new_lis

        # Add arguments for bams
        if isinstance(bams, list):
            bams = flatten(bams)
            opts.extend(["-I %s" % bam for bam in bams])
        else:
            opts.append("-I {0}".format(bams))

        opts.append("{0} {1}".format(output_file_flag, vcf))
        opts.append("-R %s" % ref)
        opts.append("--native-pair-hmm-threads %s" % nr_cpus)

        if germline_vcf is not None:
            opts.append("--germline-resource %s" % germline_vcf)

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

        # Check if an interval list and bed was provided and if yes, use interval list
        if interval is not None and bed is not None:
            logging.warning("Interval list gets the higher precedence over BED file.")
            opts.append("-L {0}".format(interval))

        # Check if an interval list was provided and if yes, place it
        elif interval is not None:
            opts.append("-L {0}".format(interval))

        # Check if a BED file was provided and if yes, place it
        elif bed is not None:
            opts.append("-L {0}".format(bed))

        # "Note that as of May, 2019 -max-mnp-distance must be set to zero to avoid a bug in GenomicsDBImport."
        if max_mnp_distance:
            opts.append("-max-mnp-distance {0}".format(max_mnp_distance))

        if pon_vcf_gz:
            opts.append("-pon {0}".format(pon_vcf_gz))

        # Generating command for Mutect2
        return "{0} Mutect2 {1} !LOG3!".format(gatk_cmd, " ".join(opts))

    def __process_samples(self):

        # Generate a dictionary that will contain the samples and their tumor status
        tumor_status = {}

        # Get samples and tumor status
        sample_names = self.get_argument("sample_name")
        is_tumor = self.get_argument("is_tumor")

        # Make sure samples and tumor status are lists
        sample_names = sample_names if isinstance(sample_names, list) else [sample_names]
        is_tumor = is_tumor if isinstance(is_tumor, list) else [is_tumor]

        # Add each sample to the tumor status dictionary
        for _name, _tumor in zip(sample_names, is_tumor):

            # Ensure tumor status is not a list but one single unique value
            if isinstance(_tumor, list):
                _tumor = set(_tumor)

                if len(_tumor) != 1:
                    raise Exception("More one tumor status {0} for a sample".format(_tumor))
                else:
                    _tumor = _tumor.pop()

            # Ensure sample name is not a list but one single unique value
            if isinstance(_name, list):
                _name = set(_name)

                if len(_name) != 1:
                    raise Exception("More one tumor status {0} for a sample".format(_name))
                else:
                    _name = _name.pop()

            # Check if the current sample name has already been introduced but with a different tumor status
            if _name in tumor_status and tumor_status[_name] != _tumor:
                logging.error("Same sample ID '%s' was provided as different tumor status!" % _name)
                raise RuntimeError("Same sample ID '%s' was provided as different tumor status!" % _name)

            # If we have not stopped, just added it (possibly again) in the dictionary
            tumor_status[_name] = _tumor

        return list(tumor_status.keys()), list(tumor_status.values())


class DepthOfCoverage(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(DepthOfCoverage, self).__init__(module_id, is_docker)
        self.prefix         = None
        self.output_keys    = ["per_base_summary", "interval_summary", "interval_statistics", "sample_summary",
                               "sample_statistics", "cumulative_coverage_counts", "cumulative_coverage_proportions"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("ref",                is_required=True, is_resource=True)
        self.add_argument("interval_list",      is_required=False)
        self.add_argument("gene_list",          is_required=False, default_value=None)
        self.add_argument("read_group",         is_required=False, default_value=None)
        self.add_argument("unsafe",             is_required=False, default_value='ALLOW_N_CIGAR_READS')
        self.add_argument("count_type",         is_required=False, default_value='COUNT_FRAGMENTS')
        self.add_argument("per_base_summary",   is_required=False, default_value=None)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self):
        # Declare unique file name for a single output file
        per_base_summary = self.generate_unique_file_name(extension=".per_base_summary.txt")

        # Split the genereated unique output file name to get prefix to use to generate other output filenames
        self.prefix = per_base_summary.split(".per_base_summary.txt")[0]

        # Generate rest of the output file names
        interval_summary                = "{0}.sample_interval_summary".format(self.prefix)
        interval_statistics             = "{0}.sample_interval_statistics".format(self.prefix)
        sample_summary                  = "{0}.sample_summary".format(self.prefix)
        sample_statistics               = "{0}.sample_statistics".format(self.prefix)
        cumulative_coverage_counts      = "{0}.sample_cumulative_coverage_counts".format(self.prefix)
        cumulative_coverage_proportions = "{0}.sample_cumulative_coverage_proportions".format(self.prefix)

        if self.get_argument("per_base_summary"):
            self.add_output("per_base_summary", self.prefix)

        self.add_output("interval_summary", interval_summary)
        self.add_output("interval_statistics", interval_statistics)
        self.add_output("sample_summary", sample_summary)
        self.add_output("sample_statistics", sample_statistics)
        self.add_output("cumulative_coverage_counts", cumulative_coverage_counts)
        self.add_output("cumulative_coverage_proportions", cumulative_coverage_proportions)

    def define_command(self):
        # Get input arguments
        bam                 = self.get_argument("bam")
        ref                 = self.get_argument("ref")
        interval_list       = self.get_argument("interval_list")
        gene_list           = self.get_argument("gene_list")
        read_group          = self.get_argument("read_group")
        unsafe              = self.get_argument("unsafe")
        count_type          = self.get_argument("count_type")
        per_base_summary    = self.get_argument("per_base_summary")

        # Get base GATK command line
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DepthOfCoverage
        cmd = "{0} DepthOfCoverage -I {1} -R {2} --countType {5} -ct 1 -ct 10 -ct 25 -ct 50 -ct 75 -ct 100 -ct 150 " \
              "-ct 200 -ct 250 -ct 500 {3} {4}".format(gatk_cmd, bam, ref, output_file_flag, self.prefix, count_type)

        if not per_base_summary:
            cmd = "{0} --omitDepthOutputAtEachBase".format(cmd)

        if interval_list is not None:
            cmd = "{0} -L {1}".format(cmd, interval_list)

        if gene_list is not None:
            cmd = "{0} -geneList {1}".format(cmd, gene_list)

        if read_group is not None:
            cmd = "{0} -pt {1}".format(cmd, read_group)

        if unsafe != 'ALLOW_N_CIGAR_READS':
            logging.error("Provided unsafe operation {0} is not allowed. Only 'ALLOW_N_CIGAR_READS' operation is "
                          "allowed.".format(unsafe))
            raise NotImplementedError("Invalid unsafe operation for DepthOfCoverage.")

        if unsafe:
            cmd = "{0} --unsafe {1}".format(cmd, unsafe)

        return "{0} !LOG3!".format(cmd)


class PreprocessIntervals(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(PreprocessIntervals, self).__init__(module_id, is_docker)
        self.output_keys = ["interval_list"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("interval_list", is_required=True, default_value=0)
        self.add_argument("bin_length", is_required=False, default_value=0)
        self.add_argument("padding", is_required=False, default_value=250)
        self.add_argument("nr_cpus", is_required=True, default_value=1)
        self.add_argument("mem", is_required=True, default_value=2)

    def define_output(self):
        # Declare interval list output filename
        interval_list = self.generate_unique_file_name(extension=".preprocessed.interval.list")
        self.add_output("interval_list", interval_list)

    def define_command(self):
        # Get input arguments
        interval_list = self.get_argument("interval_list")
        bin_length = self.get_argument("bin_length")
        padding = self.get_argument("padding")
        ref = self.get_argument("ref")

        # Get output arguments
        interval_list_out = self.get_output("interval_list")

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Generate the command line for PreProcessIntervals
        cmd = "{0} PreprocessIntervals -R {1} --bin-length {2} --padding {3} {4} {5}".format(gatk_cmd, ref, bin_length, padding,
                                                                                             output_file_flag, interval_list_out)

        # pass the location to include in the processing
        if interval_list is not None:
            cmd = "{0} -L {1} --interval-merging-rule OVERLAPPING_ONLY".format(cmd, interval_list)

        return "{0} !LOG3!".format(cmd)


class DenoiseReadCounts(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(DenoiseReadCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["std_copy_ratio", "denoise_copy_ratio"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("read_count_out", is_required=True)
        self.add_argument("pon", is_required=False, is_resource=True, default_value=None)
        self.add_argument("annotated_intervals", is_required=False, default_value=None)
        self.add_argument("number_of_eigensamples", is_required=False, default_value=None)
        self.add_argument("nr_cpus", is_required=True, default_value=1)
        self.add_argument("mem", is_required=True, default_value=2)

    def define_output(self):
        # Declare output filename for copy ratio
        std_copy_ratio = self.generate_unique_file_name(extension=".standardizedCR.txt")
        denoise_copy_ratio = self.generate_unique_file_name(extension=".denoisedCR.txt")
        self.add_output("std_copy_ratio", std_copy_ratio)
        self.add_output("denoise_copy_ratio", denoise_copy_ratio)

    def define_command(self):
        # Get input arguments
        read_count_out = self.get_argument("read_count_out")
        pon = self.get_argument("pon")
        annotated_intervals = self.get_argument("annotated_intervals")
        eigensamples = self.get_argument("number_of_eigensamples")

        # Get output arguments
        std_copy_ratio = self.get_output("std_copy_ratio")
        denoise_copy_ratio = self.get_output("denoise_copy_ratio")

        if pon is not None and annotated_intervals is not None:
            logging.error("PoN and annotated intervals both can not be provided at the same time.")
            raise NotImplementedError("PoN and annotated intervals both can not be provided at the same time.")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} DenoiseReadCounts".format(gatk_cmd)

        # if the Panel of Normal is provided, denoise the read count using it
        if pon:
            cmd = "{0} --count-panel-of-normals {1}".format(cmd, pon)

        # if the annotated interval is provided, denoise the read count using it
        if annotated_intervals:
            cmd = "{0} --annotated-intervals {1}".format(cmd, annotated_intervals)

        # if the number of eigne samples is provided, use them for denoising
        if eigensamples:
            cmd = "{0} --number-of-eigensamples {1}".format(cmd, eigensamples)

        # add the rest of the arguments to command
        cmd = "{0} -I {1} --standardized-copy-ratios {2} --denoised-copy-ratios {3}".format(cmd, read_count_out,
                                                                                            std_copy_ratio,
                                                                                            denoise_copy_ratio)

        return "{0} !LOG3!".format(cmd)


class PlotDenoisedCopyRatios(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(PlotDenoisedCopyRatios, self).__init__(module_id, is_docker)
        self.output_keys = ["denoised_plot", "denoised_plot_lm4", "std_mad", "denoised_mad", "delta_mad",
                            "scale_delta_mad"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name", is_required=True)
        self.add_argument("std_copy_ratio", is_required=True)
        self.add_argument("denoise_copy_ratio", is_required=False, default_value=None)
        self.add_argument("ref_dict", is_required=False, is_resource=True)
        self.add_argument("min_contig_len", is_required=False, default_value=46709983)
        self.add_argument("nr_cpus", is_required=True, default_value=1)
        self.add_argument("mem", is_required=True, default_value=2)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        denoised_plot = self.generate_unique_file_name(extension="denoised.png")

        # Split the genereated unique output file name to get prefix to use to generate other output filenames
        prefix = denoised_plot.split(".denoised.png")[0]

        # Generate rest of the output file names
        denoised_plot_lm4 = "{0}.denoisedLimit4.png".format(prefix)
        std_mad = "{0}.standardizedMAD.txt".format(prefix)
        denoised_mad = "{0}.denoisedMAD.txt".format(prefix)
        delta_mad = "{0}.deltaMAD.txt".format(prefix)
        scale_delta_mad = "{0}.scaledDeltaMAD.txt".format(prefix)

        # Add output file keys to be returned to Bucket
        self.add_output("denoised_plot", denoised_plot)
        self.add_output("denoised_plot_lm4", denoised_plot_lm4)
        self.add_output("std_mad", std_mad)
        self.add_output("denoised_mad", denoised_mad)
        self.add_output("delta_mad", delta_mad)
        self.add_output("scale_delta_mad", scale_delta_mad)

    def define_command(self):
        # Get input arguments
        std_copy_ratio = self.get_argument("std_copy_ratio")
        denoise_copy_ratio = self.get_argument("denoise_copy_ratio")
        ref_dict = self.get_argument("ref_dict")
        min_contig_len = self.get_argument("min_contig_len")

        # get the prefix for output file names
        prefix = self.get_output("denoised_plot").get_filename().split(".denoised.png")[0]

        # Get output directory
        out_dir = self.get_output_dir()

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} PlotDenoisedCopyRatios".format(gatk_cmd)

        # add the rest of the arguments to command
        cmd = "{0} --standardized-copy-ratios {1} --denoised-copy-ratios {2} --sequence-dictionary {3} " \
              "--minimum-contig-length {4} --output-prefix {5} {6} {7}".format(cmd, std_copy_ratio, denoise_copy_ratio,
                                                                               ref_dict, min_contig_len, prefix,
                                                                               output_file_flag, out_dir)

        return "{0} !LOG3!".format(cmd)


class CollectAllelicCounts(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CollectAllelicCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["allelic_counts"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name", is_required=True)
        self.add_argument("bam", is_required=True)
        self.add_argument("bam_idx", is_required=True)
        self.add_argument("ref", is_required=True, is_resource=True)
        self.add_argument("interval_list", is_required=True)
        self.add_argument("nr_cpus", is_required=True, default_value=8)
        self.add_argument("mem", is_required=True, default_value=30)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        allelic_counts = self.generate_unique_file_name(extension="allelicCounts.txt")

        self.add_output("allelic_counts", allelic_counts)

    def define_command(self):
        # Get input arguments
        bam = self.get_argument("bam")
        ref = self.get_argument("ref")
        interval_list = self.get_argument("interval_list")

        # get the prefix for output file names
        allelic_counts = self.get_output("allelic_counts")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} CollectAllelicCounts".format(gatk_cmd)

        # add the rest of the arguments to command
        cmd = "{0} -I {1} -R {2} -L {3} {4} {5}".format(cmd, bam, ref, interval_list, output_file_flag, allelic_counts)

        return "{0} !LOG3!".format(cmd)


class ModelSegments(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(ModelSegments, self).__init__(module_id, is_docker)
        self.output_keys = ["model_begin_seg", "model_final_seg", "cr_seg", "model_begin_af_param",
                            "model_begin_cr_param", "model_final_af_param", "model_final_cr_param"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name", is_required=True)
        self.add_argument("denoise_copy_ratio", is_required=True)
        self.add_argument("nr_cpus", is_required=True, default_value=4)
        self.add_argument("mem", is_required=True, default_value=8)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        model_begin_seg = self.generate_unique_file_name(extension="modelBegin.seg")

        # Split the genereated unique output file name to get prefix to use to generate other output filenames
        prefix = model_begin_seg.split(".modelBegin.seg")[0]

        # Generate rest of the output file names
        model_final_seg = "{0}.modelFinal.seg".format(prefix)
        cr_seg = "{0}.cr.seg".format(prefix)
        model_begin_af_param = "{0}.modelBegin.af.param".format(prefix)
        model_begin_cr_param = "{0}.modelBegin.cr.param".format(prefix)
        model_final_af_param = "{0}.modelFinal.af.param".format(prefix)
        model_final_cr_param = "{0}.modelFinal.cr.param".format(prefix)

        # Add output file keys to be returned to Bucket
        self.add_output("model_begin_seg", model_begin_seg)
        self.add_output("model_final_seg", model_final_seg)
        self.add_output("cr_seg", cr_seg)
        self.add_output("model_begin_af_param", model_begin_af_param)
        self.add_output("model_begin_cr_param", model_begin_cr_param)
        self.add_output("model_final_af_param", model_final_af_param)
        self.add_output("model_final_cr_param", model_final_cr_param)

    def define_command(self):
        # Get input arguments
        denoise_copy_ratio = self.get_argument("denoise_copy_ratio")

        # get the prefix for output file names
        prefix = self.get_output("model_begin_seg").get_filename().split(".modelBegin.seg")[0]

        # Get output directory
        out_dir = self.get_output_dir()

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} ModelSegments".format(gatk_cmd)

        # add the rest of the arguments to command
        cmd = "{0} --denoised-copy-ratios {1} --output-prefix {2} {3} {4}".format(cmd, denoise_copy_ratio, prefix,
                                                                                  output_file_flag, out_dir)

        return "{0} !LOG3!".format(cmd)


class CallCopyRatioSegments(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CallCopyRatioSegments, self).__init__(module_id, is_docker)
        self.output_keys = ["seg_call"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name", is_required=True)
        self.add_argument("cr_seg", is_required=True)
        self.add_argument("nr_cpus", is_required=True, default_value=4)
        self.add_argument("mem", is_required=True, default_value=8)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        called_seg = self.generate_unique_file_name(extension="called.seg")

        # Add output file keys to be returned to Bucket
        self.add_output("seg_call", called_seg)

    def define_command(self):
        # Get input arguments
        cr_seg = self.get_argument("cr_seg")

        # get the prefix for output file name
        called_seg = self.get_output("seg_call")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} CallCopyRatioSegments".format(gatk_cmd)

        # add the rest of the arguments to command
        cmd = "{0} -I {1} {2} {3}".format(cmd, cr_seg, output_file_flag, called_seg)

        return "{0} !LOG3!".format(cmd)


class PlotModeledSegments(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(PlotModeledSegments, self).__init__(module_id, is_docker)
        self.output_keys = ["model_plot"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name", is_required=True)
        self.add_argument("denoise_copy_ratio", is_required=True)
        self.add_argument("model_final_seg", is_required=True)
        self.add_argument("ref_dict", is_required=True)
        self.add_argument("nr_cpus", is_required=True, default_value=4)
        self.add_argument("mem", is_required=True, default_value=8)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        modeled_plot = self.generate_unique_file_name(extension="modeled.png")

        # Add output file keys to be returned to Bucket
        self.add_output("model_plot", modeled_plot)

    def define_command(self):
        # Get input arguments
        denoise_copy_ratio = self.get_argument("denoise_copy_ratio")
        model_final_seg = self.get_argument("model_final_seg")
        ref_dict = self.get_argument("ref_dict")

        # get the prefix for output file name
        prefix = self.get_output("model_plot").get_filename().split(".modeled.png")[0]

        # Get output directory
        out_dir = self.get_output_dir()

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} PlotModeledSegments".format(gatk_cmd)

        # add the rest of the arguments to command
        cmd = "{0} --denoised-copy-ratios {1} --segments {2} --sequence-dictionary {3} --output-prefix {4} {5} {6}" \
              "".format(cmd, denoise_copy_ratio, model_final_seg, ref_dict, prefix, output_file_flag, out_dir)

        return "{0} !LOG3!".format(cmd)


class CreateSomaticPanelOfNormals(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CreateSomaticPanelOfNormals, self).__init__(module_id, is_docker)
        self.output_keys = ["pon_vcf_gz", "pon_vcf_tbi"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("genomicsDB",     is_required=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value=8)

    def define_output(self):

        # Declare VCF output filename
        pon_vcf = self.generate_unique_file_name(extension=".vcf.gz")
        self.add_output("pon_vcf_gz", pon_vcf)

        # Declare VCF index output filename
        pon_vcf_tbi = self.generate_unique_file_name(extension=".vcf.gz.tbi")
        self.add_output("pon_vcf_tbi", pon_vcf_tbi)

    def define_command(self):
        # Get input arguments
        genomicsDB  = self.get_argument("genomicsDB")
        ref         = self.get_argument("ref")

        # get the output file name
        pon = self.get_output("pon_vcf_gz")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Generate the command line for CreateSomaticPanelOfNormals
        cmd = "{0} CreateSomaticPanelOfNormals -R {1} -V gendb://{2} -O {3}".format(gatk_cmd, ref, genomicsDB, pon)

        return "{0} !LOG3!".format(cmd)


class CollectGcBiasMetrics(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CollectGcBiasMetrics, self).__init__(module_id, is_docker)
        self.output_keys = ["gc_bias_matrics", "gc_bias_plot", "summary_matrics"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value=16)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        gc_bias_matrics = self.generate_unique_file_name(extension="{0}.gc.bias.matrics.txt".format(sample_name))
        gc_bias_plot    = self.generate_unique_file_name(extension="{0}.gc.bias.matrics.pdf".format(sample_name))
        summary_matrics = self.generate_unique_file_name(extension="{0}.summary.matrics.txt".format(sample_name))

        self.add_output("gc_bias_matrics", gc_bias_matrics)
        self.add_output("gc_bias_plot", gc_bias_plot)
        self.add_output("summary_matrics", summary_matrics)

    def define_command(self):

        # Get input arguments
        bam = self.get_argument("bam")
        ref = self.get_argument("ref")

        # Get the output file names
        gc_bias_matrics = self.get_output("gc_bias_matrics")
        gc_bias_plot    = self.get_output("gc_bias_plot")
        summary_matrics = self.get_output("summary_matrics")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} CollectGcBiasMetrics".format(gatk_cmd)

        # Add the rest of the arguments to command
        cmd = "{0} -I {1} -R {2} {3} {4} -CHART {5} -S {6}".format(cmd, bam, ref, output_file_flag, gc_bias_matrics,
                                                                   gc_bias_plot, summary_matrics)

        return "{0} !LOG3!".format(cmd)


class FilterSamReads(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(FilterSamReads, self).__init__(module_id, is_docker)
        self.output_keys = ["bam"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("read_names",     is_required=False)
        self.add_argument("interval_list",  is_required=False)
        self.add_argument("include_reads",  is_required=False, default_value=True)
        self.add_argument("exclude_reads",  is_required=False, default_value=False)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value=16)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        bam     = self.generate_unique_file_name(extension="{0}.bam".format(sample_name))

        self.add_output("bam", bam)

    def define_command(self):

        # Get input arguments
        bam                 = self.get_argument("bam")
        read_names_file     = self.get_argument("read_names")
        interval_list       = self.get_argument("interval_list")
        include_reads       = self.get_argument("include_reads")
        exclude_reads       = self.get_argument("exclude_reads")

        if exclude_reads:
            include_reads = False

        if read_names_file is None and interval_list is None:
            raise Exception("Neither read names file nor interval list are provided. Please provide either of them.")

        if read_names_file is not None and interval_list is not None:
            raise Exception("Both read names file and interval list are provided. Please provide either of them.")

        # Get the output file names
        bam_out = self.get_output("bam")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for DenoiseReadCounts
        cmd = "{0} FilterSamReads".format(gatk_cmd)

        # Add the rest of the arguments to command
        cmd = "{0} -I {1} {2} {3}".format(cmd, bam, output_file_flag, bam_out)

        # Add read list file if read list is provided
        if read_names_file:
            if include_reads:
                cmd = "{0} --READ_LIST_FILE {1} --FILTER includeReadList".format(cmd, read_names_file)
            elif exclude_reads:
                cmd = "{0} --READ_LIST_FILE {1} --FILTER excludeReadList".format(cmd, read_names_file)

        # Add interval list file if interval list is provided
        if interval_list:
            cmd = "{0} --INTERVAL_LIST {1} --FILTER includePairedIntervals".format(cmd, interval_list)

        return "{0} !LOG3!".format(cmd)


class CollectOxoGMetrics(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CollectOxoGMetrics, self).__init__(module_id, is_docker)
        self.output_keys = ["oxog_bias_matrics"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value=16)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        oxog_bias_matrics = self.generate_unique_file_name(extension="{0}.oxog.bias.matrics.txt".format(sample_name))

        self.add_output("oxog_bias_matrics", oxog_bias_matrics)

    def define_command(self):

        # Get input arguments
        bam = self.get_argument("bam")
        ref = self.get_argument("ref")

        # Get the output file names
        oxog_bias_matrics = self.get_output("oxog_bias_matrics")

        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for CollectOxoGMetrics
        cmd = "{0} CollectOxoGMetrics".format(gatk_cmd)

        # Add the rest of the arguments to command
        cmd = "{0} -I {1} -R {2} {3} {4}".format(cmd, bam, ref, output_file_flag, oxog_bias_matrics)

        return "{0} !LOG3!".format(cmd)


class CollectSequencingArtifactMetrics(_GATKBase):

    def __init__(self, module_id, is_docker=False):
        super(CollectSequencingArtifactMetrics, self).__init__(module_id, is_docker)
        self.output_keys = ["pre_adapter_detail_metrics","pre_adapter_summary_metrics","bait_bias_detail_metrics",
                            "bait_bias_summary_metrics", "error_summary_metrics"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=4)
        self.add_argument("mem",            is_required=True, default_value=16)

    def define_output(self):
        # Get the sample name to use it in file name creation
        sample_name = self.get_argument("sample_name")

        # Declare unique file name for a single output file
        pre_adapter_detail_metrics = self.generate_unique_file_name(extension="{0}.pre_adapter_detail_metrics".format(
            sample_name))
        pre_adapter_summary_metrics = self.generate_unique_file_name(extension="{0}.pre_adapter_summary_metrics".format(
            sample_name))
        bait_bias_detail_metrics = self.generate_unique_file_name(extension="{0}.bait_bias_detail_metrics".format(
            sample_name))
        bait_bias_summary_metrics = self.generate_unique_file_name(extension="{0}.bait_bias_summary_metrics".format(
            sample_name))
        error_summary_metrics = self.generate_unique_file_name(extension="{0}.error_summary_metrics".format(
            sample_name))

        self.add_output("pre_adapter_detail_metrics", pre_adapter_detail_metrics)
        self.add_output("pre_adapter_summary_metrics", pre_adapter_summary_metrics)
        self.add_output("bait_bias_detail_metrics", bait_bias_detail_metrics)
        self.add_output("bait_bias_summary_metrics", bait_bias_summary_metrics)
        self.add_output("error_summary_metrics", error_summary_metrics)

    def define_command(self):

        # Get input arguments
        bam = self.get_argument("bam")
        ref = self.get_argument("ref")

        # Get the output file names
        artifact_bias_matrics = self.get_output("pre_adapter_detail_metrics")
        artifact_bias_matrics = os.path.basename(artifact_bias_matrics.get_path())
        artifact_bias_matrics = os.path.splitext(artifact_bias_matrics)[0]

        output_dir = self.get_output_dir()

        artifact_bias_matrics = "{0}/{1}".format(output_dir, artifact_bias_matrics)


        # Get GATK base command
        gatk_cmd = self.get_gatk_command()

        # Get the output file flag depends on GATK version
        output_file_flag = self.get_output_file_flag()

        # Generate the command line for CollectOxoGMetrics
        cmd = "{0} CollectSequencingArtifactMetrics".format(gatk_cmd)

        # Add the rest of the arguments to command
        cmd = "{0} -I {1} -R {2} {3} {4}".format(cmd, bam, ref, output_file_flag, artifact_bias_matrics)

        return "{0} !LOG3!".format(cmd)
