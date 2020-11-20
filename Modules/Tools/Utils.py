import logging
import copy
from itertools import zip_longest

from System.Datastore import GAPFile
from Modules import Module


class ConcatFastq(Module):
    # Module designed to concatentate one or more R1, R2 files from the same sample
    # An example would be if you'd resequenced the same sample and wanted to used all sequence data as if it were a single FASTQ
    # If > 1 read pair: concat to a single read pair
    # If 1 read pair: return original file name without doing anything
    def __init__(self, module_id, is_docker=False):
        super(ConcatFastq, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2"]

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("R2")
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):

        # Declare R1 output name
        r1 = self.get_argument("R1")
        if not isinstance(r1, list):
            # Just pass the filename as is if no concatenation required (num R1 = 1)
            r1_copy = copy.deepcopy(self.arguments["R1"].get_value())
            self.add_output("R1", r1_copy)
        else:
            # Concatenate R1 files to new output
            extension = ".R1.fastq.gz" if r1[0].endswith(".gz") else "concat.R1.fastq"
            self.add_output("R1", self.generate_unique_file_name(extension=extension))

        # Conditionally declare R2 output name
        r2 = self.get_argument("R2")
        if not isinstance(r2, list):
            # Either R2 is single path or R2 is None
            r2_copy = copy.deepcopy(self.arguments["R2"].get_value())
            self.add_output("R2", r2_copy, is_path=(r2_copy is not None))
        else:
            extension = ".R2.fastq.gz" if r2[0].endswith(".gz") else "concat.R2.fastq"
            self.add_output("R2", self.generate_unique_file_name(extension=extension))

    def define_command(self):
        # Generate command for running Fastqc
        r1      = self.get_argument("R1")
        r2      = self.get_argument("R2")
        r1_out  = self.get_output("R1")
        r2_out  = self.get_output("R2")

        cmd = None
        if r1 != r1_out.get_path():
            # Concat R1 if a list of FASTQs was given as input
            cmd = "cat %s > %s !LOG2!" % (" ".join(r1), r1_out)

        if r2 is not None:
            # Check to make sure r1 and r2 contain same number of files
            self.__check_input(r1, r2)

            if r2 != r2_out.get_path():
                # Concat R2 if a list of FASTQs was given as input
                r2_cmd = "cat %s > %s !LOG2!" % (" ".join(r2), r2_out)
                # Join in the background so they run at the same time
                cmd = "%s & %s ; wait" % (cmd, r2_cmd)

        return cmd

    def __check_input(self, r1, r2):
        # Make sure each contains same number of fastq files
        error = False
        multi_r1    = isinstance(r1, list)
        multi_r2    = isinstance(r2, list)
        single_r1   = isinstance(r1, GAPFile) or isinstance(r1, str)
        single_r2   = isinstance(r2, GAPFile) or isinstance(r2, str)
        if multi_r1:
            if multi_r2 and len(r1) != len(r2):
                # Multiple R1, R2 but not the same in each
                error = True
                logging.error("ConcatFastq error! Input must contain same number of R1(%d) and R2(%d) fastq files!" % (len(r1), len(r2)))
            elif single_r2:
                # Multiple R1 only one R2
                error = True
                logging.error("ConcatFastq error! Input must contain same number of R1(%d) and R2(1) fastq files!" % len(r1))
        elif multi_r2 and single_r1:
            # One R1 multiple R2
            error = True
            logging.error("ConcatFastq error! Input must contain same number of R1(1) and R2(%d) fastq files!" % len(r2))
        if error:
            raise RuntimeError("Incorrect input to ConcatFastq!")


class ConsolidateSampleName(Module):

    def __init__(self, module_id, is_docker=False):
        super(ConsolidateSampleName, self).__init__(module_id, is_docker)
        self.output_keys = ["sample_name", "is_tumor"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("is_tumor",       is_required=False)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=1)

    def define_output(self):

        # Obtain the sample name(s)
        sample_name = self.get_argument("sample_name")
        is_tumor = self.get_argument("is_tumor")

        # Check if there is more than one sample name
        if isinstance(sample_name, list):

            # Ensure is_tumor is iterable
            if is_tumor is None:
                is_tumor = []

            # Simplify the sample names and make them unique
            samples = set()
            for _s, _t in zip_longest(sample_name, is_tumor):

                # Check if sample name is a list
                if isinstance(_s, list):
                    samples.update([(self.simplify_sample_ID(_ss), _t) for _ss in _s])
                else:
                    samples.add((self.simplify_sample_ID(_s), _t))

            # If more than one unique sample is found, throw a warning as this should NOT happen
            if len(samples) > 1:
                logging.warning("The output for sample name consolidation is more than one unique sample! "
                                "The analysis might not be biologically correct if alignment is performed "
                                "as one readgroup should be associated with maximum one sample!")

            # Obtain the first unique sample name
            sample_name, is_tumor = next(iter(samples))

        self.add_output("sample_name", sample_name, is_path=False)
        self.add_output("is_tumor", is_tumor, is_path=False)

    def define_command(self):
        return None

    @staticmethod
    def simplify_sample_ID(sample_ID):
        # Define list of special characters: (s)ubmission, (l)ibrary, (c)apture
        special_letters = ["s", "l", "c", "a"]

        # Remove elements that start with a special letter and the following characters are numbers
        valid = False
        while not valid:

            # Split current ID by the last underscore
            possible_sample_ID, last_ID = sample_ID.rsplit("_", 1)

            # Check if the ID after the last underscore is a special ID
            if last_ID[0].lower() in special_letters and \
                    (last_ID[1:].lower().isdigit() or last_ID[1:].lower() == "none"):

                sample_ID = possible_sample_ID

            # The last ID is not special, so we should keep it as part of the ID
            else:
                valid = True

        return sample_ID


class RecodeVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(RecodeVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["recoded_vcf"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("recode_vcf",         is_required=True,   is_resource=True)   # Path to RecodeVCF.py executable
        self.add_argument("min-call-depth",     is_required=True,   default_value=10)   # Minimum reads supporting an allele to call a GT
        self.add_argument("info-columns",       is_required=False)                      # Optional list of INFO column names to include in output
        self.add_argument("nr_cpus",            is_required=True,   default_value=2)
        self.add_argument("mem",                is_required=True,   default_value=10)

    def define_output(self):
        # Declare recoded VCF output filename
        recoded_vcf = self.generate_unique_file_name(extension=".recoded.vcf.txt")
        self.add_output("recoded_vcf", recoded_vcf)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        recode_vcf_exec     = self.get_argument("recode_vcf")
        min_call_depth      = self.get_argument("min-call-depth")
        info_columns        = self.get_argument("info-columns")

        # Get final recoded VCF output file path
        recoded_vcf_out = self.get_output("recoded_vcf")

        # Generate base command
        if not self.is_docker:
            cmd = "sudo -H pip install -U pyvcf ; python %s --vcf %s --output %s --min-call-depth %s -vvv" % (recode_vcf_exec, vcf_in, recoded_vcf_out, min_call_depth)
        else:
            cmd = "%s --vcf %s --output %s --min-call-depth %s -vvv" % (recode_vcf_exec, vcf_in, recoded_vcf_out, min_call_depth)

        # Optionally point to file specifying which vcf INFO fields to include in recoded output file
        if isinstance(info_columns, list):
            cmd += " --info-columns %s" % ",".join(info_columns)
        elif isinstance(info_columns, str):
            cmd += " --info-columns %s" % info_columns

        # Capture stderr
        cmd += " !LOG3!"

        # Return cmd
        return cmd


class SummarizeVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(SummarizeVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_summary"]

    def define_input(self):
        self.add_argument("vcf",                is_required=True)                       # Input VCF file
        self.add_argument("summarize_vcf",      is_required=True,   is_resource=True)   # Path to SummarizeVCF.py executable
        self.add_argument("summary_type",       is_required=True,   default_value="Multisample")
        self.add_argument("max_records",        is_required=False,  default_value=None) # Number of variants to process (None = all variants)
        self.add_argument("max_depth",          is_required=False,  default_value=None) # Upper limit of depth histogram
        self.add_argument("max_indel_len",      is_required=False,  default_value=None) # Upper limit of indel length histogram
        self.add_argument("max_qual",           is_required=False,  default_value=None) # Upper limit of quality score histogram
        self.add_argument("num_afs_bins",       is_required=False,  default_value=None) # Number of histogram bins for alternate allele frequency (AAF) distribution
        self.add_argument("nr_cpus",            is_required=True,   default_value=1)
        self.add_argument("mem",                is_required=True,   default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_summary = self.generate_unique_file_name(extension=".summary.txt")
        self.add_output("vcf_summary", vcf_summary)

    def define_command(self):
        # Get input arguments
        vcf_in              = self.get_argument("vcf")
        summarize_vcf_exec  = self.get_argument("summarize_vcf")
        summary_type        = self.get_argument("summary_type")

        # Optional arguments
        max_records     = self.get_argument("max_records")
        max_depth       = self.get_argument("max_depth")
        max_indel_len   = self.get_argument("max_indel_len")
        max_qual        = self.get_argument("max_qual")
        num_afs_bins    = self.get_argument("num_afs_bins")

        # Get final recoded VCF output file path
        vcf_summary = self.get_output("vcf_summary")

        # Generate base command
        if not self.is_docker:
            cmd = "sudo -H pip install -U pyvcf ; python %s %s --vcf %s -vvv" % (summarize_vcf_exec, summary_type, vcf_in)
        else:
            cmd = "%s %s --vcf %s -vvv" % (summarize_vcf_exec, summary_type, vcf_in)

        # Optionally point to file specifying which vcf INFO fields to include in recoded output file
        if max_records is not None:
            cmd += " --max-records %s" % max_records

        # Optionally specify upper limit of depth histogram
        if max_depth is not None:
            cmd += " --max-depth %s" % max_depth

        # Optionally specify upper limit of indel length histogram
        if max_indel_len is not None:
            cmd += " --max-indel-len %s" % max_indel_len

        # Optionallyk specify upper limit of quality score histogram
        if max_qual is not None:
            cmd += " --max-qual %s" % max_qual

        # Optionally specify number of bins for alternate allele frequency spectrum (AAFS)
        if num_afs_bins is not None:
            cmd += " --afs-bins %s" % num_afs_bins

        # Capture stderr and write stdout to output file
        cmd += " > %s !LOG2!" % vcf_summary

        # Return cmd
        return cmd


class ViralFilter(Module):
    def __init__(self, module_id, is_docker=False):
        super(ViralFilter, self).__init__(module_id, is_docker)
        self.output_keys = ["bam"]

    def define_input(self):
        self.add_argument("bam",                    is_required=True)
        self.add_argument("viral_filter",           is_resource=True, is_required=True)
        self.add_argument("nr_cpus",                is_required=True, default_value=1)
        self.add_argument("mem",                    is_required=True, default_value=4)
        self.add_argument("min_align_length",       is_required=False, default_value=40)
        self.add_argument("min_map_quality",        is_required=False, default_value=30)
        self.add_argument("only_properly_paired",   is_required=False, default_value=False)
        self.add_argument("max_window_length",      is_required=False, default_value=3)
        self.add_argument("max_window_freq",        is_required=False, default_value=0.6)

    def define_output(self):
        # Declare output bam filename
        output_bam = self.generate_unique_file_name(extension=".filtered.bam")
        self.add_output("bam", output_bam)

    def define_command(self):
        # Define command for running viral filter from a platform
        bam                 = self.get_argument("bam")
        viral_filter        = self.get_argument("viral_filter")
        min_align_length    = self.get_argument("min_align_length")
        min_map_quality     = self.get_argument("min_map_quality")
        only_properly_paired = self.get_argument("only_properly_paired")
        max_window_length   = self.get_argument("max_window_length")
        max_window_freq     = self.get_argument("max_window_freq")
        output_bam          = self.get_output("bam")

        # Generating filtering command
        if not self.is_docker:
            return "sudo -H pip install -U pysam; {0} {1} -v {2} -o {3} -l {4} -q {5} -w {6} -f {7}".format(
                viral_filter, "-p" if only_properly_paired else "", bam, output_bam, min_align_length,
                min_map_quality, max_window_length, max_window_freq)

        return "{0} {1} -v {2} -o {3} -l {4} -q {5} -w {6} -f {7}".format(
            viral_filter, "-p" if only_properly_paired else "", bam, output_bam, min_align_length,
            min_map_quality, max_window_length, max_window_freq)


class IndexVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(IndexVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_gz", "vcf_tbi"]

    def define_input(self):
        self.add_argument("vcf",        is_required=True)
        self.add_argument("bgzip",      is_required=True,   is_resource=True)
        self.add_argument("tabix",      is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=8)
        self.add_argument("mem",        is_required=True,   default_value=16)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_in = self.get_argument("vcf")
        self.add_output("vcf_gz", "%s.gz" % vcf_in)
        self.add_output("vcf_tbi", "%s.gz.tbi" % vcf_in)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        bgzip       = self.get_argument("bgzip")
        tabix       = self.get_argument("tabix")
        vcf_out     = self.get_output("vcf_gz")
        # Get final normalized VCF output file path
        return "{0} {1} !LOG2!; {2} -p vcf {3} !LOG2!".format(bgzip, vcf_in, tabix, vcf_out)


class IndexVCFGZ(Module):

    def __init__(self, module_id, is_docker=False):
        super(IndexVCFGZ, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_tbi"]

    def define_input(self):
        self.add_argument("vcf_gz",     is_required=True)
        self.add_argument("tabix",      is_required=True,   is_resource=True)
        self.add_argument("nr_cpus",    is_required=True,   default_value=8)
        self.add_argument("mem",        is_required=True,   default_value=16)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_in = self.get_argument("vcf_gz")
        self.add_output("vcf_tbi", f'{vcf_in}.tbi')

    def define_command(self):
        # Get input arguments
        vcf_gz      = self.get_argument("vcf_gz")
        tabix       = self.get_argument("tabix")

        # Get final normalized VCF output file path
        return f'{tabix} -p vcf {vcf_gz} !LOG3!'


class IndexBED(Module):

    def __init__(self, module_id, is_docker=False):
        super(IndexBED, self).__init__(module_id, is_docker)
        self.output_keys    = ["bed_tbi"]

    def define_input(self):
        self.add_argument("bed_gz",     is_required=True)
        self.add_argument("tabix",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=4)

    def define_output(self):
        # Declare recoded VCF output filename
        bed_in = self.get_argument("bed_gz")

        self.add_output("bed_tbi", "{0}.tbi".format(bed_in))

    def define_command(self):
        # Get input arguments
        bed_in      = self.get_argument("bed_gz")
        tabix       = self.get_argument("tabix")

        # return the command line
        return "{0} -p bed {1} !LOG2!".format(tabix, bed_in)


class GunzipVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(GunzipVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf"]

    def define_input(self):
        self.add_argument("vcf_gz",         is_required=True)
        self.add_argument("gunzip",         is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_in = self.get_argument("vcf_gz").rsplit(".",1)[0]

        self.add_output("vcf", vcf_in)

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf_gz")
        gunzip       = self.get_argument("gunzip")

        # Get final normalized VCF output file path
        cmd = "{0} {1} !LOG2!".format(gunzip, vcf_in)

        return cmd


class BGZipVCF(Module):

    def __init__(self, module_id, is_docker=False):
        super(BGZipVCF, self).__init__(module_id, is_docker)
        self.output_keys    = ["vcf_gz"]

    def define_input(self):
        self.add_argument("vcf",        is_required=True)
        self.add_argument("bgzip",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        vcf_in = self.get_argument("vcf")

        self.add_output("vcf_gz", "{0}.gz".format(vcf_in))

    def define_command(self):
        # Get input arguments
        vcf_in      = self.get_argument("vcf")
        bgzip       = self.get_argument("bgzip")
        vcf_out     = self.get_output("vcf_gz")

        # Get final normalized VCF output file path
        cmd = "{0} -c {1} > {2} !LOG2!".format(bgzip, vcf_in, vcf_out)
        return cmd


class BGZipBED(Module):

    def __init__(self, module_id, is_docker=False):
        super(BGZipBED, self).__init__(module_id, is_docker)
        self.output_keys    = ["bed_gz"]

    def define_input(self):
        self.add_argument("bed",        is_required=True)
        self.add_argument("bgzip",      is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=2)

    def define_output(self):
        # Declare recoded VCF output filename
        bed_in = self.get_argument("bed")

        self.add_output("bed_gz", "{0}.gz".format(bed_in))

    def define_command(self):
        # Get input arguments
        bed_in      = self.get_argument("bed")
        bgzip       = self.get_argument("bgzip")
        bed_out     = self.get_output("bed_gz")

        # Get final normalized VCF output file path
        cmd = "{0} -c {1} > {2} !LOG2!".format(bgzip, bed_in, bed_out)
        return cmd


class GetReadGroup(Module):
    def __init__(self, module_id, is_docker = False):
        super(GetReadGroup, self).__init__(module_id, is_docker)
        self.output_keys = ["read_group"]
        self.does_process_output = True

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("lib_name",       is_required=True)
        self.add_argument("seq_platform",   is_required=True, default_value="Illumina")
        self.add_argument("nr_cpus",        is_required=True, default_value=1)
        self.add_argument("mem",            is_required=True, default_value=1)

    def define_output(self):
        self.add_output("read_group", None, is_path=False)

    def define_command(self):
        # Get arguments to run BWA aligner
        R1              = self.get_argument("R1")
        if R1.endswith(".gz"):
            cmd = "zcat %s | head -n 1" % R1
        else:
            cmd = "head -n 1 %s" % R1
        return cmd

    def process_cmd_output(self, out, err):

        # Obtain necessary data
        lib_name = self.get_argument("lib_name")
        seq_platform = self.get_argument("seq_platform")
        fastq_header_data = out.lstrip("@").strip("\n").split(":")

        # Obtain the sample name(s)
        sample_name = self.get_argument("sample_name")

        # Generating the read group information from command output
        rg_id = ":".join(fastq_header_data[0:4])  # Read Group ID
        rg_pu = fastq_header_data[-1]  # Read Group Platform Unit
        rg_sm = sample_name if not isinstance(sample_name, list) else sample_name[0]    # Read Group Sample
        rg_lb = lib_name if not isinstance(lib_name, list) else lib_name[0]             # Read Group Library ID
        rg_pl = seq_platform if not isinstance(seq_platform, list) else seq_platform[0] # Read Group Platform used
        read_group_header = "\\t".join(["@RG", "ID:%s" % rg_id, "PU:%s" % rg_pu,
                                        "SM:%s" % rg_sm, "LB:%s" % rg_lb, "PL:%s" % rg_pl])

        # Setting the read group
        logging.info("Read group: %s" % read_group_header)
        self.set_output("read_group", read_group_header)


class CombineExpressionWithMetadata(Module):
    def __init__(self, module_id, is_docker = False):
        super(CombineExpressionWithMetadata, self).__init__(module_id, is_docker)
        self.output_keys = ["annotated_expression_file"]

    def define_input(self):
        self.add_argument("expression_file",    is_required=True)
        self.add_argument("gtf",                is_required=True, is_resource=True)
        self.add_argument("combine_script",     is_required=True, is_resource=True)
        self.add_argument("result_type",        is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=4)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 5")

    def define_output(self):

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(extension=".txt")
        self.add_output("annotated_expression_file", output_file_name)

    def define_command(self):

        # Get arguments
        expression_file     = self.get_argument("expression_file")
        gtf_file            = self.get_argument("gtf")
        result_type         = self.get_argument("result_type")

        #get the script that combines the expression with metadata
        combine_script = self.get_argument("combine_script")

        #get the output file and make appropriate path for it
        output_file = self.get_output("annotated_expression_file")

        if not self.is_docker:
            #generate command line for Rscript
            cmd = "sudo Rscript --vanilla {0} -e {1} -a {2} -t {3} -o {4} !LOG3!".format(combine_script, expression_file,
                                                                                         gtf_file, result_type,
                                                                                         output_file)
        else:
            cmd = "Rscript --vanilla {0} -e {1} -a {2} -t {3} -o {4} !LOG3!".format(combine_script, expression_file,
                                                                                    gtf_file, result_type, output_file)

        return cmd


class GetVCFChroms(Module):
    def __init__(self, module_id, is_docker = False):
        super(GetVCFChroms, self).__init__(module_id, is_docker)
        self.output_keys = ["chrom_list"]
        self.does_process_output = True

    def define_input(self):
        self.add_argument("vcf",        is_required=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):
        self.add_output("chrom_list", [], is_path=False)

    def define_command(self):
        # Get arguments
        vcf = self.get_argument("vcf")
        cmd = 'cat {0} | grep -v "#" | cut -f1 | sort | uniq'.format(vcf)
        return cmd

    def process_cmd_output(self, out, err):
        # holds the chromosome list
        chrom_list = list()

        # iterate throgh the output generated by the command in define command
        for line in out.split("\n"):
            # Skip empty lines
            if len(line) > 0:
                chrom_list.append(line)
        self.set_output("chrom_list", out)


class GetRefChroms(Module):
    def __init__(self, module_id, is_docker = False):
        super(GetRefChroms, self).__init__(module_id, is_docker)
        self.output_keys = ["chrom_list"]
        self.does_process_output = True

    def define_input(self):
        self.add_argument("ref_idx",    is_required=True, is_resource=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=1)
        self.add_argument("mem",        is_required=True, default_value=1)

    def define_output(self):
        self.add_output("chrom_list", [], is_path=False)

    def define_command(self):
        # Get arguments
        ref_idx = self.get_argument("ref_idx")
        cmd = "cut -f1 {0}".format(ref_idx)
        return cmd

    def process_cmd_output(self, out, err):
        # holds the chromosome list
        chrom_list = list()

        # iterate throgh the output generated by the command in define command
        for line in out.split("\n"):
            # Skip empty lines
            if len(line) > 0:
                chrom_list.append(line)

        logging.info("Chrom List: %s" % ",".join(chrom_list))
        self.set_output("chrom_list", chrom_list)


class GetCellBarcodes(Module):
    def __init__(self, module_id, is_docker=False):
        super(GetCellBarcodes, self).__init__(module_id, is_docker)
        self.output_keys = ["barcode_list"]
        self.does_process_output = True

    def define_input(self):
        self.add_argument("barcode_file", is_required=True)
        self.add_argument("nr_cpus",      is_required=True, default_value=1)
        self.add_argument("mem",          is_required=True, default_value=1)

    def define_output(self):
        self.add_output("barcode_list", None, is_path=False)

    def define_command(self):
        # Cat the input so you can read the file
        barcode_file = self.get_argument("barcode_file")
        cmd = "cat {0}".format(barcode_file)
        return cmd

    def process_cmd_output(self, out, err):
        # Process the output into a Python list
        barcode_list = out.strip().split(",")

        # Check to see that formatting is okay
        if len(barcode_list) == 0:
            raise RuntimeError("Empty output")
        else:
            for barcode in barcode_list:
                # We expect each barcode to be 16 bp and only contain ACTG
                if not ((len(barcode) == 16) and (set(barcode).issubset({"A", "C", "T", "G"}))):
                    raise RuntimeError("Format of barcode input file is incorrect. Expected a file with one line of "
                                       "16-bp barcodes separated by commas.")

        # Log number of barcodes
        logging.info("Number of cellular barcodes: {0}".format(len(barcode_list)))

        self.set_output("barcode_list", barcode_list)

        
class SubsetBamByBarcode(Module):
    def __init__(self, module_id, is_docker=False):
        super(SubsetBamByBarcode, self).__init__(module_id, is_docker)
        self.output_keys = ["bam"]

    def define_input(self):
        self.add_argument("barcode",  is_required=True)
        self.add_argument("bam",      is_required=True)
        self.add_argument("samtools", is_required=True, is_resource=True)
        self.add_argument("nr_cpus",  is_required=True, default_value=2)
        self.add_argument("mem",      is_required=True, default_value=4)

    def define_output(self):
        bam_out = self.generate_unique_file_name(extension=".bam")
        self.add_output("bam", bam_out)

    def define_command(self):
        # Get arguments
        barcode    = self.get_argument("barcode")
        input_bam  = self.get_argument("bam")
        output_bam = self.get_output("bam")
        samtools   = self.get_argument("samtools")
        nr_cpus    = self.get_argument("nr_cpus")

        # Generating the commands that will be piped together
        cmds = list()

        # View BAM as a SAM so you can read the barcode; include the SAM header with -h
        cmds.append("{0} view -@ {1} -h {2}".format(samtools, nr_cpus, input_bam))

        # Select only the header and reads with the barcode
        cmds.append("grep -e '^@' -e 'CB:Z:{0}' -".format(barcode))

        # Convert back to BAM and write to output
        cmds.append("{0} view -@ {1} -S -b - > {2} !LOG2! ".format(samtools, nr_cpus, output_bam))

        # Pipe everything together
        cmd = " | ".join(cmds)

        return cmd


class ReplaceGVCFSampleName(Module):
    def __init__(self, module_id, is_docker=False):
        super(ReplaceGVCFSampleName, self).__init__(module_id, is_docker)
        self.output_keys = ["gvcf"]

    def define_input(self):
        self.add_argument("barcode",      is_required=True)
        self.add_argument("gvcf",         is_required=True)
        self.add_argument("sample_name",  is_required=True)
        self.add_argument("nr_cpus",      is_required=True, default_value=1)
        self.add_argument("mem",          is_required=True, default_value=1)

    def define_output(self):
        gvcf = self.generate_unique_file_name(extension=".g.vcf")
        self.add_output("gvcf", gvcf)

    def define_command(self):
        sample_name = self.get_argument("sample_name")
        barcode     = self.get_argument("barcode")
        gvcf_in     = self.get_argument("gvcf")
        gvcf_out    = self.get_output("gvcf")

        # The one line with the sample name at the end of the line ($) is the header line #CHROM ... sample_name,
        # so we replace that sample name with the barcode
        cmd = 'sed "s/{0}$/{1}/g" {2} > {3}'.format(sample_name, barcode, gvcf_in, gvcf_out)

        return cmd


class GetDemuxFASTQ(Module):

    def __init__(self, module_id, is_docker=False):
        super(GetDemuxFASTQ, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2"]

    def define_input(self):
        self.add_argument("R1",             is_required=True)
        self.add_argument("R2",             is_required=True)
        self.add_argument("assay_type",     is_required=True)
        self.add_argument("keep_assay_type", is_required=True)
        self.add_argument("nr_cpus",        is_required=True,   default_value=1)
        self.add_argument("mem",            is_required=True,   default_value=1)

    def define_output(self):
        # Obtain arguments
        R1 = self.get_argument("R1")
        R2 = self.get_argument("R2")
        assay_type = self.get_argument("assay_type")
        keep_type = self.get_argument("keep_assay_type").lower()

        # make sure the R1 is a list
        if not isinstance(R1, list):
            logging.error("Provided R1 is not a list. Please make sure you provide a list of R1.")
            raise TypeError("Provided R1 is not a list. Please make sure you provide a list of R1.")

        # make sure the R1 is a list
        if not isinstance(R2, list):
            logging.error("Provided R2 is not a list. Please make sure you provide a list of R2.")
            raise TypeError("Provided R2 is not a list. Please make sure you provide a list of R2.")

        # change the case for available assay type
        assay_type = [_el.lower() for _el in assay_type]

        # Check if assay type to keep is available
        if keep_type not in assay_type:
            logging.error("Provided assay type is not available. The available assay types are RNA and DNA.")
            raise NotImplementedError("Provided assay type is not available. The available assay types are RNA and DNA.")

        # Placeholder list to keep the respective R1 and R2
        keep_R1 = []
        keep_R2 = []

        # Get correct R1 and R2 based on assay type
        for r1_file, r2_file, _type in zip(R1, R2, assay_type):
            if _type == keep_type:
                keep_R1.append(r1_file)
                keep_R2.append(r2_file)

        # Add the first element of the list to ouput if the R1 and R2 is not a list, otherwise add the list
        if len(keep_R1) == 1:
            self.add_output(key="R1", value=keep_R1[0])
            self.add_output(key="R2", value=keep_R2[0])
        else:
            self.add_output(key="R1", value=keep_R1)
            self.add_output(key="R2", value=keep_R2)

    def define_command(self):
        return None


class GetSampleFromBAMHeader(Module):
    def __init__(self, module_id, is_docker = False):
        super(GetSampleFromBAMHeader, self).__init__(module_id, is_docker)
        self.output_keys = ["sample_name"]
        self.does_process_output = True

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=4)

    def define_output(self):
        self.add_output("sample_name", None, is_path=False)

    def define_command(self):
        # Get arguments to run SAMTOOLS
        bam = self.get_argument("bam")
        # extract read group line from the header
        cmd = 'samtools view -H {0} | grep "^@RG"'.format(bam)

        return cmd

    def process_cmd_output(self, out, err):

        # example of read group line
        # @RG	ID:HISEQ-WALDORF:249:C92LDANXX:2	PU:TATGATGG	SM:4339_B_S21599	LB:S21599	PL:Illumina
        sample_name = out.split('\t')[3].split(':')[-1]

        # Obtain necessary data
        self.set_output("sample_name", sample_name)


class GetReadNames(Module):
    def __init__(self, module_id, is_docker=False):
        super(GetReadNames, self).__init__(module_id, is_docker)
        self.output_keys = ["read_names"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bed",                is_required=False, is_resource=True)
        self.add_argument("bedtools",           is_required=True, is_resource=True)
        self.add_argument("samtools",           is_required=True, is_resource=True)
        self.add_argument("spliced_reads",      is_required=False)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=4)

    def define_output(self):
        read_names = self.generate_unique_file_name(extension=".read_names.txt")
        self.add_output("read_names", read_names)

    def define_command(self):
        # Get arguments
        bam             = self.get_argument("bam")
        bed             = self.get_argument("bed")
        bedtools        = self.get_argument("bedtools")
        samtools        = self.get_argument("samtools")
        spliced_reads   = self.get_argument("spliced_reads")

        # get the file name to store the read names
        read_names = self.get_output("read_names")

        if spliced_reads:

            # Generating the commands that will be piped together
            cmds = list()

            # Convert BAM to SAM
            cmds.append("{0} view {1}".format(samtools, bam))

            # Search for the "N" character in CIGAR string
            cmds.append('awk \'\"\'\"\'($6 ~ /N/)\'\"\'\"\'')

            # get the spliced read names
            cmds.append("cut -f 1 > {0} !LOG2!".format(read_names))

            # Pipe everything together
            cmd = " | ".join(cmds)

            return cmd

        # Generating the commands that will be piped together
        cmds = list()

        # Intersect BAM with a given BED
        if bed is not None:
            cmds.append("{0} intersect -a {1} -b {2} -split".format(bedtools, bam, bed))

        # Convert BAM to SAM
        cmds.append("{0} view {1}".format(samtools, bam))

        # get the read names
        cmds.append("cut -f 1 > {0} !LOG2!".format(read_names))

        # Pipe everything together
        cmd = " | ".join(cmds)

        return cmd


class CovertToSAM(Module):
    def __init__(self, module_id, is_docker=False):
        super(CovertToSAM, self).__init__(module_id, is_docker)
        self.output_keys = ["sam"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("bed",            is_required=False, is_resource=True)
        self.add_argument("samtools",       is_required=True, is_resource=True)
        self.add_argument("exclude_flag",   is_required=False)
        self.add_argument("include_flag",   is_required=False)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=4)

    def define_output(self):
        sam = self.generate_unique_file_name(extension=".sam")
        self.add_output("sam", sam)

    def define_command(self):
        # Get arguments
        bam             = self.get_argument("bam")
        bed             = self.get_argument("bed")
        samtools        = self.get_argument("samtools")
        exclude_flag    = self.get_argument("exclude_flag")
        include_flag    = self.get_argument("include_flag")

        # get the file name to store the read names
        sam = self.get_output("sam")

        # generate base command to convert BAM to SAM
        cmd = "{0} view {1}".format(samtools, bam)

        # if the exclude flag provided
        if exclude_flag:
            cmd += " -F {0}".format(exclude_flag)

        # if the include flag provided
        if include_flag:
            cmd += " -f {0}".format(include_flag)

        # if bed file provided
        if bed:
            cmd += " -L {0}".format(bed)

        cmd += " > {0} !LOG2!".format(sam)

        return cmd


class GetERCCReadCounts(Module):
    def __init__(self, module_id, is_docker=False):
        super(GetERCCReadCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["read_counts"]

    def define_input(self):
        self.add_argument("sam",                is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value=4)

    def define_output(self):
        read_counts = self.generate_unique_file_name(extension=".ercc_read_counts.txt")
        self.add_output("read_counts", read_counts)

    def define_command(self):
        # Get arguments
        sam             = self.get_argument("sam")

        read_counts     = self.get_output("read_counts")

        # generate command to count the reads and then filter for ERCC baits
        cmd = "if [ -s %s ]; then awk '{A[$3]++}END{for(i in A)print i,A[i]}' %s | grep '^ERCC' > %s !LOG2!; " \
              "else echo -e \"ERCC-00000\t0\" > %s !LOG2!; fi" % (sam,sam,read_counts,read_counts)

        return cmd


class SubsetFASTQ(Module):
    def __init__(self, module_id, is_docker=False):
        super(SubsetFASTQ, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2"]

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("R2")
        self.add_argument("seqtk",      is_required=True, is_resource=True)
        self.add_argument("sample",     is_required=True, default_value=1000000)
        self.add_argument("nr_cpus",    is_required=True, default_value=2)
        self.add_argument("mem",        is_required=True, default_value=4)

    def define_output(self):
        # Declare R1 output name
        sample = self.get_argument("sample")

        self.add_output("R1", self.generate_unique_file_name(extension=f'subset.{sample}.R1.fastq'))

        if self.get_argument("R2"):
            self.add_output("R2", self.generate_unique_file_name(extension=f'subset.{sample}.R2.fastq'))

    def define_command(self):
        # Get the input arguments
        R1      = self.get_argument("R1")
        R2      = self.get_argument("R2")
        seqtk   = self.get_argument("seqtk")
        sample  = self.get_argument("sample")

        # Get the output arguments
        r1_out = self.get_output("R1")

        r1_cmd = f'{seqtk} sample -s100 {R1} {sample} > {r1_out}'

        if R2:
            r2_out = self.get_output("R2")

            r2_cmd = f'{seqtk} sample -s100 {R2} {sample} > {r2_out}'

            return f'{r1_cmd} !LOG2!; {r2_cmd} !LOG2!'

        return f'{r1_cmd} !LOG2!'


class SpringUnzip(Module):
    def __init__(self, module_id, is_docker=False):
        super(SpringUnzip, self).__init__(module_id, is_docker)
        self.output_keys = ["R1", "R2"]

    def define_input(self):
        self.add_argument("R1",         is_required=True)
        self.add_argument("R2",         is_required=True)
        self.add_argument("nr_cpus",    is_required=True, default_value=4)
        self.add_argument("mem",        is_required=True, default_value=20)

    def define_output(self):
        # Declare R1 output name
        R1          = self.get_argument("R1")
        R2          = self.get_argument("R2")

        self.add_output("R1", R1)
        self.add_output("R2", R2)

    def define_command(self):
        # Get the input arguments
        R1_in       = self.get_argument("R1")
        R2_in       = self.get_argument("R2")
        
        # Get the output arguments
        R1_out = self.get_output("R1")
        R2_out = self.get_output("R2")

        # check if you can gunzip - if not, then rename to genozip and 
        # genounzip. This will give us the completely unzipped version.
        # Then, we gzip it back to gz

        cmd = ""

        R1 = R1_in.rstrip(".gz")
        cmd += f"gunzip {R1} || {{ mv {R1} {R1}.spring && spring -d -i {R1}.spring -o {R1_out} -g !LOG3!; }}"

        R2 = R2_in.rstrip(".gz")
        cmd += f"gunzip {R2} || {{ mv {R2} {R2}.spring && spring -d -i {R2}.spring -o {R2_out} -g !LOG3!; }}"
        
        return cmd