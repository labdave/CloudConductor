from abc import ABC

from Modules import Module
from System.Platform import Platform


class _SentieonBase(Module, ABC):

    def __init__(self, module_id, is_docker=False):
        super(_SentieonBase, self).__init__(module_id, is_docker)

    def define_base_args(self):

        # Set Sentieon executable arguments
        self.add_argument("sentieon", is_required=True, is_resource=True)

    def get_sentieon_command(self):
        # Get input arguments
        sentieon    = self.get_argument("sentieon")

        return f'{sentieon}'


class BWAMem(_SentieonBase):

    def __init__(self, module_id, is_docker=False):
        super(BWAMem, self).__init__(module_id, is_docker)
        self.output_keys  = ["bam", "bam_sorted"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("R1",          is_required=True)
        self.add_argument("R2")
        self.add_argument("ref",         is_required=True)
        self.add_argument("read_group",  is_required=True)
        self.add_argument("nr_cpus",     is_required=True, default_value="max")
        self.add_argument("mem",         is_required=True, default_value="max(nr_cpus * 3, 35)")

    def define_output(self):
        # Declare bam output file
        bam_out = self.generate_unique_file_name(extension=".sorted.bam")
        self.add_output("bam", bam_out)

        # Declare that bam is sorted
        self.add_output("bam_sorted", True, is_path=False)

    def define_command(self):
        # Get arguments to run BWA aligner
        R1          = self.get_argument("R1")
        R2          = self.get_argument("R2")
        ref         = self.get_argument("ref")
        rg_header   = self.get_argument("read_group")
        nr_cpus     = self.get_argument("nr_cpus")

        # Get output arguments
        bam_out         = self.get_output("bam")

        # get the base command for Sentieon BWA-MEM command line
        sentieon_cmd = self.get_sentieon_command()

        if R2 is not None:
            # Generate bwa-mem paired-end command
            align_cmd = f'{sentieon_cmd} bwa mem -M -R "{rg_header}" -t {nr_cpus} {ref} {R1} {R2}'

        else:
            # Generate bwa-mem single-end command
            align_cmd = f'{sentieon_cmd} bwa mem -M -R "{rg_header}" -t {nr_cpus} {ref} {R1}'

        # Generating command to sort the SAM and converting it to BAM
        bam_sort_cmd = f'{sentieon_cmd} util sort -r {ref} -o {bam_out} -t {nr_cpus} --sam2bam -i -'

        return f'{align_cmd} !LOG2! | {bam_sort_cmd} !LOG2!'


class LocusCollector(_SentieonBase):

    def __init__(self, module_id, is_docker=False):
        super(LocusCollector, self).__init__(module_id, is_docker)
        self.output_keys  = ["vcf_gz"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("fun",        is_required=True, default_value="score_info")
        self.add_argument("nr_cpus",    is_required=True, default_value=8)
        self.add_argument("mem",        is_required=True, default_value="nr_cpus * 2")

    def define_output(self):
        # Declare score output file
        score_file = self.generate_unique_file_name(extension=".SCORE.vcf.gz")
        self.add_output("vcf_gz", score_file)


    def define_command(self):
        # Get arguments to run LocusCollector
        bam         = self.get_argument("bam")
        function    = self.get_argument("fun")
        nr_cpus     = self.get_argument("nr_cpus")

        # Get output arguments
        score_file         = self.get_output("vcf_gz")

        # get the base command for Sentieon command line
        sentieon_cmd = self.get_sentieon_command()

        return f'{sentieon_cmd} driver -t {nr_cpus} -i {bam} --algo LocusCollector --fun {function} {score_file} !LOG3!'


class MarkDuplicates(_SentieonBase):

    def __init__(self, module_id, is_docker=False):
        super(MarkDuplicates, self).__init__(module_id, is_docker)
        self.output_keys  = ["bam", "dedup_metric"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",        is_required=True)
        self.add_argument("bam_idx",    is_required=True)
        self.add_argument("vcf_gz",     is_required=True)
        self.add_argument("vcf_tbi",    is_required=True)
        self.add_argument("rmdup",      default_value=False)
        self.add_argument("nr_cpus",    is_required=True, default_value=8)
        self.add_argument("mem",        is_required=True, default_value="nr_cpus * 2")

    def define_output(self):
        # Declare dedup metric output file
        dedup_metric_file = self.generate_unique_file_name(extension=".dedup.metric.txt")

        # Declare dedup bam file
        dedup_bam = self.generate_unique_file_name(extension=".dedup.bam")

        self.add_output("dedup_metric", dedup_metric_file)
        self.add_output("bam", dedup_bam)


    def define_command(self):
        # Get arguments to run LocusCollector
        bam     = self.get_argument("bam")
        score   = self.get_argument("vcf_gz")
        rmdup   = self.get_argument("rmdup")
        nr_cpus = self.get_argument("nr_cpus")

        # Get output arguments
        dedup_bam   = self.get_output("bam")
        metric_file = self.get_output("dedup_metric")

        # get the base command for Sentieon command line
        sentieon_cmd = self.get_sentieon_command()

        # if the duplicates should be removed
        if rmdup:
            return f'{sentieon_cmd} driver -t {nr_cpus} -i {bam} --algo Dedup --rmdup --score_info {score} --metrics' \
                   f' {metric_file} {dedup_bam} !LOG3!'

        return f'{sentieon_cmd} driver -t {nr_cpus} -i {bam} --algo Dedup --score_info {score} --metrics' \
               f' {metric_file} {dedup_bam} !LOG3!'


class Haplotyper(_SentieonBase):

    def __init__(self, module_id, is_docker=False):
        super(Haplotyper, self).__init__(module_id, is_docker)
        self.output_keys  = ["vcf_gz", "vcf_idx"]

    def define_input(self):
        self.define_base_args()
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("ref",            is_required=True)
        self.add_argument("ref_idx",        is_required=True)
        self.add_argument("ref_dict",       is_required=True)
        self.add_argument("bed",            is_required=True)
        self.add_argument("dbsnp")
        self.add_argument("recal_table")
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Generate randomer string to attach with file name
        randomer = Platform.generate_unique_id()

        # Declare VCF output file
        vcf = self.generate_unique_file_name(extension=f'{randomer}.vcf.gz')
        self.add_output("vcf_gz", vcf)

        # Declare VCF index output filename
        vcf_idx = self.generate_unique_file_name(extension=f'{randomer}.vcf.gz.tbi')
        self.add_output("vcf_idx", vcf_idx)

    def define_command(self):
        # Get arguments to run Haplotyper
        bam         = self.get_argument("bam")
        ref         = self.get_argument("ref")
        bed         = self.get_argument("bed")
        dbsnp       = self.get_argument("dbsnp")
        recal_table = self.get_argument("recal_table")
        nr_cpus     = self.get_argument("nr_cpus")

        # Get output arguments
        vcf_gz         = self.get_output("vcf_gz")

        # get the base command for Sentieon BWA-MEM command line
        sentieon_cmd = self.get_sentieon_command()

        sentieon_cmd = f'{sentieon_cmd} driver -t {nr_cpus} -r {ref} -i {bam}'

        # if the target bed provided
        if bed:
            sentieon_cmd = f'{sentieon_cmd} --interval {bed}'

        # if recalibration table provided
        if recal_table:
            sentieon_cmd = f'{sentieon_cmd} -q {recal_table}'

        # add the algo
        sentieon_cmd = f'{sentieon_cmd} --algo Haplotyper'

        # if know sites/dbSNP provided
        if dbsnp:
            sentieon_cmd = f'{sentieon_cmd} -d {dbsnp}'

        return f'{sentieon_cmd} {vcf_gz} !LOG3!'
