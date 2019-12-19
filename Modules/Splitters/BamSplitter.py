
from Modules import Splitter

class BamSplitter(Splitter):

    def __init__(self, module_id, is_docker=False):
        super(BamSplitter, self).__init__(module_id, is_docker)
        self.output_keys = ["bam", "is_aligned", "chroms"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("samtools",           is_required=True, is_resource=True)
        self.add_argument("chrom_list",         is_required=False)
        self.add_argument("nr_splits",          is_required=True, default_value=23)
        self.add_argument("nr_cpus",            is_required=True, default_value=8)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 1.5")

    def define_output(self):
        # Obtaining the arguments
        chrom_list  = self.get_argument("chrom_list")
        nr_splits   = self.get_argument("nr_splits")

        chroms, remains = self.__get_chrom_splits(chrom_list, nr_splits)

        # Add split info for named chromosomes
        for chrom in chroms:
            self.make_split(split_id=chrom)
            split_bam = self.generate_unique_file_name(split_id=chrom, extension=".bam")
            self.add_output(split_id=chrom, key="bam", value=split_bam)
            self.add_output(split_id=chrom, key="chroms", value=chrom, is_path=False)
            self.add_output(split_id=chrom, key="is_aligned", value=True, is_path=False)

        # Add split info for all chromosomes that aren't named in config
        self.make_split(split_id="remains")
        bam = self.generate_unique_file_name(split_id="remains", extension=".bam")
        self.add_output(split_id="remains", key="bam", value=bam)
        self.add_output(split_id="remains", key="chroms", value=remains, is_path=False)
        self.add_output(split_id="remains", key="is_aligned", value=True, is_path=False)

        # Add split info for unmapped reads
        self.make_split(split_id="unmapped")
        bam = self.generate_unique_file_name(split_id="unmapped", extension=".bam")
        self.add_output(split_id="remains", key="bam", value=bam)
        self.add_output(split_id="remains", key="chroms", value=None, is_path=False)
        self.add_output(split_id="remains", key="is_aligned", value=False, is_path=False)

    def define_command(self):
        # Obtaining the arguments
        bam         = self.get_argument("bam")
        samtools    = self.get_argument("samtools")
        nr_cpus     = self.get_argument("nr_cpus")

        # Get names of chromosomes to split
        chroms  = [self.output[split_name]["chroms"] for split_name in list(self.output.keys()) if split_name not in ["remains", "unmapped"]]

        # Get names of remaining chromosomes
        remains = self.output["remains"]["chroms"]

        # Get output file basename
        split_name      = list(self.output.keys())[0]
        output_basename = self.output[split_name]["bam"].split(split_name)[0]

        # Generating the commands
        cmds = list()

        # Obtaining the chromosomes in parallel
        cmd = '%s view -@ %d -u -F 4 %s $chrom_name > %s$chrom_name.bam' % (samtools, nr_cpus, bam, output_basename)
        cmds.append('for chrom_name in %s; do %s & done' % (" ".join(chroms), cmd))

        # Obtaining the remaining chromosomes from the bam header
        cmds.append('%s view -@ %d -u -F 4 %s %s > %sremains.bam'
                    % (samtools, nr_cpus, bam, " ".join(remains), output_basename))

        # Obtaining the unaligned reads
        cmds.append('%s view -@ %d -u -f 4 %s > %sunmapped.bam'
                    % (samtools, nr_cpus, bam, output_basename))

        # Parallel split of the files
        return "%s ; wait" % " & ".join(cmds)

    @staticmethod
    def __get_chrom_splits(chrom_list, nr_splits):
        # Returns two lists, one containing the names of chromosomes which will be considered separate splits
        # Create list of split and lumped chromosome by adding chromosomes in order of size until number of splits is reached
        chroms = list()
        remains = chrom_list
        i = 0
        while (i < nr_splits-1) and (len(remains) > 0):
            # Create split for next largest chromosome if any more chromosomes are left
            chroms.append(chrom_list[i])
            # Remove chromosome name from list of chromosomes to be lumped together
            remains.remove(chrom_list[i])
            i += 1
        return chroms, remains


class BamCombinator(Splitter):

    def __init__(self, module_id, is_docker=False):
        super(BamCombinator, self).__init__(module_id, is_docker)
        self.output_keys = ["bam", "bam_idx", "sample_id"]

    def define_input(self):
        self.add_argument("bam",                is_required=True)
        self.add_argument("bam_idx",            is_required=True)
        self.add_argument("sample_id",          is_required=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=1)
        self.add_argument("mem",                is_required=True, default_value=1)

    def define_output(self):
        bam         = self.get_argument("bam")
        bam_idx     = self.get_argument("bam_idx")
        sample_id   = self.get_argument("sample_id")

        for id1, (bam1, bam_idx1, sample_id1) in enumerate(zip(bam, bam_idx, sample_id)):
            for id2, (bam2, bam_idx2, sample_id2) in enumerate(zip(bam[id1:], bam_idx[id1:], sample_id[id1:])):
                split_name = "{0}_{1}".format(sample_id1, sample_id2)
                self.make_split(split_id=split_name)
                self.add_output(split_id=split_name, key="bam", value=[
                                    self.generate_gapfile(split_name, "bam", bam1, sample_name=sample_id1),
                                    self.generate_gapfile(split_name, "bam", bam2, sample_name=sample_id2)
                                ], is_path=False)
                self.add_output(split_id=split_name, key="bam_idx", value=[
                                    self.generate_gapfile(split_name, "bam_idx", bam_idx1, sample_name=sample_id1),
                                    self.generate_gapfile(split_name, "bam_idx", bam_idx2, sample_name=sample_id2)
                                ], is_path=False)
                self.add_output(split_id=split_name, key="sample_id", value=[sample_id1, sample_id2], is_path=False)

    def define_command(self):
        return None
