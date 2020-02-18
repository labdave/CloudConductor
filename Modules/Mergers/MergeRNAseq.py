import os
import re
from Modules import Merger

def generate_sample_sheet_cmd(sample_names, sample_files, outfile, in_type=None):
    # list of cmds
    cmds = list()

    # make a list of one sample if theere is only one sample in the analysis
    if not isinstance(sample_names, list):
        sample_names = [sample_names]
        sample_files = [sample_files]

    # Define the output file as a bash variable
    cmds.append("o={0}".format(outfile))

    # Define the containing directory of the input files
    cmds.append("i={0}".format(os.path.dirname(sample_files[0])))

    #iterate through all the samples to create a sample info file for Rscript
    for index in range(len(sample_names)):
        if index == 0:
            if in_type == "cuffquant":
                cmds.append('echo -e "sample_id\\tgroup_label" > $o')
            else:
                cmds.append('echo -e "samples\\tfiles" > $o')
        if in_type == "cuffquant":
            cmds.append('echo -e "$i/{0}\\t{1}" >> $o'.format(os.path.basename(sample_files[index]), sample_names[index]))
        else:
            cmds.append('echo -e "{0}\\t$i/{1}" >> $o'.format(sample_names[index], os.path.basename(sample_files[index])))
    return " ; ".join(cmds)

def generate_sample_diease_cmd(names, diagnosis, outfile):

    # list of cmds
    cmds = list()

    # make a list of one sample if theere is only one sample in the analysis
    if not isinstance(names, list):
        names = [names]
        diagnosis = [diagnosis]

    # Define the output file as a bash variable
    cmds.append("o={0}".format(outfile))

    # Define the containing directory of the input files
    # cmds.append("i={0}".format(os.path.dirname(outfile)))

    #iterate through all the samples to create a sample info file for Rscript
    for index in range(len(names)):
        if index == 0:
            cmds.append('echo -e "sample\\tdisease" > $o')
            cmds.append('echo -e "{0}\\t{1}" >> $o'.format(names[index], diagnosis[index]))
        else:
            cmds.append('echo -e "{0}\\t{1}" >> $o'.format(names[index], diagnosis[index]))
    return " ; ".join(cmds)

class AggregateRawReadCounts(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateRawReadCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["expression_file"]

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("raw_read_counts",    is_required=True)
        self.add_argument("aggregate",          is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=2)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare unique file name
        output_file_name = self.generate_unique_file_name(extension=".txt")

        self.add_output("expression_file", output_file_name)

    def define_command(self):

        # Get arguments
        samples             = self.get_argument("sample_name")
        raw_read_counts     = self.get_argument("raw_read_counts")

        #get the aggregate script to run
        aggregate_script    = self.get_argument("aggregate")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate output file name prefix for STAR
        sample_sheet_file = os.path.join(working_dir, "{0}".format("sample_info.txt"))

        #get the output file and make appropriate path for it
        output_file = self.get_output("expression_file")

        # generate command line for Rscript
        mk_sample_sheet_cmd = generate_sample_sheet_cmd(samples, raw_read_counts, sample_sheet_file)

        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script, sample_sheet_file, output_file)
        else:
            cmd = "Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script, sample_sheet_file,
                                                                           output_file)
        return "{0} ; {1}".format(mk_sample_sheet_cmd, cmd)

class AggregateRSEMResults(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateRSEMResults, self).__init__(module_id, is_docker)
        self.output_keys = ["isoform_expression_matrix", "isoform_expression_gene_metadata",
                            "expression_file", "gene_expression_gene_metadata"]

    def define_input(self):
        self.add_argument("sample_name",                    is_required=True)
        self.add_argument("isoforms_results",               is_required=True)
        self.add_argument("genes_results",                  is_required=True)
        self.add_argument("aggregate_rsem_results_script",  is_required=True, is_resource=True)
        self.add_argument("count_type",                     is_required=True, default_value="fpkm")
        self.add_argument("nr_cpus",                        is_required=True, default_value=8)
        self.add_argument("mem",                            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        count_type = self.get_argument("count_type").upper()

        # Declare unique file name
        isoform_expression_file_name                = self.generate_unique_file_name(extension=".isoform_expression_matrix.{0}.txt".format(count_type))
        gene_expression_file_name                   = self.generate_unique_file_name(extension=".gene_expression_matrix.{0}.txt".format(count_type))
        isoform_expression_gene_metadata_file_name  = self.generate_unique_file_name(extension=".isoform_expression_gene_metadata.{0}.txt".format(count_type))
        gene_expression_gene_metadata_file_name     = self.generate_unique_file_name(extension=".gene_expression_gene_metadata.{0}.txt".format(count_type))

        self.add_output("isoform_expression_matrix", isoform_expression_file_name)
        self.add_output("isoform_expression_gene_metadata", isoform_expression_gene_metadata_file_name)
        self.add_output("expression_file", gene_expression_file_name)
        self.add_output("gene_expression_gene_metadata", gene_expression_gene_metadata_file_name)

    def define_command(self):

        # Get arguments
        samples                 = self.get_argument("sample_name")
        isoforms_results        = self.get_argument("isoforms_results")
        genes_results           = self.get_argument("genes_results")
        count_type              = self.get_argument("count_type")

        #transform count type to all upper case
        count_type = count_type.upper()

        #get the aggregate script to run
        aggregate_script = self.get_argument("aggregate_rsem_results_script")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate output file name prefix for STAR
        isoforms_input_file = os.path.join(working_dir, "{0}".format("isoforms_sample_info.txt"))
        genes_input_file = os.path.join(working_dir, "{0}".format("genes_sample_info.txt"))

        #get the output file and make appropriate path for it
        isoform_expression_file_name                 = self.get_output("isoform_expression_matrix")
        gene_expression_file_name                    = self.get_output("expression_file")
        isoform_expression_gene_metadata_file_name   = self.get_output("isoform_expression_gene_metadata")
        gene_expression_gene_metadata_file_name      = self.get_output("gene_expression_gene_metadata")

        # generate command line for Rscript
        mk_sample_sheet_cmd1 = generate_sample_sheet_cmd(samples, isoforms_results, isoforms_input_file)
        mk_sample_sheet_cmd2 = generate_sample_sheet_cmd(samples, genes_results, genes_input_file)

        # generate command line for Rscript
        # possible values for count_type is TPM/FPKM/EXPECTED_COUNT
        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} -f {1} -e {2} -m {3} -t {4} !LOG3!; " \
                  "sudo Rscript --vanilla {0} -f {5} -e {6} -m {7} -t {4} !LOG3!".format\
                (aggregate_script, isoforms_input_file, isoform_expression_file_name,
                 isoform_expression_gene_metadata_file_name, count_type,
                 genes_input_file, gene_expression_file_name, gene_expression_gene_metadata_file_name)
        else:
            cmd = "Rscript --vanilla {0} -f {1} -e {2} -m {3} -t {4} !LOG3!; " \
                  "Rscript --vanilla {0} -f {5} -e {6} -m {7} -t {4} !LOG3!".format\
                (aggregate_script, isoforms_input_file, isoform_expression_file_name,
                 isoform_expression_gene_metadata_file_name, count_type,
                 genes_input_file, gene_expression_file_name, gene_expression_gene_metadata_file_name)

        return [mk_sample_sheet_cmd1, mk_sample_sheet_cmd2, cmd]

class AggregateNormalizedCounts(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateNormalizedCounts, self).__init__(module_id, is_docker)
        self.output_keys = ["aggregated_normalized_gene_counts", "aggregated_normalized_gene_counts_long",
                            "sample_disease"]

    def define_input(self):
        self.add_argument("sample_name",                    is_required=True)
        self.add_argument("sample_id",                      is_required=True)
        self.add_argument("nickname",                       is_required=True)
        self.add_argument("diagnosis",                      is_required=True)
        self.add_argument("normalized_gene_counts",         is_required=True)
        self.add_argument("aggregate_script",               is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                        is_required=True, default_value=8)
        self.add_argument("mem",                            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        # Declare unique file name
        aggregated_normalized_gene_counts       = self.generate_unique_file_name(extension=".normalized.counts.combined.txt")
        aggregated_normalized_gene_counts_long  = self.generate_unique_file_name(extension=".normalized.counts.combined.long.txt")

        sample_disease = self.generate_unique_file_name(extension=".sample.disease.txt")

        self.add_output("aggregated_normalized_gene_counts", aggregated_normalized_gene_counts)
        self.add_output("aggregated_normalized_gene_counts_long", aggregated_normalized_gene_counts_long)

        self.add_output("sample_disease", sample_disease)

    def define_command(self):

        # Get arguments
        samples                 = self.get_argument("sample_name")
        sample_ids              = self.get_argument("sample_id")
        nickname                = self.get_argument("nickname")
        diagnosis               = self.get_argument("diagnosis")
        normalized_gene_counts  = self.get_argument("normalized_gene_counts")

        diagnosis_pairs = {
            "Diffuse large B cell lymphoma, NOS(Activated B-cell type)": "ABC_DLBCL",
            "Diffuse large B cell lymphoma, NOS(Germinal center B-cell type)": "GCB_DLBCL"
        }

        diagnosis = [diagnosis_pairs[x] for x in diagnosis]

        #get the aggregate script to run
        aggregate_script = self.get_argument("aggregate_script")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate file containing information about the normalized count file with sample information
        normalized_gene_counts_info = os.path.join(working_dir, "{0}".format("normalized_gene_counts.txt"))

        # Generate file containing information about the normalized count file with sample information
        sample_disease = self.get_output("sample_disease")

        #get the output file and make appropriate path for it
        aggregated_normalized_gene_counts = self.get_output("aggregated_normalized_gene_counts")

        # replace space/s with underscore in nickname
        nickname = [re.sub('\s+','_', x) for x in nickname]

        # combine the nicknames with the sample names
        sample_name_nickname = ['_'.join([i,j]) for i, j in zip(samples, nickname)]

        # generate command line for Rscript
        # mk_sample_sheet_cmd = generate_sample_sheet_cmd(sample_ids, normalized_gene_counts, normalized_gene_counts_info)
        mk_sample_sheet_cmd = generate_sample_sheet_cmd(sample_name_nickname, normalized_gene_counts, normalized_gene_counts_info)

        # generate command line to generate file containing sample and associated dignosis/disease
        mk_sample_disease_cmd = generate_sample_diease_cmd(sample_name_nickname, diagnosis, sample_disease.get_path())

        # generate command line for Rscript
        if not self.is_docker:
            cmd = "sudo Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script,
                                                                           normalized_gene_counts_info,
                                                                           aggregated_normalized_gene_counts)
        else:
            cmd = "Rscript --vanilla {0} -f {1} -o {2} !LOG3!".format(aggregate_script, normalized_gene_counts_info,
                                                                      aggregated_normalized_gene_counts)

        return [mk_sample_sheet_cmd, mk_sample_disease_cmd, cmd]


class Cuffnorm(Merger):
    def __init__(self, module_id, is_docker = False):
        super(Cuffnorm, self).__init__(module_id, is_docker)
        self.output_keys = ["expression_file", "genes_count_table", "genes_attr_table"]

    def define_input(self):
        self.add_argument("sample_name",        is_required=True)
        self.add_argument("cuffquant_cxb",      is_required=True)
        self.add_argument("cuffnorm",           is_required=True, is_resource=True)
        self.add_argument("gtf",                is_required=True, is_resource=True)
        self.add_argument("nr_cpus",            is_required=True, default_value=32)
        self.add_argument("mem",                is_required=True, default_value="nr_cpus * 6")

    def define_output(self):

        expr_file   = os.path.join(self.get_output_dir(), "genes.fpkm_table")
        count_file  = os.path.join(self.get_output_dir(), "genes.count_table")
        attr_file   = os.path.join(self.get_output_dir(), "genes.attr_table")
        self.add_output("expression_file", expr_file)
        self.add_output("genes_count_table", count_file)
        self.add_output("genes_attr_table", attr_file)

    def define_command(self):

        # Get arguments
        samples             = self.get_argument("sample_name")
        cuffquant_cxbs      = self.get_argument("cuffquant_cxb")
        cuffnorm            = self.get_argument("cuffnorm")
        gtf                 = self.get_argument("gtf")
        nr_cpus             = self.get_argument("nr_cpus")

        # Get current working dir
        working_dir = self.get_output_dir()

        # Generate output file name prefix for STAR
        sample_sheet = os.path.join(working_dir, "{0}".format("cuffnorm_sample_sheet.txt"))

        # generate command line for Rscript
        mk_sample_sheet_cmd = generate_sample_sheet_cmd(samples, cuffquant_cxbs, sample_sheet, in_type="cuffquant")

        #generate command line for Rscript
        cmd = "{0} --no-update-check -v -p {1} -o {2} --use-sample-sheet {3} {4} !LOG3!".format(cuffnorm, nr_cpus, working_dir, gtf, sample_sheet)

        return "{0} ; {1}".format(mk_sample_sheet_cmd, cmd)
