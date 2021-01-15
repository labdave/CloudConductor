from Modules import Merger

class AggregateCNVSegments(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateCNVSegments, self).__init__(module_id, is_docker)
        self.output_keys    = ["gene_seg", "cyto_seg"]

    def define_input(self):
        self.add_argument("norm_seg",       is_required=True)
        self.add_argument("sample_id",      is_required=True)
        self.add_argument("gene_bed",       is_resource=True)
        self.add_argument("cyto_bed",       is_resource=True)
        self.add_argument("mem",            default_value=10)
        self.add_argument("nr_cpus",        default_value=2)

    def define_output(self):
        gene_seg_file       = self.generate_unique_file_name(extension=".gene.csv")
        self.add_output("gene_seg", gene_seg_file)
        cyto_seg_file       = self.generate_unique_file_name(extension=".cyto.csv")
        self.add_output("cyto_seg", cyto_seg_file)

    def define_command(self):
        segs                = self.get_argument("norm_seg")
        samples             = self.get_argument("sample_id")
        gene_bed            = self.get_argument("gene_bed")
        cyto_bed            = self.get_argument("cyto_bed")

        gene_seg            = self.get_output("gene_seg")
        cyto_seg            = self.get_output("cyto_seg")

        join_gene_seg       = self.generate_unique_file_name(".join_gene_seg")
        join_cyto_seg       = self.generate_unique_file_name(".join_cyto_seg")
        join_sample         = self.generate_unique_file_name(".join_sample")

        cmd1, cmd2, cmd3, cmd4, cmd5, cmd6, cmd7 = "", "", "", "", "", "", ""

        # if there's only one sample, make it a list
        if not isinstance(segs, list):
            seg = [segs]
            samples = [samples]

        # need to parse and intersect the seg files
        for seg in segs:
            filtered_seg = seg.replace("norm.seg", "filtered.seg")
            gene_intersect_seg = seg.replace("norm.seg", "gene_intersect.seg")
            cyto_intersect_seg = seg.replace("norm.seg", "cyto_intersect.seg")
            cmd1 += "grep -v '@' {0} | grep -v 'CONTIG' > {1}; ".format(seg, filtered_seg)
            cmd2 += "bedtools intersect -loj -a {0} -b {1} > {2}; ".format(gene_bed, filtered_seg, gene_intersect_seg)
            cmd3 += "bedtools intersect -loj -a {0} -b {1} > {2}; ".format(cyto_bed, filtered_seg, cyto_intersect_seg)

        # command becomes too long, need to store data in a file
        for seg in segs:
            gene_intersect_seg = seg.replace("norm.seg", "gene_intersect.seg")
            cyto_intersect_seg = seg.replace("norm.seg", "cyto_intersect.seg")
            cmd3 += "echo {0} >> {1}; ".format(gene_intersect_seg, join_gene_seg)
            cmd4 += "echo {0} >> {1}; ".format(cyto_intersect_seg, join_cyto_seg)

        for sample in samples:
            cmd5 += "echo {0} >> {1}; ".format(sample, join_sample)

        cmd6 += "python get_gene_cn.py {0} {1} {2} {3} !LOG3!; ".format(gene_bed, join_gene_seg, join_sample, gene_seg)
        cmd7 += "python get_cyto_cn.py {0} {1} {2} {3} !LOG3!; ".format(cyto_bed, join_cyto_seg, join_sample, cyto_seg)

        return [cmd1, cmd2, cmd3, cmd4, cmd5, cmd6, cmd7]


class MakeCNVPoN(Merger):
    def __init__(self, module_id, is_docker = False):
        super(MakeCNVPoN, self).__init__(module_id, is_docker)
        self.output_keys    = ["ref_cnn"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("bam_idx",        is_required=True)
        self.add_argument("pooled_normal",  is_required=True)
        self.add_argument("cnvkit",         is_required=True, is_resource=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("targets",        is_required=True, is_resource=True)
        self.add_argument("access",         is_required=True, is_resource=True)
        self.add_argument("method",         is_required=True, default_value="hybrid")
        self.add_argument("nr_cpus",        is_required=True, default_value=32)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        ref_cnn_file = self.generate_unique_file_name(extension=".ref.cnn")
        self.add_output("ref_cnn", ref_cnn_file)

    def define_command(self):

        # Get arguments
        bams            = self.get_argument("bam")
        cnvkit          = self.get_argument("cnvkit")
        pooled_normals  = self.get_argument("pooled_normal")
        ref             = self.get_argument("ref")
        targets         = self.get_argument("targets")
        access          = self.get_argument("access")
        method          = self.get_argument("method")
        nr_cpus         = self.get_argument("nr_cpus")

        #get the filename which store Panel of Normal ref cnn
        ref_cnn = self.get_output("ref_cnn")

        #join cns file names with space delimiter
        pooled_normal_bams = []
        for i in range(len(bams)):
            if pooled_normals[i]:
                pooled_normal_bams.append(bams[i])
        bams = " ".join(pooled_normal_bams)

        # Get current working dir
        working_dir = self.get_output_dir()

        # generate command line for cnvkit for hybrid (WES) method
        if method == "hybrid":
            cmd = "{0} batch --normal {1} --targets {2} --fasta {3} --access {4} " \
                  "--output-reference {5} --output-dir {6} -p {7}".format(cnvkit, bams, targets, ref, access, ref_cnn,
                                                                   working_dir, nr_cpus)

        # generate command line for cnvkit for WGS method
        elif method == "wgs":
            cmd = "{0} batch --normal {1} --targets {2} --fasta {3} --access {4} " \
                  "--output-reference {5} --output-dir {6} -p {7} --method {8}".format(cnvkit, bams, targets, ref,
                                                                                       access, ref_cnn, working_dir,
                                                                                       nr_cpus, method)

        else:
            raise NotImplementedError("Method {0} is not implemented in CNVKit module.".format(method))

        cmd = "{0} !LOG3!".format(cmd)

        return cmd


class CNVkitExport(Merger):
    def __init__(self, module_id, is_docker = False):
        super(CNVkitExport, self).__init__(module_id, is_docker)
        self.output_keys    = ["export"]

    def define_input(self):
        self.add_argument("cns",            is_required=True)
        self.add_argument("cnvkit",         is_required=True, is_resource=True)
        self.add_argument("export_type",    is_required=True, default_value="seg")
        self.add_argument("nr_cpus",        is_required=True, default_value=8)
        self.add_argument("mem",            is_required=True, default_value="nr_cpus * 2")

    def define_output(self):

        #generate unique name for reference bin file generated by CNVKit
        export_type = self.get_argument("export_type")

        if export_type == "seg":
            export_file_name = self.generate_unique_file_name(extension=".seg.txt")
        elif export_type == "bed":
            export_file_name = self.generate_unique_file_name(extension=".cnv.bed")
        elif export_type == "vcf":
            export_file_name = self.generate_unique_file_name(extension=".cnv.vcf")
        elif export_type == "cdt":
            export_file_name = self.generate_unique_file_name(extension=".cnv.cdt")
        elif export_type == "jtv":
            export_file_name = self.generate_unique_file_name(extension=".cnv.jtv.txt")
        elif export_type == "theta":
            export_file_name = self.generate_unique_file_name(extension=".theta2.interval_count")
        else:
            raise NotImplementedError("Export method {0} is not supported in CNVKit".format(export_type))

        self.add_output("export", export_file_name)

    def define_command(self):

        # Get arguments
        cns         = self.get_argument("cns")
        cnvkit      = self.get_argument("cnvkit")
        export_type = self.get_argument("export_type")

        #get the filename which store segmentation values
        export_file_name = self.get_output("export")

        #join cns file names with space delimiter
        cns = " ".join(cns)

        #command line to export CNVkit CNS to GISTIC2 seg format
        cmd = "{0} export {1} {2} -o {3} !LOG3!".format(cnvkit, export_type, cns, export_file_name)

        return cmd
