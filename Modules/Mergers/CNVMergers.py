from Modules import Merger

class AggregateCNVSegments(Merger):
    def __init__(self, module_id, is_docker = False):
        super(AggregateCNVSegments, self).__init__(module_id, is_docker)
        self.output_keys    = ["gene_seg", "cyto_seg"]

    def define_input(self):
        self.add_argument("seg_call",       is_required=True)
        self.add_argument("gene_bed",       is_resource=True)
        self.add_argument("cyto_bed",       is_resource=True)
        self.add_argument("mem",            default_value=10)
        self.add_argument("nr_cpus",        default_value=2)

    def define_output(self):
        gene_seg_file       = self.generate_unique_file_name(extension=".gene.seg")
        self.add_output("gene_seg", gene_seg_file)
        cyto_seg_file       = self.generate_unique_file_name(extension=".cyto.seg")
        self.add_output("cyto_seg", cyto_seg_file)

    def define_command(self):
        segs                = self.get_argument("seg_call")
        gene_bed            = self.get_argument("gene_bed")
        cyto_bed            = self.get_argument("cyto_bed")

        gene_seg            = self.get_output("gene_seg")
        cyto_seg            = self.get_output("cyto_seg")

        # if there's only one sample, make it a list
        if not isinstance(segs, list):
            seg = [segs]

        cmd = ""
        # cmd += "python get_gene_cn.py {}".format(gene_bed)
        # for seg in segs:
        #     cmd += " {}".format(seg)
        # cmd += " !LOG3!;"

        # cmd += "python get_cyto_cn.py {}".format(cyto_bed)
        # for seg in segs:
        #     cmd += " {}".format(seg)
        # cmd += " !LOG3!"

        cmd += "ls -l !LOG3!; ls -l /data/ !LOG3!"
        return cmd


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
