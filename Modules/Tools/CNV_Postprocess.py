from Modules import Module

# Module created using CC_module_helper.py
class CNV_Postprocess(Module):
    def __init__(self, module_id, is_docker=False):
        super(CNV_Postprocess, self).__init__(module_id, is_docker)
        # Add output keys here if needed
        self.output_keys        = ["norm_seg"]


    def define_input(self):
        # Module creator needs to define which arguments have is_resource=True
        # Module creator needs to rename arguments as required by CC
        self.add_argument("nr_cpus",    default_value=1)
        self.add_argument("mem",        default_value=4)
        self.add_argument("global",     is_required=True)
        self.add_argument("sample_id",  is_required=True)
        self.add_argument("seg_call",   is_required=True)


    def define_output(self):
        # Module creator needs to define what the outputs are
        # based on the output keys provided during module creation
        sample_id               = self.get_argument("sample_id")
        norm_seg                = self.generate_unique_file_name(".{}.norm.seg".format(sample_id))
        self.add_output("norm_seg",     norm_seg)

    def define_command(self):
        # Module creator needs to use renamed arguments as required by CC
        seg                     = self.get_argument("seg_call")

        # get output
        norm_seg                = self.get_output("norm_seg")

        # add command
        cmd = "python3 normalize.py {0} {1}".format(seg, norm_seg)

        # add logging
        cmd += " !LOG3!"

        return cmd

class CNV_Aggregate_Global(Module):
    def __init__(self, module_id, is_docker=False):
        super(CNV_Aggregate_Global, self).__init__(module_id, is_docker)
        # Add output keys here if needed
        self.output_keys        = ["cyto_seg", "arm_seg"]


    def define_input(self):
        # Module creator needs to define which arguments have is_resource=True
        # Module creator needs to rename arguments as required by CC
        self.add_argument("nr_cpus",    default_value=1)
        self.add_argument("mem",        default_value=4)
        self.add_argument("sample_id",  is_required=True)
        self.add_argument("norm_seg",   is_required=True)


    def define_output(self):
        # Module creator needs to define what the outputs are
        # based on the output keys provided during module creation
        sample_id               = self.get_argument("sample_id")
        
        cyto_seg                = self.generate_unique_file_name(".{}.cyto.seg".format(sample_id))
        arm_seg                 = self.generate_unique_file_name(".{}.arm.seg".format(sample_id))
        self.add_output("cyto_seg",     cyto_seg)
        self.add_output("arm_seg",      arm_seg)

    def define_command(self):
        # Module creator needs to use renamed arguments as required by CC
        norm_seg                = self.get_argument("norm_seg")

        # get output
        cyto_seg                = self.get_output("cyto_seg")
        arm_seg                 = self.get_output("arm_seg")

        cyto_intersect_seg      = norm_seg.replace("norm.seg", "cyto_intersect.seg")
        arm_intersect_seg       = norm_seg.replace("norm.seg", "arm_intersect.seg")
        sample_id               = self.get_argument("sample_id")
        
        # add command
        cmd = ""
        cmd += "bedtools intersect -loj -a {0} -b {1} > {2} !LOG3!; ".format(cyto_bed, norm_seg, cyto_intersect_seg)
        cmd += "bedtools intersect -loj -a {0} -b {1} > {2} !LOG3!; ".format(arm_bed, norm_seg, arm_intersect_seg)
        cmd += "python3 get_cyto_cn.py {0} {1} {2} {3} !LOG3!; ".format(cyto_bed, cyto_intersect_seg, cyto_seg, sample_id)
        cmd += "python3 get_arm_cn.py {0} {1} {2} {3} !LOG3!".format(arm_bed, arm_intersect_seg, arm_seg, sample_id)

        return cmd

class CNV_Aggregate_Focal(Module):
    def __init__(self, module_id, is_docker=False):
        super(CNV_Aggregate_Focal, self).__init__(module_id, is_docker)
        # Add output keys here if needed
        self.output_keys        = ["gene_seg"]


    def define_input(self):
        # Module creator needs to define which arguments have is_resource=True
        # Module creator needs to rename arguments as required by CC
        self.add_argument("nr_cpus",    default_value=1)
        self.add_argument("mem",        default_value=4)
        self.add_argument("sample_id",  is_required=True)
        self.add_argument("norm_seg",   is_required=True)


    def define_output(self):
        # Module creator needs to define what the outputs are
        # based on the output keys provided during module creation
        sample_id               = self.get_argument("sample_id")
        gene_seg                = self.generate_unique_file_name(".{}.gene.seg".format(sample_id))
        self.add_output("gene_seg",     gene_seg)

    def define_command(self):
        # Module creator needs to use renamed arguments as required by CC
        norm_seg                = self.get_argument("norm_seg")

        # get output
        gene_seg                = self.get_output("gene_seg")

        gene_intersect_seg      = norm_seg.replace("norm.seg", "gene_intersect.seg")
        sample_id               = self.get_argument("sample_id")
        
        # add command
        cmd = ""
        cmd += "bedtools intersect -loj -a {0} -b {1} > {2} !LOG3!; ".format(gene_bed, norm_seg, gene_intersect_seg)
        cmd += "python3 get_gene_cn.py {0} {1} {2} {3} !LOG3!; ".format(gene_bed, gene_intersect_seg, gene_seg, sample_id)

        return cmd

