# paste stuff here
from Modules import Module

# Module created using CC_module_helper.py
class EBV_detection(Module):
    def __init__(self, module_id, is_docker=False):
        super(EBV_detection, self).__init__(module_id, is_docker)
        # Add output keys here if needed
        self.output_keys = ["paired_ebv_sam", "single_ebv_sam"]


    def define_input(self):
        # Module creator needs to define which arguments have is_resource=True
        # Module creator needs to rename arguments as required by CC
        self.add_argument("bam",                        is_required=True)
        self.add_argument("ref_masked_ebv",             is_required=True, is_resource=True)
        self.add_argument("nr_cpus",                    default_value=8)
        self.add_argument("mem",                        default_value=20.0)
        self.add_argument("f",                          default_value=4)
        self.add_argument("F",                          default_value=1024)
        self.add_argument("outFilterMismatchNmax",      default_value=5)
        self.add_argument("outFilterMultimapNmax",      default_value=10)
        self.add_argument("limitOutSAMoneReadBytes",    default_value=1000000)


    def define_output(self):
        # Module creator needs to define what the outputs are
        # based on the output keys provided during module creation
        paired_ebv_sam  = self.generate_unique_file_name("ebv_paired_Aligned.out.sam")
        single_ebv_sam  = self.generate_unique_file_name("ebv_single_Aligned.out.sam")
        #log_file        
        self.add_output("paired_ebv_sam",       paired_ebv_sam)
        self.add_output("single_ebv_sam",       single_ebv_sam)


    def define_command(self):
        # Module creator needs to use renamed arguments as required by CC
        bam                     = self.get_argument("bam")
        ref_masked_ebv          = self.get_argument("ref_masked_ebv")
        nr_cpus                 = self.get_argument("nr_cpus")
        f                       = self.get_argument("f")
        F                       = self.get_argument("F")
        outFilterMismatchNmax   = self.get_argument("outFilterMismatchNmax")
        outFilterMultimapNmax   = self.get_argument("outFilterMultimapNmax")
        limitOutSAMoneReadBytes = self.get_argument("limitOutSAMoneReadBytes")
        

        # get output
        paired_ebv_sam                  = self.get_output("paired_ebv_sam").replace("_Aligned.out.sam", "")
        single_ebv_sam                  = self.get_output("single_ebv_sam").replace("_Aligned.out.sam", "")

        # add module
        cmd = "bash /usr/local/bin/ebv_detection.sh"

        # add arguments
        cmd += " {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}".format(
            bam, ref_masked_ebv, nr_cpus,
            f, F, paired_ebv_sam, single_ebv_sam, outFilterMismatchNmax, outFilterMultimapNmax, limitOutSAMoneReadBytes)

        # add logging verbosity
        cmd += " !LOG3!"

        return cmd