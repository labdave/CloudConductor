from Modules import Module

class Delly(Module):
    def __init__(self, module_id, is_docker = False):
        super(Delly, self).__init__(module_id, is_docker)
        self.output_keys = ["delly_bcf", "delly_csi"]

    def define_input(self):
        self.add_argument("bam",            is_required=True)
        self.add_argument("delly",          is_required=True, is_resource=True)
        self.add_argument("is_tumor",       is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("exclude_list",   is_resource=True)
        self.add_argument("nr_cpus",        is_required=True, default_value=2)
        self.add_argument("mem",            is_required=True, default_value=13)

    def define_output(self):

        # Declare unique file name for bcf file
        bcf_file = self.generate_unique_file_name("delly.bcf")
        self.add_output("delly_bcf", bcf_file)

        # Declare the path to the csi index file
        csi_file = bcf_file + ".csi"
        self.add_output("delly_csi", csi_file)

    def define_command(self):

        # Get arguments to run Delly
        bam             = self.get_argument("bam")
        is_tumor        = self.get_argument("is_tumor")
        ref             = self.get_argument("ref")
        exclude_list    = self.get_argument("exclude_list")
        delly           = self.get_argument("delly")

        # Get output paths
        bcf             = self.get_output("delly_bcf")

        libs_path = None

        if not self.is_docker:
            # Get libraries path
            libs_path = "{0}/modular-boost/stage/lib/:{0}/htslib/".format(delly.rsplit("/",1)[0])

        # Identify the tumor and the normal
        if is_tumor[0]:
            tumor = bam[0]
            normal = bam[1]
        else:
            tumor = bam[1]
            normal = bam[0]

        # Generate command
        if not self.is_docker:
            if exclude_list:
                cmd = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{0} ; {1} call -x {2} -g {3} -o {4} {5} {6} !LOG3!".format\
                    (libs_path, delly, exclude_list, ref, bcf, tumor, normal)
            else:
                cmd = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{0} ; {1} call -g {2} -o {3} {4} {5} !LOG3!".format\
                    (libs_path, delly, ref, bcf, tumor, normal)
        else:
            if exclude_list:
                cmd = "{0} call -x {1} -g {2} -o {3} {4} {5} !LOG3!".format(delly, exclude_list, ref, bcf, tumor, normal)
            else:
                cmd = "{0} call -g {1} -o {2} {3} {4} !LOG3!".format(delly, ref, bcf, tumor, normal)

        return cmd
