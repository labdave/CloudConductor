from Modules import Merger

class Dndscv(Merger):
        def __init__(self, module_id, is_docker=False):
                super(Dndscv, self).__init__(module_id, is_docker)
                self.output_keys = ["dndscv_in","dndscv_out"]


        def define_input(self):
                self.add_argument("run_dndscv",is_resource=True, is_required=True)
                self.add_argument("nr_cpus",    default_value=2)
                self.add_argument("mem",        default_value=8.0)
                self.add_argument("-o",         default_value="dndscv_file")
                self.add_argument("ref",         is_resource=True, is_required=True)
                self.add_argument("vcf_txt", is_required=True)
                self.add_argument("-h",        default_value=False)


        def define_output(self):
                # Combine output prefix with output directory 
                self.output_path = "{0}/{1}".format(self.output_dir,self.get_argument("-o"))
                name_in = "{0}.input.tsv".format(self.output_path)
                name_out = "{0}.tsv".format(self.output_path)
                self.add_output("dndscv_in",name_in)
                self.add_output("dndscv_out",name_out)

        def define_command(self):
                run_dndscv      = self.get_argument("run_dndscv")
                ref             = self.get_argument("ref")
                file_list       = self.get_argument("vcf_txt")
                _o                = "{0}/{1}".format(self.output_dir,self.get_argument("-o"))
                _h                = self.get_argument("-h")

                # Change file_list into string
                file_string = ",".join(file_list)

                cmd = "Rscript {0} -o {1} -r {2} -f {3}".format(run_dndscv, _o, ref, file_string)
                return cmd
