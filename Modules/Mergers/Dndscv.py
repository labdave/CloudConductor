from Modules import Merger

class Dndscv(Merger):
        def __init__(self, module_id, is_docker=False):
                super(Dndscv, self).__init__(module_id, is_docker)
                self.output_keys = ["dndscv_in","dndscv_out"]


        def define_input(self):
                self.add_argument("dndscv",     is_resource=True, is_required=True)
                self.add_argument("ref",        is_resource=True, is_required=True)
                self.add_argument("recoded_vcf",    is_required=True)
                self.add_argument("out_file",   default_value="dndscv_file")
                self.add_argument("nr_cpus",    default_value=4)
                self.add_argument("mem",        default_value=26)


        def define_output(self):
                # Combine output prefix with output directory
                out_file        = self.get_argument("out_file")
                output_path     = "{0}/{1}".format(self.output_dir,out_file)

                # generate output file names
                name_in         = "{0}.input.tsv".format(output_path)
                name_out        = "{0}.tsv".format(output_path)

                self.add_output("dndscv_in",name_in)
                self.add_output("dndscv_out",name_out)

        def define_command(self):
                dndscv          = self.get_argument("dndscv")
                ref             = self.get_argument("ref")
                recoded_vcfs    = self.get_argument("recoded_vcf")
                out_file        = self.get_argument("out_file")

                out_file = "{0}/{1}".format(self.output_dir,out_file)

                # Change into string
                file_string = ",".join(recoded_vcfs)

                if not self.is_docker:
                        cmd = "sudo Rscript {0} -o {1} -r {2} -f {3}".format(dndscv, out_file, ref, file_string)
                else:
                        cmd = "Rscript {0} -o {1} -r {2} -f {3}".format(dndscv, out_file, ref, file_string)

                return cmd
