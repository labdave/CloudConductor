from Modules import Merger

#debug
# Module created using CC_module_helper.py
class Dellylumpy_anno_merger(Merger):
	def __init__(self, module_id, is_docker=False):
		super(Dellylumpy_anno_merger, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["anno_vcf"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("all_merged_cons_vcf",	is_required=True)
		self.add_argument("bed",					is_resource=True)
		self.add_argument("translocation_table",	is_required=True)
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10.0)

	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		anno_vcf = self.generate_unique_file_name("anno.tsv")
		self.add_output("anno_vcf", anno_vcf)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		all_merged_cons_vcf				= self.get_argument("all_merged_cons_vcf")
		translocation_table				= self.get_argument("translocation_table")

		# get output
		anno_vcf						= self.get_output("anno_vcf")

		# add script
		cmd = "python3 merge_D_L_with_DW.py"

		# add arguments
		cmd += " -dl {0}".format(all_merged_cons_vcf)
		cmd += " -dw {0}".format(translocation_table)
		cmd += " -out {0}".format(anno_vcf)

		# add logging
		cmd += " !LOG3!"

		return cmd
