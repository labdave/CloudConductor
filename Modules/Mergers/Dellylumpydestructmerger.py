from Modules import Merger

# Module created using CC_module_helper.py
class Dellylumpydestructmerger(Merger):
	def __init__(self, module_id, is_docker=False):
		super(Dellylumpydestructmerger, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["all_merged_vcf", "all_merged_cons_vcf"] 


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("merged_vcf",				is_required=True)
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10.0)
		self.add_argument("chr_filter",				default_value=0)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		all_merged_vcf			= self.generate_unique_file_name("all.merged.vcf")
		all_merged_cons_vcf		= all_merged_vcf.replace('.vcf', '_cons.vcf')
		self.add_output("all_merged_vcf",			all_merged_vcf)
		self.add_output("all_merged_cons_vcf",		all_merged_cons_vcf)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		merged_vcf_list			= self.get_argument("merged_vcf")
		chr_filter				= self.get_argument("chr_filter")

		# DEPENDS ON INTEGRITY OF FILE NAMES FROM INDIVIDUAL MERGERS
		for vcf in merged_vcf_list:
			if 'lumpy.merged' in vcf:
				lumpy_file = vcf
				continue
			if 'delly.merged' in vcf:
				delly_file = vcf
				continue

		# get output
		all_merged_vcf			= self.get_output("all_merged_vcf")

		# add module
		cmd = " python3 Merge_delly_lumpy.py"

		# add arguments
		cmd += " {0} {1}".format(all_merged_vcf, chr_filter)

		# add merged vcf files
		cmd += " {0} {1} {2}".format(delly_file, lumpy_file, destruct_file)

		# add logging
		cmd += " !LOG3!"

		return cmd