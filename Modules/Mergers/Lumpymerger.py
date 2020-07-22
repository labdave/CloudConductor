from Modules import Merger

# Module created using CC_module_helper.py
class Lumpymerger(Merger):
	def __init__(self, module_id, is_docker=False):
		super(Lumpymerger, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["merged_vcf"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("gt_vcf",					is_required=True)
		self.add_argument("sample_name",				is_required=True)
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10.0)
		self.add_argument("chr_switch",				default_value=0)
		self.add_argument("chr_filter",				default_value=0)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation

		# FILE NAME IN ALL MERGERS DEPENDS ON THIS: CHANGE WITH CAUTION
		lumpy_merged_vcf		= self.generate_unique_file_name("lumpy.merged.vcf")
		self.add_output("merged_vcf",		lumpy_merged_vcf)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		vcf_list				= self.get_argument("gt_vcf")
		sample_name				= self.get_argument("sample_name")
		chr_switch				= self.get_argument("chr_switch")
		chr_filter				= self.get_argument("chr_filter")

		# get output
		lumpy_merged_vcf		= self.get_output("merged_vcf")

		# add module
		cmd = " python Merge_sample_level_Lumpy.py"

		# edit sample_name list into one item
		if isinstance(sample_name, str):
			sample_name = ''.join(sample_name)
		else:
			sample_name = '?'.join(sample_name)

		# add arguments
		cmd += " {0} {1} {2} {3}".format(
			lumpy_merged_vcf, sample_name, chr_switch, chr_filter)

		# add vcf files
		if isinstance(vcf_list, str):
			cmd += " {}".format(''.join(vcf_list))
		else:
			for vcf in vcf_list:
				cmd += " {}".format(vcf)

		# add logging
		cmd += " !LOG3!"

		return cmd