from Modules import Merger

# Module created using CC_module_helper.py
class Dellylumpydestructmerger(Merger):
	def __init__(self, module_id, is_docker=False):
		super(Dellylumpydestructmerger, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["all_merged_vcf", "all_merged_cons_vcf", "tmp_bed", "tmp1_bed", "tmp_all_bed"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("delly_merged_vcf",		is_required=True)
		self.add_argument("lumpy_merged_vcf",		is_required=True)
		self.add_argument("destruct_merged_vcf",	is_required=True)
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10.0)
		self.add_argument("chr_filter",				default_value=0)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		all_merged_vcf			= self.generate_unique_file_name("all.merged.vcf")
		all_merged_cons_file	= all_merged_vcf.replace('.vcf', '_cons.vcf')
		tmp_bed					= all_merged_vcf+'.tmp.bed'
		tmp1_bed				= all_merged_vcf+'.tmp1.bed'
		tmp_all_bed				= all_merged_vcf+'.tmp.all.bed'
		self.add_output("all_merged_vcf",			all_merged_vcf)
		self.add_output("all_merged_cons_vcf",		all_merged_cons_vcf)
		self.add_output("tmp_bed",					tmp_bed)
		self.add_output("tmp1_bed",					tmp1_bed)
		self.add_output("tmp_all_bed",				tmp_all_bed)



	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		delly_file				= self.get_argument("delly_merged_vcf")
		lumpy_file				= self.get_argument("lumpy_merged_vcf")
		destruct_file			= self.get_argument("destruct_merged_vcf")
		sample_id				= self.get_argument("sample_id")
		chr_filter				= self.get_argument("chr_filter")

		# get output
		all_merged_cons_vcf		= self.get_output("all_merged_vcf")

		# add module
		cmd = " python Merge_delly_lumpy_destruct.py"

		# edit sample_id list into one item
		sample_id = '?'.join(sample_id)

		# add arguments
		cmd += " {0} {1} {2}".format(all_merged_vcf, sample_id, chr_filter)

		# add merged vcf files
		cmd += " {0} {1} {2}".format(delly_file, lumpy_file, destruct_file)

		# add logging
		cmd += " !LOG3!"

		return cmd