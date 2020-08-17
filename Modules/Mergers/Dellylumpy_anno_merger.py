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
		self.add_argument("dac_gap_blacklist",		is_resource=True)
		self.add_argument("repeat_blacklist",		is_resource=True)
		self.add_argument("segmental_blacklist",	is_resource=True)
		self.add_argument("level1_bp",				is_resource=True)
		self.add_argument("gtf",					is_resource=True)
		self.add_argument("chr_names",				is_resource=True)
		self.add_argument("paper_freq_pairs",		is_resource=True)
		self.add_argument("translocation_table",	is_required=True)
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10.0)
		self.add_argument("chr_filter",				default_value=0)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		anno_vcf						= self.generate_unique_file_name("anno.vcf")
		self.add_output("anno_vcf",					anno_vcf)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		all_merged_cons_vcf				= self.get_argument("all_merged_cons_vcf")
		bed								= self.get_argument("bed")
		dac_gap_blacklist				= self.get_argument("dac_gap_blacklist")
		repeat_blacklist				= self.get_argument("repeat_blacklist")
		segmental_blacklist				= self.get_argument("segmental_blacklist")
		level1_bp						= self.get_argument("level1_bp")
		gtf								= self.get_argument("gtf")
		chr_names						= self.get_argument("chr_names")
		paper_freq_pairs				= self.get_argument("paper_freq_pairs")
		chr_filter						= self.get_argument("chr_filter")
		translocation_table				= self.get_argument("translocation_table")

		# get output
		anno_vcf						= self.get_output("anno_vcf")

		# add module
		cmd = " python3 Merge_delly_lumpy_destruct_annotation.py"

		# add arguments
		cmd += " -i {0} -c {1} -o {2}".format(all_merged_cons_vcf, bed, anno_vcf)
		cmd += " -t /data/tmp_folder -g {0} -r {1}".format(dac_gap_blacklist, repeat_blacklist)
		cmd += " -l {0} -G {1} -C {2}".format(level1_bp, gtf, chr_names)
		cmd += " -p {0} -F {1} -s {2}".format(paper_freq_pairs, chr_filter, segmental_blacklist)
		cmd += " -D {0}".format(translocation_table)

		# add logging
		cmd += " !LOG3!"

		return cmd
