from Modules import Module

# Module created using CC_module_helper.py
class Lumpy(Module):
	def __init__(self, module_id, is_docker=False):
		super(Lumpy, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["lumpy_vcf", "gt_vcf"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",						is_required=True)
		self.add_argument("nr_cpus",					default_value=2)
		self.add_argument("mem",						default_value=10.0)
		self.add_argument("read_length",				default_value=152)
		self.add_argument("discordant_z",				default_value=5)
		self.add_argument("back_distance",				default_value=10)
		self.add_argument("weight",						default_value=1)
		self.add_argument("min_mapping_threshold",		default_value=20)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		lumpy_vcf		= self.generate_unique_file_name("lumpy.vcf")
		gt_vcf 			= self.generate_unique_file_name("gt.vcf")
		self.add_output("lumpy_vcf",		lumpy_vcf)
		self.add_output("gt_vcf",			gt_vcf)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		bam						= self.get_argument("bam")
		read_length				= self.get_argument("read_length")
		discordant_z			= self.get_argument("discordant_z")
		back_distance			= self.get_argument("back_distance")
		weight					= self.get_argument("weight")
		min_mapping_threshold	= self.get_argument("min_mapping_threshold")

		# get output
		lumpy_vcf				= self.get_output("lumpy_vcf")
		gt_vcf					= self.get_output("gt_vcf")

		# add module
		cmd = "bash /usr/local/bin/lumpy.sh"

		# add arguments
		cmd += " {0} {1} {2} {3} {4} {5}".format(
			bam, read_length, discordant_z, back_distance, weight,
			min_mapping_threshold, lumpy_vcf, gt_vcf)

		# add logging
		cmd += " !LOG3!"

		return cmd