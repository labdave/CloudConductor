from Modules import Module

# Module created using CC_module_helper.py
class CNV_Postprocess(Module):
	def __init__(self, module_id, is_docker=False):
		super(CNV_Postprocess, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys 		= ["norm_seg"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",		is_required=True)
		self.add_argument("nr_cpus",	default_value=1)
		self.add_argument("mem",		default_value=4)
		self.add_argument("sample_id")
		self.add_argument("seg_call")


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		sample_id				= self.get_argument("sample_id")
		norm_seg				= self.generate_unique_file_name(".{}.norm.seg".format(sample_id))
		self.add_output("norm_seg",		norm_seg)

	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		seg						= self.get_argument("seg_call")

		# get output
		norm_seg				= self.get_output("norm_seg")

		# add command
		cmd = "python3 normalize.py {0} {1}".format(seg, norm_seg)

		# add logging
		cmd += " !LOG3!"

		return cmd