from Modules import Merger

# Module created using CC_module_helper.py
class Purity(Merger):
	def __init__(self, module_id, is_docker=False):
		super(Purity, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys 				= ["purity_estimate"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("wl_variants",		is_required=True)
		self.add_argument("nr_cpus",			default_value=1)
		self.add_argument("mem",				default_value=5)
		self.add_argument("min_dpmax", 			default_value=5)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		purity							= self.generate_unique_file_name("purity.tsv")
		self.add_output("purity_estimate",		purity)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		wl_variants						= self.get_argument("wl_variants")
		min_dpmax 						= self.get_argument("min_dpmax")

		# get output
		purity_estimate					= self.get_output("purity_estimate")

		# add arguments
		cmd = "python3 new_format_parser.py {0} {1} {2}".format(
			wl_variants, purity_estimate, min_dpmax)

		# add logging
		cmd += " !LOG3!"

		return cmd
