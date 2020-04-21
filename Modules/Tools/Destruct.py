from Modules import Module

# Module created using CC_module_helper.py
class Destruct(Module):
	def __init__(self, module_id, is_docker=False):
		super(Destruct, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["breaks", "break_libs", "break_reads"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",			is_required=True)
		self.add_argument("nr_cpus",		default_value=2)
		self.add_argument("mem",			default_value=10.0)
		self.add_argument("destruct",		is_required=True, is_resource=True)
		self.add_argument("lib_ids",		default_value="sample")
		self.add_argument("submit",			default_value="local")

	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		breaks					= self.generate_unique_file_name("breaks.tsv")
		break_libs				= self.generate_unique_file_name("break_libs.tsv")
		break_reads				= self.generate_unique_file_name("break_reads.tsv")
		self.add_output("breaks",			breaks)
		self.add_output("break_libs",		break_libs)
		self.add_output("break_reads",		break_reads)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		bam						= self.get_argument("bam")
		destruct				= self.get_argument("destruct")
		lib_ids					= self.get_argument("lib_ids")
		submit					= self.get_argument("submit")
		
		# get output
		breaks					= self.get_output("breaks")
		break_libs				= self.get_output("break_libs")
		break_reads				= self.get_output("break_reads")

		# add module
		cmd = destruct

		# add arguments
		cmd += " run /usr/local/bin/destruct_ref/ {0} {1} {2} --bam_files {3}".format(
			breaks, break_libs, break_reads, bam)

		cmd += " --lib_ids {0} --submit {1}".format(lib_ids, submit)

		# add logging
		cmd += " !LOG3!"

		return cmd