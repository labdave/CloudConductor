from Modules import Module

# Module created using CC_module_helper.py
class Filter_chrom_reads(Module):
	def __init__(self, module_id, is_docker=False):
		super(Filter_chrom_reads, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["filtered_bam", "filtered_bam_bai"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",			is_required=True)
		self.add_argument("nr_cpus",		default_value=2)
		self.add_argument("mem",			default_value=10.0)
		self.add_argument("F",				default_value=1294)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		filtered_bam			= self.generate_unique_file_name(".filtered.bam")
		self.add_output("filtered_bam",		filtered_bam)
		self.add_output("filtered_bam_bai",	filtered_bam+'.bai')


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		bam						= self.get_argument("bam")
		threads					= self.get_argument("nr_cpus")
		F						= self.get_argument("F")

		# get output
		filtered_bam			= self.get_output("filtered_bam")

		# add module
		cmd = "bash /usr/local/bin/filter_chrom_reads.sh"

		# add arguments
		cmd += " {0} {1} {2} {3}".format(
			bam, threads, F, filtered_bam)

		# add logging
		cmd += " !LOG3!"

		return cmd