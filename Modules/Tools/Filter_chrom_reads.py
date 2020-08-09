from Modules import Module

# Module created using CC_module_helper.py
class Filter_chrom_reads(Module):
	def __init__(self, module_id, is_docker=False):
		super(Filter_chrom_reads, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["bam", "bam_idx"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",			is_required=True)
		self.add_argument("nr_cpus",		default_value=8)
		self.add_argument("mem",			default_value=120)
		self.add_argument("F",				default_value=1294)
		self.add_argument("bed",			is_resource=True)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		bam_out					= self.generate_unique_file_name(".filtered.bam")
		self.add_output("bam",				bam_out)
		self.add_output("bam_idx",			bam_out+'.bai')
		self.add_output("temp1_bam",		"/data/output/temp1.bam")
		self.add_output("temp2_bam",		"/data/output/temp2.bam")
		self.add_output("tmp",				"/data/output/tmp")
		self.add_output("tmp1",				"/data/output/tmp1")
		self.add_output("npr",				"/data/output/non-primary.reads.txt")
		self.add_output("otr",				"/data/output/on_target.reads.txt")
		self.add_output("on_target_bam",	"/data/output/on_target.bam")

	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		bam						= self.get_argument("bam")
		threads					= self.get_argument("nr_cpus")
		F						= self.get_argument("F")
		bed						= self.get_argument("bed")

		# get output
		bam_out					= self.get_output("bam")

		# add module
		cmd = "bash /usr/local/bin/filter_chrom_reads.sh"

		# add arguments
		cmd += " {0} {1} {2} {3} {4}".format(
			bam, threads, F, bam_out, bed)

		# add logging
		cmd += " !LOG3!"

		return cmd