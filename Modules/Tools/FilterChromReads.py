from Modules import Module

# Module created using CC_module_helper.py
class FilterChromReads(Module):
	def __init__(self, module_id, is_docker=False):
		super(Filter_chrom_reads, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["bam", "bam_idx"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",			is_required=True)
		self.add_argument("nr_cpus",		default_value=16)
		self.add_argument("mem",			default_value=200)
		self.add_argument("F",				default_value=1294)
		self.add_argument("bed",			is_resource=True)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		bam_out					= self.generate_unique_file_name(".filtered.bam")
		self.add_output("bam",				bam_out)
		self.add_output("bam_idx",			bam_out+'.bai')

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


# Module created using CC_module_helper.py
class FilterLongInsertExonicReads(Module):
	def __init__(self, module_id, is_docker=False):
		super(FilterLongInsertExonicReads, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["bam", "long_insert_bam"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("bam",			is_required=True)
		self.add_argument("nr_cpus",		default_value=16)
		self.add_argument("mem",			default_value=200)
		self.add_argument("sample_name",	is_required=True)
		self.add_argument("pad10_exon_bed",	is_resource=True)
		self.add_argument("thresh",			default_value=500)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		sample_name				= self.get_argument("sample_name")
		bam						= self.generate_unique_file_name(f".{sample_name}.small_insert.bam")
		self.add_output("bam",				bam)
		long_insert_bam			= self.generate_unique_file_name(f".{sample_name}.long_insert.bam")
		self.add_output("long_insert_bam",	long_insert_bam)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		sample_name				= self.get_argument("sample_name")
		input_bam				= self.get_argument("bam")
		threads					= self.get_argument("nr_cpus")
		thresh					= self.get_argument("thresh")
		pad10_exon_bed			= self.get_argument("pad10_exon_bed")

		# get output
		bam						= self.get_output("bam")
		long_insert_bam			= self.get_output("long_insert_bam")

		# add module
		cmd = "bash filter_long_insert_exonic_reads.sh"

		# add arguments
		cmd += " {0} {1} {2} {3} {4} {5} {6}".format(
			sample_name, input_bam, threads, thresh, pad10_exon_bed, bam, long_insert_bam)

		# add logging
		cmd += " !LOG3!"

		return cmd