from Modules import Module

class TranslocationFiltering(Module):
	def __init__(self, module_id, is_docker=False):
		super(TranslocationFiltering, self).__init__(module_id, is_docker)
		self.output_keys = ["all_translocations", "filt_translocations", "fish_translocations"]

	def define_input(self):
		# Input keys
		self.add_argument("anno_vcf",			   is_required=True)
		self.add_argument("min_reads", 			   default_value=5)
		self.add_argument("filter_translocations", is_required=True, is_resource=True)
		self.add_argument("nr_cpus",			   default_value=2)
		self.add_argument("mem",				   default_value=10)

	def define_output(self):
		# Output keys
		all_translocations			= self.generate_unique_file_name("all_translocations.txt")
		filt_translocations			= self.generate_unique_file_name("filt_translocations.txt")
		fish_translocations			= self.generate_unique_file_name("fish_translocations.txt")

		self.add_output("all_translocations", all_translocations)
		self.add_output("filt_translocations", filt_translocations)
		self.add_output("fish_translocations", fish_translocations)

	def define_command(self):
		# Get input
		vcf = self.get_argument("anno_vcf")
		min_reads = self.get_argument("min_reads")

		# Get output
		all_translocations = self.get_output("all_translocations")
		filt_translocations = self.get_output("filt_translocations")
		fish_translocations = self.get_output("fish_translocations")
    
		# Save the input
		cmd = "cp {0} {1}".format(vcf, all_translocations)

		# add arguments
		cmd += "; Rscript filter_translocations.R"

		cmd += " -i {0}".format(vcf)
		cmd += " -t {0}".format(filt_translocations)
		cmd += " -g {0}".format(fish_translocations)
		cmd += " -r {0}".format(min_reads)

		# add logging
		cmd += " !LOG3!"

		return cmd
