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
		self.add_argument("vcf_gz",				is_required=True)
		self.add_argument("nr_cpus",			default_value=1)
		self.add_argument("mem",				default_value=5)
		self.add_argument("whitelist",			is_required=True, is_resource=True)
		self.add_argument("extended_whitelist", is_resource=True)
		self.add_argument("e",					default_value=False)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		purity							= self.generate_unique_file_name("purity.tsv")
		self.add_output("purity_estimate",		purity)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		vcf_gz							= self.get_argument("vcf_gz")
		whitelist						= self.get_argument("whitelist")
		ext_whitelist					= self.get_argument("extended_whitelist")
		e								= self.get_argument("e")

		for vcf in vcf_gz:
			if 'variants.vcf' in vcf:
				strelka_vcf = vcf.rstrip('.gz')
				continue
			if 'deepvariant' in vcf:
				deepvariant_vcf = vcf.rstrip('.gz')
				continue
			if 'haplotypecaller' in vcf:
				haplotypecaller_vcf = vcf.rstrip('.gz')
				continue
		# get output
		purity_estimate					= self.get_output("purity_estimate")

		# add arguments
		cmd = " bash vcf_pipeline.sh -w {0} -d {1} -s {2} -g {3} -o {4}".format(
			whitelist, deepvariant_vcf, strelka_vcf, haplotypecaller_vcf, purity_estimate)

		if e:
			cmd += " -e {0}".format(ext_whitelist)

		# add logging
		cmd += " !LOG3!"

		return cmd