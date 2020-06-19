from Modules import Merger

# Module created using CC_module_helper.py
class VariantFiltering(Merger):
	def __init__(self, module_id, is_docker=False):
		super(VariantFiltering, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys 				= ["all_variants", "filt_variants", "wl_variants", "single_sample_merge"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("vcf",				is_required=True)
		self.add_argument("nr_cpus",			default_value=1)
		self.add_argument("mem",				default_value=5)
		self.add_argument("sample_id", is_required=True)



	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		all_variants							= self.generate_unique_file_name("all_variants.csv")
		filt_variants							= self.generate_unique_file_name("filt_variants.csv")
		wl_variants							  = self.generate_unique_file_name("wl_variants.csv")
		single_sample_merge				= self.generate_unique_file_name("single_sample_merge.RData")
		self.add_output("all_variants",		all_variants)
		self.add_output("filt_variants",		filt_variants)
		self.add_output("wl_variants",		wl_variants)
		self.add_output("single_sample_merge",		single_sample_merge)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		vcf_list							= self.get_argument("vcf")
		sample_id             = self.get_argument("sample_id")

		for vcf in vcf_list:
			if 'strelka2' in vcf:
			  strelka2_vcf=vcf
				continue
			if 'deepvariant' in vcf:
			  deepvariant_vcf=vcf
				continue
			if 'haplotypecaller' in vcf:
			  haplotypecaller_vcf=vcf
				continue
		# get output
		all_variants					= self.get_output("all_variants")
		filt_variants					= self.get_output("filt_variants")
    wl_variants					= self.get_output("wl_variants")
    single_sample_merge					= self.get_output("single_sample_merge")

    
		# add arguments
		cmd = " Rscript single_sample_VCF_merge_and_filter.R {1} {2} {3} {4} {5} {6} {7} {8}".format(
			haplotypecaller_vcf, strelka2_vcf, deepvariant_vcf, all_variants, filt_variants, wl_variants, single_sample_merge, sample_id)

		
		# add logging
		cmd += " !LOG3!"

		return cmd
