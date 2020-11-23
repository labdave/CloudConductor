from Modules import Merger

# Module created using CC_module_helper.py
class VariantFiltering(Merger):
	def __init__(self, module_id, is_docker=False):
		super(VariantFiltering, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys 				= ["all_variants", "filt_variants", "wl_variants", "single_sample_merge", "filt_variants_val", "wl_variants_val"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("vcf",				is_required=True)
		self.add_argument("nr_cpus",			default_value=8)
		self.add_argument("mem",				default_value=48)
		self.add_argument("sample_id", 			is_required=True)
		self.add_argument("ref", 				is_required=True)
		self.add_argument("ref_idx",            is_required=True)
		self.add_argument("bam", 				is_required=True)
		self.add_argument("bam_idx", 			is_required=True)
		self.add_argument("rna_bam",			is_required=True)
		self.add_argument("rna_bam_idx",		is_required=True)




	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		all_variants							= self.generate_unique_file_name("all_variants.csv")
		filt_variants							= self.generate_unique_file_name("filt_variants.csv")
		wl_variants							  	= self.generate_unique_file_name("wl_variants.csv")
		single_sample_merge						= self.generate_unique_file_name("single_sample_merge.RData")
		filt_variants_val						=self.generate_unique_file_name("filt_variants_val.RData")
		wl_variants_val							=self.generate_unique_file_name("wl_variants_val.RData")


		self.add_output("all_variants",		all_variants)
		self.add_output("filt_variants",		filt_variants)
		self.add_output("wl_variants",		wl_variants)
		self.add_output("single_sample_merge",		single_sample_merge)
		self.add_output("filt_variants_val", filt_variants_val)
		self.add_output("wl_variants_val", wl_variants_val)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		vcf_list							= self.get_argument("vcf")
		sample_id             = self.get_argument("sample_id")
		bam = self.get_argument("bam")
		rna_bam=self.get_argument("rna_bam")
		ref = self.get_argument("ref")
		ref_idx = self.get_argument("ref_idx")

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
		wl_variants						= self.get_output("wl_variants")
		single_sample_merge				= self.get_output("single_sample_merge")
		filt_variants_val 				=self.get_output("filt_variants_val")
		wl_variants_val					=self.get_output("wl_variants_val")

    
		# add arguments
		cmd = " Rscript single_sample_VCF_merge_and_filter.R {0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13}".format(haplotypecaller_vcf, strelka2_vcf, deepvariant_vcf, bam, rna_bam, ref, ref_idx, all_variants, filt_variants, wl_variants, single_sample_merge, filt_variants_val, wl_variants_val, sample_id)


		
		# add logging
		cmd += " !LOG3!"

		return cmd
