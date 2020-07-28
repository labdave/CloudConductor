from Modules import Module
import os.path

# Module created using CC_module_helper.py
class IGV_Translocation(Module):
	def __init__(self, module_id, is_docker=False):
		super(IGV_Translocation, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["igv_translocation_dir"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("anno_vcf",				is_required=True)
		self.add_argument("bam",					is_required=True)
		self.add_argument("bam_idx",				is_required=True)
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10)
		self.add_argument("pe_sr_threshold",		default_value=10)
		self.add_argument("split",					default_value=1)
		self.add_argument("grouped",				default_value=1)
		self.add_argument("zoom",					default_value=300)
		self.add_argument("bed",					is_resource=True)
		self.add_argument("repeat_blacklist",		is_resource=True)
		self.add_argument("repeat_blacklist_index",	is_resource=True)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		igv_translocation_dir		= os.path.join(self.get_output_dir(), "igv_translocation_snapshots")
		self.add_output("igv_translocation_dir",	igv_translocation_dir)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		anno_vcf					= self.get_argument("anno_vcf")
		bam							= self.get_argument("bam")
		pe_sr_threshold				= self.get_argument("pe_sr_threshold")
		split						= self.get_argument("split")
		grouped						= self.get_argument("grouped")
		zoom						= self.get_argument("zoom")
		nr_cpus						= self.get_argument("nr_cpus")
		bed							= self.get_argument("bed")
		repeat_blacklist			= self.get_argument("repeat_blacklist")

		# get output
		igv_translocation_dir		= self.get_output("igv_translocation_dir")

		# cut table into required columns and store > threshold
		cmd = "python filter_rows.py {0} {1} filtered_records.tsv !LOG3!;".format(anno_vcf, pe_sr_threshold)
		cmd += "cat filtered_records.tsv !LOG3!;"
		cmd += "mkdir -p {0}; !LOG3!;".format(igv_translocation_dir)
		cmd += "mkdir -p {0}/nonsquished; !LOG3!;".format(igv_translocation_dir)
		cmd += "mkdir -p {0}/squished; !LOG3!;".format(igv_translocation_dir)


		# run non-squished version
		cmd += "python igv_script_creator.py -f filtered_records.tsv -o nonsquished.script -b {0} -z {1} -t {2} -r {3} -d {4}/nonsquished".format(bam, zoom, bed, repeat_blacklist, igv_translocation_dir)
		if split:
			cmd += " -s"
		if grouped:
			cmd += " -g"
		cmd += " !LOG3!;"

		# run squished version
		cmd += "python igv_script_creator.py -f filtered_records.tsv -o squished.script -b {0} -z {1} -t {2} -r {3} -d {4}/squished".format(bam, zoom, bed, repeat_blacklist, igv_translocation_dir)
		if split:
			cmd += " -s"
		if grouped:
			cmd += " -g"
		cmd += " -q !LOG3!;"

		cmd += "cat nonsquished.script !LOG3!; cat squished.script !LOG3!;"
		
		# run igv non-squished version
		cmd += "xvfb-run --auto-servernum --server-args=\"-screen 0 4000x2400x24\" java -Xmx{0}000m -jar IGV_2.3.81/igv.jar -b nonsquished.script !LOG3!;".format(nr_cpus)
		# run igv squished version
		cmd += "xvfb-run --auto-servernum --server-args=\"-screen 0 4000x2400x24\" java -Xmx{0}000m -jar IGV_2.3.81/igv.jar -b squished.script !LOG3!".format(nr_cpus)
		# cmd += " run /usr/local/bin/destruct_ref/ {0} {1} {2} --bam_files {3}".format(
		# 	breaks, break_libs, break_reads, bam)

		# cmd += " --lib_ids {0} --maxjobs {1} --submit {2}".format(lib_ids, nr_cpus, submit)

		# # add logging
		# cmd += " !LOG3!"

		return cmd