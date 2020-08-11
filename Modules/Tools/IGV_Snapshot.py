from Modules import Module
import os.path

# Module created using CC_module_helper.py
class IGV_Snapshot(Module):
	def __init__(self, module_id, is_docker=False):
		super(IGV_Snapshot, self).__init__(module_id, is_docker)
		# Add output keys here if needed
		self.output_keys = ["igv_snapshot_dir"]


	def define_input(self):
		# Module creator needs to define which arguments have is_resource=True
		# Module creator needs to rename arguments as required by CC
		self.add_argument("anno_vcf")
		self.add_argument("wl_variants")
		self.add_argument("bam",					is_required=True)
		self.add_argument("bam_idx",				is_required=True)
		self.add_argument("non_split_bam")
		self.add_argument("non_split_bam_idx")
		self.add_argument("nr_cpus",				default_value=2)
		self.add_argument("mem",					default_value=10)
		self.add_argument("pe_sr_threshold",		default_value=10)
		self.add_argument("split",					default_value=1)
		self.add_argument("grouped",				default_value=1)
		self.add_argument("zoom",					default_value=300)
		self.add_argument("bed",					is_resource=True)
		self.add_argument("repeat_blacklist",		is_resource=True)
		self.add_argument("repeat_blacklist_index",	is_resource=True)
		self.add_argument("segmental_blacklist",		is_resource=True)
		self.add_argument("ig_bed",					is_resource=True)
		self.add_argument("fish_bed",				is_resource=True)


	def define_output(self):
		# Module creator needs to define what the outputs are
		# based on the output keys provided during module creation
		igv_snapshot_dir		= os.path.join(self.get_output_dir(), "igv_snapshots")
		self.add_output("igv_snapshot_dir",	igv_snapshot_dir)


	def define_command(self):
		# Module creator needs to use renamed arguments as required by CC
		anno_vcf					= self.get_argument("anno_vcf")
		wl_variants					= self.get_argument("wl_variants")
		bam							= self.get_argument("bam")
		non_split_bam				= self.get_argument("non_split_bam")
		pe_sr_threshold				= self.get_argument("pe_sr_threshold")
		split						= self.get_argument("split")
		grouped						= self.get_argument("grouped")
		zoom						= self.get_argument("zoom")
		nr_cpus						= self.get_argument("nr_cpus")
		bed							= self.get_argument("bed")
		repeat_blacklist			= self.get_argument("repeat_blacklist")
		segmental_blacklist			= self.get_argument("segmental_blacklist")
		ig_bed						= self.get_argument("ig_bed")
		fish_bed					= self.get_argument("fish_bed")

		# get output
		igv_snapshot_dir			= self.get_output("igv_snapshot_dir")

		if anno_vcf is not None:
			vcf = anno_vcf
			columns = 5
			thresh_column = 7

			# command
			cmd = "python filter_rows.py -o filtered.tsv -c {0} -t {1} -T {2} -i {3} !LOG3!;".format(
				columns, pe_sr_threshold, thresh_column, vcf)
			
			cmd += "cat filtered.tsv !LOG3!;"
			cmd += "mkdir -p {0} !LOG3!;".format(igv_snapshot_dir)
			cmd += "mkdir -p {0}/nonsquished_nosplitreads !LOG3!;".format(igv_snapshot_dir)
			cmd += "mkdir -p {0}/squished_nosplitreads !LOG3!;".format(igv_snapshot_dir)
			cmd += "mkdir -p {0}/nonsquished_splitreads !LOG3!;".format(igv_snapshot_dir)

			# BAM WITH SPLIT READS

			# run non-squished version
			cmd += "python igv_script_creator.py -f filtered.tsv -o nonsquished_splitreads.script"
			cmd += " -b {0} -z {1} -t {2} -r {3} -S {4} -F {5} -i {6} -d {7}/nonsquished_splitreads".format(
				bam, zoom, bed, repeat_blacklist, segmental_blacklist, fish_bed, ig_bed, igv_snapshot_dir)
			if split:
				cmd += " -s"
			if grouped:
				cmd += " -g"
			cmd += " !LOG3!;"

			# BAM WITH NO SPLIT READS

			# run non-squished version
			cmd += "python igv_script_creator.py -f filtered.tsv -o nonsquished_nosplitreads.script"
			cmd += " -b {0} -z {1} -t {2} -r {3} -S {4} -F {5} -i {6} -d {7}/nonsquished_nosplitreads".format(
				non_split_bam, zoom, bed, repeat_blacklist, segmental_blacklist, fish_bed, ig_bed, igv_snapshot_dir)
			if split:
				cmd += " -s"
			if grouped:
				cmd += " -g"
			cmd += " !LOG3!;"

			# run squished version
			cmd += "python igv_script_creator.py -f filtered.tsv -o squished_nosplitreads.script"
			cmd += " -b {0} -z {1} -t {2} -r {3} -S {4} -F {5} -i {6} -d {7}/squished_nosplitreads".format(
				non_split_bam, zoom, bed, repeat_blacklist, segmental_blacklist, fish_bed, ig_bed, igv_snapshot_dir)
			if split:
				cmd += " -s"
			if grouped:
				cmd += " -g"
			cmd += " -q !LOG3!;"

			cmd += "cat nonsquished_splitreads.script !LOG3!;"
			cmd += "cat nonsquished_nosplitreads.script !LOG3!;"
			cmd += "cat squished_nosplitreads.script !LOG3!;"
			
			# NOPE run igv non-squished version
			# NOPE cmd += "xvfb-run --auto-servernum --server-args=\"-screen 0 4000x2400x24\" java -Xmx{0}000m -jar IGV_2.3.81/igv.jar -b nonsquished_splitreads.script !LOG3!;".format(nr_cpus)
			
			# run igv non-squished version nosplitreads
			cmd += "xvfb-run --auto-servernum --server-args=\"-screen 0 4000x2400x24\" java -Xmx{0}000m -jar IGV_2.3.81/igv.jar -b nonsquished_nosplitreads.script !LOG3!;".format(nr_cpus)
			
			# NOPE run igv squished version nosplitreads
			# NOPE cmd += "xvfb-run --auto-servernum --server-args=\"-screen 0 4000x2400x24\" java -Xmx{0}000m -jar IGV_2.3.81/igv.jar -b squished_nosplitreads.script !LOG3!;".format(nr_cpus)
			
			cmd += "echo done"

			return cmd
		else:
			vcf = wl_variants
			columns = 3

			# command
			cmd = "python filter_rows.py -o filtered.tsv -c {0} -i {1} !LOG3!;".format(
				columns, vcf)

			cmd += "python igv_script_creator.py -f filtered.tsv -o snv.script"
			cmd += " -b {0} -z {1} -t {2} -r {3} -d {4}/nonsquished_splitreads !LOG3!;".format(
				bam, zoom, bed, repeat_blacklist, igv_snapshot_dir)

			cmd += "cat snv.script !LOG3!;"

			# run igv
			cmd += "xvfb-run --auto-servernum --server-args=\"-screen 0 4000x2400x24\" java -Xmx{0}000m -jar IGV_2.3.81/igv.jar -b snv.script !LOG3!;".format(nr_cpus)
			
			cmd += "echo done"

			return cmd