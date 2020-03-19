from Modules import Module


class Vcf2Maf(Module):
    def __init__(self, module_id, is_docker = True):
        super(Vcf2Maf, self).__init__(module_id, is_docker)
        self.output_keys = ["maf"]

    def define_input(self):
        self.add_argument("sample_name",    is_required=True)
        self.add_argument("vcf",            is_required=True)
        self.add_argument("ref",            is_required=True, is_resource=True)
        self.add_argument("exac",           is_required=True, is_resource=True)
        self.add_argument("vep",            is_required=True, is_resource=True)
        self.add_argument("vcf2maf",        is_required=True, is_resource=True)
        self.add_argument("ncbi_build",     default_value="GRCh38")

        self.add_argument("nr_cpus",        default_value=4)
        self.add_argument("mem",            default_value=15)

    def define_output(self):
        #
        sample_name = self.get_argument("sample_name")
        output_file = self.generate_unique_file_name(extension="{0}.maf".format(sample_name))

        self.add_output("maf",output_file)


    def define_command(self):

        # Get program options
        vcf2maf             = self.get_argument("vcf2maf")
        input_vcf           = self.get_argument("vcf")
        output_maf          = self.get_output("maf")
        ref_fasta           = self.get_argument("ref")
        vep_data            = self.get_argument("vep")
        filter_vcf          = self.get_argument("exac")
        ncbi_build          = self.get_argument("ncbi_build")


        cmd = "{0} --input-vcf {1} --output-maf {2} --vep-path /usr/local/bin/ --vep-data {3} --ref-fasta {4} " \
              "--filter-vcf {5} --ncbi-build {6} --retain-info AC,AF,AN,BaseQRankSum,DP,ExcessHet,FS,MLEAC,MLEAF,MQ,MQRankSum,QD,ReadPosRankSum,SOR,ANNOVAR_DATE,Func.refGene,Gene.refGene,GeneDetail.refGene,ExonicFunc.refGene,AAChange.refGene,genomicSuperDups,ExAC_ALL,ExAC_AFR,ExAC_AMR,ExAC_EAS,ExAC_FIN,ExAC_NFE,ExAC_OTH,ExAC_SAS,gnomAD_exome_ALL,gnomAD_exome_AFR,gnomAD_exome_AMR,gnomAD_exome_ASJ,gnomAD_exome_EAS,gnomAD_exome_FIN,gnomAD_exome_NFE,gnomAD_exome_OTH,gnomAD_exome_SAS,gnomAD_genome_ALL,gnomAD_genome_AFR,gnomAD_genome_AMR,gnomAD_genome_ASJ,gnomAD_genome_EAS,gnomAD_genome_FIN,gnomAD_genome_NFE,gnomAD_genome_OTH,avsnp150,esp6500siv2_all,1000g2015aug_all,cosmic87_coding,cosmic87_noncoding,SIFT_score,SIFT_converted_rankscore,SIFT_pred,Polyphen2_HDIV_score,Polyphen2_HDIV_rankscore,Polyphen2_HDIV_pred,Polyphen2_HVAR_score,Polyphen2_HVAR_rankscore,Polyphen2_HVAR_pred,LRT_score,LRT_converted_rankscore,LRT_pred,MutationTaster_score,MutationTaster_converted_rankscore,MutationTaster_pred,MutationAssessor_score,MutationAssessor_score_rankscore,MutationAssessor_pred,FATHMM_score,FATHMM_converted_rankscore,FATHMM_pred,PROVEAN_score,PROVEAN_converted_rankscore,PROVEAN_pred,VEST3_score,VEST3_rankscore,MetaSVM_score,MetaSVM_rankscore,MetaSVM_pred,MetaLR_score,MetaLR_rankscore,MetaLR_pred,M-CAP_score,M-CAP_rankscore,M-CAP_pred,REVEL_score,REVEL_rankscore,MutPred_score,MutPred_rankscore,CADD_raw,CADD_raw_rankscore,CADD_phred,DANN_score,DANN_rankscore,fathmm-MKL_coding_score,fathmm-MKL_coding_rankscore,fathmm-MKL_coding_pred,Eigen_coding_or_noncoding,Eigen-raw,Eigen-PC-raw,GenoCanyon_score,GenoCanyon_score_rankscore,integrated_fitCons_score,integrated_fitCons_score_rankscore,integrated_confidence_value,GERP++_RS,GERP++_RS_rankscore,phyloP100way_vertebrate,phyloP100way_vertebrate_rankscore,phyloP20way_mammalian,phyloP20way_mammalian_rankscore,phastCons100way_vertebrate,phastCons100way_vertebrate_rankscore,phastCons20way_mammalian,phastCons20way_mammalian_rankscore,SiPhy_29way_logOdds,SiPhy_29way_logOdds_rankscore,Interpro_domain,GTEx_V6p_gene,GTEx_V6p_tissue,cadd14gt10,nci60,CLNALLELEID,CLNDN,CLNDISDB,CLNREVSTAT,CLNSIG,ALLELE_END".format(
            vcf2maf, input_vcf, output_maf, vep_data, ref_fasta, filter_vcf, ncbi_build)

        return cmd
