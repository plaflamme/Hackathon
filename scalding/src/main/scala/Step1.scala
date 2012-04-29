package com.twitter.scalding.examples

import com.twitter.scalding._

class Step1(args : Args) extends Job(args) {
  
  // 30K rows
  val donors = Tsv( "/datasets/icgc/icgc8.donor.tsv", ('donor_id, 'donor_sex, 'donor_region_of_residence,'donor_vital_status  ,'disease_status_last_followup  ,'donor_relapse_type  ,'donor_age_at_diagnosis  ,'donor_age_at_enrollment ,'donor_age_at_last_followup  ,'donor_relapse_interval  ,'donor_diagnosis_icd10 ,'donor_tumour_staging_system_at_diagnosis  ,'donor_tumour_stage_at_diagnosis ,'donor_tumour_stage_at_diagnosis_supplemental  ,'donor_survival_time ,'donor_interval_of_last_followup ,'donor_notes))
     // Filters out the header row
    .filter(('donor_id)) { donor_id : String => donor_id != "donor_id" }
     // Only people between 40 and 60 years
    .filter('donor_age_at_diagnosis) { age : Int => age > 40 && age < 60}
    .filter('donor_region_of_residence) { country : String => country.charAt(0) != '-'}
    .project(('donor_id, 'donor_region_of_residence));

  val ssm_m = Tsv("/datasets/icgc/icgc8.ssm_m.tsv", ('analysis_id, 'donor_id,'tumour_sample_id  ,'matched_sample_id ,'assembly_version  ,'platform  ,'experimental_protocol ,'base_calling_algorithm  ,'alignment_algorithm ,'variation_calling_algorithm ,'other_analysis_algorithm  ,'seq_coverage  ,'raw_data_repository ,'raw_data_accession  ,'note)) 
     // Filters out the header row
    .filter(('donor_id)) { donor_id : String => donor_id != "donor_id" }
    .project(('analysis_id, 'donor_id, 'tumour_sample_id,'matched_sample_id));

  // 90M rows
  val ssm_p = Tsv("/datasets/icgc/icgc8.ssm_p.tsv", ('analysis_id ,'matched_sample_id, 'mutation_id ,'mutation_type ,'chromosome  ,'chromosome_start  ,'chromosome_end  ,'chromosome_strand ,'refsnp_allele ,'refsnp_strand ,'reference_genome_allele ,'control_genotype  ,'tumour_genotype ,'mutation  ,'expressed_allele  ,'quality_score ,'probability ,'read_count  ,'is_annotated  ,'validation_status ,'validation_platform ,'xref_ensembl_var_id)) 
     // Filters out the header row
    .filter(('analysis_id)) { analysis_id : String => analysis_id != "analysis_id" }
    .project('analysis_id,'matched_sample_id, 'mutation_id, 'mutation_type , 'chromosome  ,'chromosome_start  ,'chromosome_end  ,'chromosome_strand ,'refsnp_allele ,'refsnp_strand ,'reference_genome_allele ,'control_genotype  ,'tumour_genotype ,'mutation  ,'expressed_allele )

  val ssm_s = Tsv("/datasets/icgc/icgc8.ssm_s.tsv", ('analysis_id  ,'tumour_sample_id  ,'mutation_id ,'consequence_type  ,'aa_mutation ,'cds_mutation  ,'protein_domain_affected ,'gene_affected ,'transcript_affected ,'gene_build_version  ,'note)) 
     // Filters out the header row
    .filter(('analysis_id)) { analysis_id : String => analysis_id != "analysis_id" }
    .project('mutation_id, 'consequence_type  ,'gene_affected)

  val affectedGene1 =  ssm_m.joinWithLarger(('analysis_id  ,'matched_sample_id)-> ('analysis_id  ,'matched_sample_id), ssm_p)
         .joinWithSmaller('mutation_id -> 'mutation_id, ssm_s)
         .joinWithSmaller('donor_id -> 'donor_id, donors)
         .project('donor_id, 'donor_region_of_residence, 'gene_affected, 'mutation)
         .write( Tsv("/mnt/users/team7/step1.tsv") );
}
