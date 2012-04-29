package com.twitter.scalding.examples

import com.twitter.scalding._

class Support(args : Args) extends Job(args) {
  
  Tsv("/datasets/icgc/icgc8.gene_info.tsv", ('tax_id, 'gene_id, 'symbol, 'tail))
  .map('symbol -> 'symbol2) { symbol : String => symbol }
  .project('symbol, 'symbol2)
  .write(Tsv("/mnt/users/team7/gene_nodes.tsv"))

  Tsv( "/datasets/icgc/icgc8.donor.tsv", ('donor_id, 'donor_sex, 'donor_region_of_residence,'donor_vital_status  ,'disease_status_last_followup  ,'donor_relapse_type  ,'donor_age_at_diagnosis  ,'donor_age_at_enrollment ,'donor_age_at_last_followup  ,'donor_relapse_interval  ,'donor_diagnosis_icd10 ,'donor_tumour_staging_system_at_diagnosis  ,'donor_tumour_stage_at_diagnosis ,'donor_tumour_stage_at_diagnosis_supplemental  ,'donor_survival_time ,'donor_interval_of_last_followup ,'donor_notes))
  .flatMap('donor_region_of_residence -> 'donor_region_of_residence2) { country :String => country }
  .project('donor_region_of_residence, 'donor_region_of_residence2)
  .unique()
  .write(Tsv("/mnt/users/team7/country_nodes.tsv"))
}
