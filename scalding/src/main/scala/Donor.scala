package com.twitter.scalding.examples

import com.twitter.scalding._

class DonorJob(args : Args) extends Job(args) {
  Tsv( args("input"), ('donor_id, 'donor_sex, 'donor_region_of_residence,'donor_vital_status  ,'disease_status_last_followup  ,'donor_relapse_type  ,'donor_age_at_diagnosis  ,'donor_age_at_enrollment ,'donor_age_at_last_followup  ,'donor_relapse_interval  ,'donor_diagnosis_icd10 ,'donor_tumour_staging_system_at_diagnosis  ,'donor_tumour_stage_at_diagnosis ,'donor_tumour_stage_at_diagnosis_supplemental  ,'donor_survival_time ,'donor_interval_of_last_followup ,'donor_notes))
  
     // Filters out the header row
    .filter(('donor_id)) { donor_id : String => donor_id != "donor_id" }
     // Group by sex,region
    .groupBy(('donor_sex, 'donor_region_of_residence)) { _.size }
    // Keep only sex,region,count
    .project(('donor_sex, 'donor_region_of_residence, 'size))
    .write( Tsv( args("output")) )
}
