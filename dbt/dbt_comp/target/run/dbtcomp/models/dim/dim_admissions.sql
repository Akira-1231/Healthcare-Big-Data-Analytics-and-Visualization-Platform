
  
    

  create  table "ecg_dw"."dev"."dim_admissions__dbt_tmp"
  
  
    as
  
  (
    

select 
    admission_key,
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id
from "ecg_dw"."dev"."staging_admissions"
  );
  