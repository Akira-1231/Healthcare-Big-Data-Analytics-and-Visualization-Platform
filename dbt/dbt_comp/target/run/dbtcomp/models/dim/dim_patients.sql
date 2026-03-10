
  
    

  create  table "ecg_dw"."dev"."dim_patients__dbt_tmp"
  
  
    as
  
  (
    

select 
    patient_key,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod
from "ecg_dw"."dev"."staging_patients"
  );
  