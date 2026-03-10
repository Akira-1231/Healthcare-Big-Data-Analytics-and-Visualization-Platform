
  create view "ecg_dw"."dev"."src_patients__dbt_tmp"
    
    
  as (
    WITH raw_patients AS (

    SELECT * FROM "ecg_dw"."import"."ecgcsv"
)

select 
    subject_id,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod,
    loaded_timestamp
from raw_patients
  );