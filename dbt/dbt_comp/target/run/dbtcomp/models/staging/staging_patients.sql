
  create view "ecg_dw"."dev"."staging_patients__dbt_tmp"
    
    
  as (
    WITH patients AS (

    SELECT 
    subject_id,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod,
    row_number() over (partition by subject_id order by loaded_timestamp desc ) as rn 
    FROM 
    "ecg_dw"."dev"."src_patients"
)

select 
    subject_id as patient_key,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod
from patients
where rn = 1
  );