

select 
    patient_key,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod
from "ecg_dw"."dev"."staging_patients"