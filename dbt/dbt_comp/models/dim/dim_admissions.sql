{{
config(
materialized = 'table',
unique_key = 'admission_key'
)
}}

select 
    admission_key,
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id
from {{ref('staging_admissions')}}

