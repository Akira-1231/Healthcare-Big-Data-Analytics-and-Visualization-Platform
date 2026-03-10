{{
config(
materialized = 'table',
unique_key = 'patient_key'
)
}}

select 
    patient_key,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod
from {{ref('staging_patients')}}


