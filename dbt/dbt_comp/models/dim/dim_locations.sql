{{
config(
materialized = 'table',
unique_key = 'location_key'
)
}}
select 
    location_key,
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp
from {{ref('staging_locations')}}


