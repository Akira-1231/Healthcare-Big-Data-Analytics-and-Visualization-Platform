
  create view "ecg_dw"."dev"."staging_admissions__dbt_tmp"
    
    
  as (
    WITH admissions AS (

    SELECT 
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id,
    row_number() over (partition by md5(cast(coalesce(cast(ed_stay_id as TEXT), '') || '-' || coalesce(cast(ed_hadm_id as TEXT), '') || '-' || coalesce(cast(hosp_hadm_id as TEXT), '') as TEXT)) order by loaded_timestamp desc ) as rn 
    FROM 
    "ecg_dw"."dev"."src_admissions"
)

select 
    md5(cast(coalesce(cast(ed_stay_id as TEXT), '') || '-' || coalesce(cast(ed_hadm_id as TEXT), '') || '-' || coalesce(cast(hosp_hadm_id as TEXT), '') as TEXT)) AS admission_key,
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id
from admissions
where rn = 1
  );