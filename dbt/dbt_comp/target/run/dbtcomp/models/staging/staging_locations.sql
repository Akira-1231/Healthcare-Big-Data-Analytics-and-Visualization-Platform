
  create view "ecg_dw"."dev"."staging_locations__dbt_tmp"
    
    
  as (
    WITH locations AS (

    SELECT 
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp,
    row_number() over (partition by md5(cast(coalesce(cast(ecg_taken_in_ed as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_hosp as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_ed_or_hosp as TEXT), '') as TEXT)) order by loaded_timestamp desc ) as rn 
    FROM 
     "ecg_dw"."dev"."src_locations"
)

select 
    md5(cast(coalesce(cast(ecg_taken_in_ed as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_hosp as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_ed_or_hosp as TEXT), '') as TEXT)) AS location_key, 
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp
from locations
where rn = 1
  );