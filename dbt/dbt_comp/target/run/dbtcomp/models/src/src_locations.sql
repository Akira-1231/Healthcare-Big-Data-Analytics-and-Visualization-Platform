
  create view "ecg_dw"."dev"."src_locations__dbt_tmp"
    
    
  as (
    WITH raw_locations AS (

    SELECT * FROM "ecg_dw"."import"."ecgcsv"
)

select 
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp,
    loaded_timestamp
from raw_locations
  );