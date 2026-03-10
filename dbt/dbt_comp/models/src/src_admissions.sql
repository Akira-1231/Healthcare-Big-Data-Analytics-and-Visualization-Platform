WITH raw_admissions AS (
    SELECT * FROM {{source('import', 'ecgcsv' )}}
)

select 
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id,
    loaded_timestamp
from raw_admissions

