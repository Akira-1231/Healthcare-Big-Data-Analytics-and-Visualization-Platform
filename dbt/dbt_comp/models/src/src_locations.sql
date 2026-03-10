WITH raw_locations AS (

    SELECT * FROM {{source('import', 'ecgcsv' )}}
)

select 
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp,
    loaded_timestamp
from raw_locations


