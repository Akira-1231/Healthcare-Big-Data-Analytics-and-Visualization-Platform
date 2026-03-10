WITH raw_patients AS (

    SELECT * FROM {{source('import', 'ecgcsv' )}}
)

select 
    subject_id,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod,
    loaded_timestamp
from raw_patients


