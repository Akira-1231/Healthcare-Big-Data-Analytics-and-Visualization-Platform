WITH raw_info AS (

    SELECT * FROM "ecg_dw"."import"."ecgcsv"
)

select 
    file_name_1,
    file_name_2,
    file_name_3,
    file_name_4,
    file_name_5,
    study_id,
    subject_id,
    ecg_time,
    loaded_timestamp
from raw_info