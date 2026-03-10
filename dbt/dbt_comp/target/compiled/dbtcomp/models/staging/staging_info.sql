WITH info AS (

    SELECT 
    file_name_1,
    file_name_2,
    file_name_3,
    file_name_4,
    file_name_5,
    study_id,
    subject_id,
    ecg_time,
    row_number() over (partition by md5(cast(coalesce(cast(file_name_1 as TEXT), '') || '-' || coalesce(cast(file_name_2 as TEXT), '') || '-' || coalesce(cast(file_name_3 as TEXT), '') || '-' || coalesce(cast(file_name_4 as TEXT), '') || '-' || coalesce(cast(file_name_5 as TEXT), '') as TEXT)) order by loaded_timestamp desc ) as rn 
    FROM 
    "ecg_dw"."dev"."src_info"
)

select 
    md5(cast(coalesce(cast(file_name_1 as TEXT), '') || '-' || coalesce(cast(file_name_2 as TEXT), '') || '-' || coalesce(cast(file_name_3 as TEXT), '') || '-' || coalesce(cast(file_name_4 as TEXT), '') || '-' || coalesce(cast(file_name_5 as TEXT), '') as TEXT)) AS info_key,
    file_name_1,
    file_name_2,
    file_name_3,
    file_name_4,
    file_name_5,
    study_id,
    subject_id,
    ecg_time
from info
where rn = 1