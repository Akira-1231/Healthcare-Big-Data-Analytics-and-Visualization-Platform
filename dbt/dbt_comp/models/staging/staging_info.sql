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
    row_number() over (partition by {{ dbt_utils.generate_surrogate_key(
      [ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']
    ) }} order by loaded_timestamp desc ) as rn 
    FROM 
    {{ref('src_info')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(
      [ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']
    ) }} AS info_key,
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

