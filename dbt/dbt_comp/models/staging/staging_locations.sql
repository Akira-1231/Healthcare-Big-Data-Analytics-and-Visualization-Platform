WITH locations AS (

    SELECT 
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp,
    row_number() over (partition by {{ dbt_utils.generate_surrogate_key(
      ['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']
    ) }} order by loaded_timestamp desc ) as rn 
    FROM 
     {{ref('src_locations')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(
      ['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']
    ) }} AS location_key, 
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp
from locations
where rn = 1


