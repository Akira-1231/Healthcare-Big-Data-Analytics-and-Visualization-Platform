WITH admissions AS (

    SELECT 
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id,
    row_number() over (partition by {{ dbt_utils.generate_surrogate_key([ 'ed_stay_id','ed_hadm_id','hosp_hadm_id']) }} order by loaded_timestamp desc ) as rn 
    FROM 
    {{ref('src_admissions')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(
      [ 'ed_stay_id','ed_hadm_id','hosp_hadm_id']
    ) }} AS admission_key,
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id
from admissions
where rn = 1

