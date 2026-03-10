insert into warehouse.dim_admissions(admission_key,ed_stay_id,ed_hadm_id,hosp_hadm_id)
SELECT 
    {{ dbt_utils.generate_surrogate_key(
      [ 'ed_stay_id','ed_hadm_id','hosp_hadm_id']
    ) }} AS admission_key,
    ed_stay_id,
    ed_hadm_id,
    hosp_hadm_id
FROM import.ecgcsv
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.dim_admissions w
    WHERE w.admission_key = {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }}
);

UPDATE warehouse.dim_admissions w
set ed_stay_id = e.ed_stay_id, ed_hadm_id =  e.ed_hadm_id, hosp_hadm_id = e.hosp_hadm_id, loaded_timestamp=now()
FROM import.ecgcsv e 
where e.admission_key = w.admission_key and 

(w.ed_stay_id <> e.ed_stay_id or w.ed_hadm_id <> e.ed_hadm_id or w.hosp_hadm_id <> e.hosp_hadm_id);