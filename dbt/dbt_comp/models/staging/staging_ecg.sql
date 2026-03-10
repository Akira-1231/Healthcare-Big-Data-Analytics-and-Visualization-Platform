WITH ecg_data AS (
    SELECT * FROM {{source('import', 'ecgcsv' )}} 
)


SELECT
    {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }} AS info_key,
    {{ dbt_utils.generate_surrogate_key([ 'ed_stay_id','ed_hadm_id','hosp_hadm_id']) }} AS admission_key, 
    subject_id AS patient_key, 
    ecg_no_within_stay,
    {{ dbt_utils.generate_surrogate_key([ 'ecg_no_within_stay', 'ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }} AS location_key, 
    ed_diag_ed, 
    ed_diag_hosp, 
    hosp_diag_hosp, 
    all_diag_hosp, 
    all_diag_all,
    fold, 
    strat_fold
FROM ecg_data