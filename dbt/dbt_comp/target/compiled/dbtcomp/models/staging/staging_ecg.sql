WITH ecg_data AS (
    SELECT * FROM "ecg_dw"."import"."ecgcsv" 
)


SELECT
    md5(cast(coalesce(cast(file_name_1 as TEXT), '') || '-' || coalesce(cast(file_name_2 as TEXT), '') || '-' || coalesce(cast(file_name_3 as TEXT), '') || '-' || coalesce(cast(file_name_4 as TEXT), '') || '-' || coalesce(cast(file_name_5 as TEXT), '') as TEXT)) AS info_key,
    md5(cast(coalesce(cast(ed_stay_id as TEXT), '') || '-' || coalesce(cast(ed_hadm_id as TEXT), '') || '-' || coalesce(cast(hosp_hadm_id as TEXT), '') as TEXT)) AS admission_key, 
    subject_id AS patient_key, 
    ecg_no_within_stay,
    md5(cast(coalesce(cast(ecg_no_within_stay as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_ed as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_hosp as TEXT), '') || '-' || coalesce(cast(ecg_taken_in_ed_or_hosp as TEXT), '') as TEXT)) AS location_key, 
    ed_diag_ed, 
    ed_diag_hosp, 
    hosp_diag_hosp, 
    all_diag_hosp, 
    all_diag_all,
    fold, 
    strat_fold
FROM ecg_data