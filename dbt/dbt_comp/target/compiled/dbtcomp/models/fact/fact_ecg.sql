

SELECT
    info_key, 
    admission_key, 
    ed_diag_ed, 
    ed_diag_hosp, 
    hosp_diag_hosp, 
    all_diag_hosp, 
    all_diag_all, 
    patient_key,
    ecg_no_within_stay,
    location_key, 
    fold, 
    strat_fold
FROM "ecg_dw"."dev"."staging_ecg"