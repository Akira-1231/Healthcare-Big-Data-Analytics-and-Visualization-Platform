-- Insert new records into the warehouse.fact_ecg table
INSERT INTO warehouse.fact_ecg (
    info_key, 
    admission_key, 
    ed_diag_ed, ed_diag_hosp, hosp_diag_hosp, all_diag_hosp, all_diag_all, 
    patient_key,
    ecg_no_within_stay,
    location_key, 
    fold, 
    strat_fold
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }} AS info_key, -- info_key as primary key
    {{ dbt_utils.generate_surrogate_key([ 'ed_stay_id','ed_hadm_id','hosp_hadm_id']) }} AS admission_key,  -- admission_key
    ed_diag_ed, 
    ed_diag_hosp, 
    hosp_diag_hosp, 
    all_diag_hosp, 
    all_diag_all, 
    subject_id AS patient_key,  -- patient_key
    ecg_no_within_stay,
    {{ dbt_utils.generate_surrogate_key(['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }} AS location_key,  -- location_key
    fold, 
    strat_fold
FROM 
    import.ecgcsv e
WHERE NOT EXISTS (
    SELECT 1 
    FROM warehouse.fact_ecg w
    WHERE w.info_key = {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }}
);

-- Update existing records in warehouse.fact_ecg if the data has changed
UPDATE warehouse.fact_ecg w
SET 
    admission_key = e.admission_key,
    ed_diag_ed = e.ed_diag_ed,
    ed_diag_hosp = e.ed_diag_hosp,
    hosp_diag_hosp = e.hosp_diag_hosp,
    all_diag_hosp = e.all_diag_hosp,
    all_diag_all = e.all_diag_all,
    patient_key = e.subject_id,
    ecg_no_within_stay = e.ecg_no_within_stay,
    location_key = {{ dbt_utils.generate_surrogate_key(['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }},
    fold = e.fold,
    strat_fold = e.strat_fold,
    loaded_timestamp = NOW()  -- Assuming you have a timestamp column to track the updates
FROM 
    import.ecgcsv e
WHERE 
    w.info_key = {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }}
AND (
    w.admission_key <> {{ dbt_utils.generate_surrogate_key([ 'ed_stay_id','ed_hadm_id','hosp_hadm_id']) }} OR
    w.ed_diag_ed <> e.ed_diag_ed OR
    w.ed_diag_hosp <> e.ed_diag_hosp OR
    w.hosp_diag_hosp <> e.hosp_diag_hosp OR
    w.all_diag_hosp <> e.all_diag_hosp OR
    w.all_diag_all <> e.all_diag_all OR
    w.patient_key <> e.subject_id OR
    w.ecg_no_within_stay <> e.ecg_no_within_stay OR
    w.location_key <> {{ dbt_utils.generate_surrogate_key(['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }} OR
    w.fold <> e.fold OR
    w.strat_fold <> e.strat_fold
);