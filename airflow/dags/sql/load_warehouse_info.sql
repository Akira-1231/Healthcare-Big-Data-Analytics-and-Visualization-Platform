-- Insert new records into the warehouse.dim_info table
INSERT INTO warehouse.dim_info (info_key, file_name_1, file_name_2, file_name_3, file_name_4, file_name_5, study_id, subject_id, ecg_time)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }} AS info_key,
    file_name_1,
    file_name_2,
    file_name_3,
    file_name_4,
    file_name_5,
    study_id,
    subject_id,
    ecg_time
FROM 
    import.ecgcsv e
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.dim_info w
    WHERE w.info_key = {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }}
);

-- Update existing records in warehouse.dim_info if any file names or other data have changed
UPDATE warehouse.dim_info w
SET 
    file_name_1 = e.file_name_1,
    file_name_2 = e.file_name_2,
    file_name_3 = e.file_name_3,
    file_name_4 = e.file_name_4,
    file_name_5 = e.file_name_5,
    study_id = e.study_id,
    subject_id = e.subject_id,
    ecg_time = e.ecg_time,
    loaded_timestamp = NOW()
FROM import.ecgcsv e
WHERE w.info_key = {{ dbt_utils.generate_surrogate_key([ 'file_name_1', 'file_name_2', 'file_name_3', 'file_name_4', 'file_name_5']) }}
AND (
    w.file_name_1 <> e.file_name_1 OR
    w.file_name_2 <> e.file_name_2 OR
    w.file_name_3 <> e.file_name_3 OR
    w.file_name_4 <> e.file_name_4 OR
    w.file_name_5 <> e.file_name_5 OR
    w.study_id <> e.study_id OR
    w.subject_id <> e.subject_id OR
    w.ecg_time <> e.ecg_time
);