-- Insert new records into the warehouse.dim_patients table
INSERT INTO warehouse.dim_patients (patient_key, gender, age, anchor_age, anchor_year, dod)
SELECT
    subject_id AS patient_key,
    gender,
    age,
    anchor_age,
    anchor_year,
    dod
FROM 
    import.ecgcsv e
WHERE NOT EXISTS (
    SELECT 1 
    FROM warehouse.dim_patients w
    WHERE w.patient_key = e.subject_id
);

-- Update existing records in warehouse.dim_patients if patient data has changed
UPDATE warehouse.dim_patients w
SET 
    gender = e.gender,
    age = e.age,
    anchor_age = e.anchor_age,
    anchor_year = e.anchor_year,
    dod = e.dod,
    loaded_timestamp = NOW()  -- Assuming you have a loaded_timestamp column for tracking changes
FROM import.ecgcsv e
WHERE w.patient_key = e.subject_id
AND (
    w.gender <> e.gender OR
    w.age <> e.age OR
    w.anchor_age <> e.anchor_age OR
    w.anchor_year <> e.anchor_year OR
    w.dod <> e.dod
);