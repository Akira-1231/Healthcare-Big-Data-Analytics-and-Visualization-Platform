-- Insert new records into the warehouse.dim_locations table
INSERT INTO warehouse.dim_locations (location_key, ecg_taken_in_ed, ecg_taken_in_hosp, ecg_taken_in_ed_or_hosp)
SELECT
    {{ dbt_utils.generate_surrogate_key(['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }} AS location_key,
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp
FROM 
    import.ecgcsv e
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.dim_locations w
    WHERE w.location_key = {{ dbt_utils.generate_surrogate_key(['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }}
);

-- Update existing records in warehouse.dim_locations if data has changed
UPDATE warehouse.dim_locations w
SET 
    ecg_taken_in_ed = e.ecg_taken_in_ed,
    ecg_taken_in_hosp = e.ecg_taken_in_hosp,
    ecg_taken_in_ed_or_hosp = e.ecg_taken_in_ed_or_hosp,
    loaded_timestamp = NOW() -- Assuming you have a loaded_timestamp column to track changes
FROM import.ecgcsv e
WHERE w.location_key = {{ dbt_utils.generate_surrogate_key(['ecg_taken_in_ed', 'ecg_taken_in_hosp', 'ecg_taken_in_ed_or_hosp']) }}
AND (
    w.ecg_taken_in_ed <> e.ecg_taken_in_ed OR
    w.ecg_taken_in_hosp <> e.ecg_taken_in_hosp OR
    w.ecg_taken_in_ed_or_hosp <> e.ecg_taken_in_ed_or_hosp
);