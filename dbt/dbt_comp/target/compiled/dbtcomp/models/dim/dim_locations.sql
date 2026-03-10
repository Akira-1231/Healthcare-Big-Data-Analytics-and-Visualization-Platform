
select 
    location_key,
    ecg_taken_in_ed, 
    ecg_taken_in_hosp, 
    ecg_taken_in_ed_or_hosp
from "ecg_dw"."dev"."staging_locations"