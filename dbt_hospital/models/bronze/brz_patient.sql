select
    patient_id,
    first_name,
    last_name,
    gender,
    cast(dob as date) as dob,
    city,
    _ingested_at
from {{ source('hospital_source', 'patient') }}
