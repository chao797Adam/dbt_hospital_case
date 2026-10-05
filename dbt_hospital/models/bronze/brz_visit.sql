select
    visit_id,
    patient_id,
    hospital_id,
    cast(admission_date as date) as admission_date,
    cast(discharge_date as date) as discharge_date,
    diagnosis_code,
    cast(cost as double) as cost,
    _ingested_at
from {{ source('hospital_source', 'visit') }}
