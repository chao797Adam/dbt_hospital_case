select diagnosis_code, diagnosis_desc, _ingested_at
from {{ source('hospital_source', 'diagnosis') }}
