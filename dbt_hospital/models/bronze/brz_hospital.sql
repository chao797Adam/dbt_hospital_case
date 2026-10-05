select
    hospital_id, hospital_name, city, cast(bed_count as int) as bed_count, _ingested_at
from {{ source('hospital_source', 'hospital') }}
