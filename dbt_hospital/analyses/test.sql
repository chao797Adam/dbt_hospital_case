select * from {{ source('hospital_source', 'patient') }}
