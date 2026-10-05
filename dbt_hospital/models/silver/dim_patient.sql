{{ config(
    materialized='incremental',
    unique_key='patient_id',
    incremental_strategy='merge'
) }}

with
    source as (
        select
            patient_id,
            first_name,
            last_name,
            sha2(
                concat_ws('|', first_name, last_name), 256
            ) as patient_first_last_name_masked,
            city,
            gender,
            dob,
            _ingested_at,
            current_timestamp() as silver_load_timestamp
        from {{ ref('brz_patient') }}
        {% if is_incremental() %}
            where
                _ingested_at
                > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
        {% endif %}
    )

select *
from source
qualify row_number() over (partition by patient_id order by _ingested_at desc) = 1
