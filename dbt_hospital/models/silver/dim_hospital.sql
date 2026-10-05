{{ config(
    materialized='incremental',
    unique_key='hospital_id',
    incremental_strategy='merge'
) }}

with
    source as (
        select
            hospital_id,
            hospital_name,
            city,
            cast(bed_count as int) as bed_count,
            _ingested_at,
            current_timestamp() as silver_load_timestamp
        from {{ ref('brz_hospital') }}
        {% if is_incremental() %}
            where
                _ingested_at
                > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
        {% endif %}
    )

select *
from source
qualify row_number() over (partition by hospital_id order by _ingested_at desc) = 1
