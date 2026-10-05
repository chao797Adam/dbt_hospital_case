{{ config(
    materialized='incremental',
    unique_key='diagnosis_code',
    incremental_strategy='merge'
) }}

select
    diagnosis_code,
    diagnosis_desc,
    _ingested_at,
    current_timestamp() as silver_load_timestamp
from {{ ref('brz_diagnosis') }}

{% if is_incremental() %}
    where
        _ingested_at
        > (select coalesce(max(load_timestamp), '1900-01-01') from {{ this }})
{% endif %}

qualify row_number() over (partition by diagnosis_code order by _ingested_at desc) = 1
