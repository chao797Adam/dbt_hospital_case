{{ config(
    materialized='incremental',
    unique_key='visit_id',
    incremental_strategy='merge'
) }}

with
    visit as (
        select *
        from {{ ref('brz_visit') }}
        {% if is_incremental() %}
            where
                _ingested_at
                > (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
        {% endif %}
        qualify row_number() over (partition by visit_id order by _ingested_at desc) = 1
    ),

    patient as (select * from {{ ref('dim_patient') }}),
    hospital as (select * from {{ ref('dim_hospital') }}),
    diagnosis as (select * from {{ ref('dim_diagnosis') }})

select
    v.visit_id,
    v.patient_id,
    v.hospital_id,
    v.admission_date,
    v.discharge_date,
    v.diagnosis_code,
    v.cost,
    v._ingested_at,

    p.patient_first_last_name_masked,
    p.city as patient_city,
    p.gender,
    p.dob,
    p.first_name,
    p.last_name,

    h.hospital_name,
    h.city as hospital_city,
    h.bed_count,

    d.diagnosis_desc,
    current_timestamp() as silver_load_timestamp
from visit v
left join patient p on v.patient_id = p.patient_id
left join hospital h on v.hospital_id = h.hospital_id
left join diagnosis d on v.diagnosis_code = d.diagnosis_code
