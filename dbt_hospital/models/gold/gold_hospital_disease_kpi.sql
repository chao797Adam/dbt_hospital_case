{{ config(materialized='table') }}

with
    visit_with_prev as (
        select
            visit_id,
            patient_id,
            hospital_id,
            admission_date,
            discharge_date,
            lag(discharge_date) over (
                partition by patient_id order by admission_date
            ) as previous_discharge,  -- �\udc90 close to discharge_date
            diagnosis_code,
            cost,
            _ingested_at,
            patient_first_last_name_masked,
            patient_city,
            gender,
            dob,
            first_name,
            last_name,
            hospital_name,
            hospital_city,
            bed_count,
            diagnosis_desc,
            silver_load_timestamp
        from {{ ref('fact_visit') }}
    ),

    visit_flagged as (
        select
            *,
            datediff(admission_date, previous_discharge) as days_since_last_visit,
            case
                when datediff(admission_date, previous_discharge) <= 30 then 1 else 0
            end as is_readmission_30d
        from visit_with_prev
    )

select
    hospital_id,
    hospital_name,
    diagnosis_desc,
    count(*) as total_visits,
    sum(is_readmission_30d) as total_readmissions,
    round(sum(is_readmission_30d) * 1.0 / count(*), 3) as readmission_rate,
    sum(cost) as total_cost,
    avg(cost) as avg_cost,
    current_timestamp() as gold_load_timestamp
from visit_flagged
group by hospital_id, hospital_name, diagnosis_desc
