{{ config(materialized='table') }}

with
    visit_with_prev as (
        select
            visit_id,
            patient_id,
            hospital_id,
            hospital_name,
            diagnosis_code,
            diagnosis_desc,
            admission_date,
            discharge_date,
            cost,
            lag(discharge_date) over (
                partition by patient_id, diagnosis_code order by admission_date
            ) as previous_discharge
        from {{ ref('fact_visit') }}
    ),
    visit_flagged as (
        select
            *,
            datediff(admission_date, previous_discharge) as days_since_last_visit,
            case
                when datediff(admission_date, previous_discharge) between 0 and 30
                then 1
                else 0
            end as is_readmission_30d
        from visit_with_prev
    )

select
    hospital_id,
    hospital_name,
    diagnosis_code,
    diagnosis_desc,
    count(*) as total_visits,
    sum(is_readmission_30d) as total_readmissions,
    round(sum(is_readmission_30d) * 1.0 / count(*), 3) as readmission_rate,
    round(sum(cost), 2) as total_cost,
    round(avg(cost), 2) as avg_cost,
    current_timestamp() as gold_load_timestamp
from visit_flagged
group by hospital_id, hospital_name, diagnosis_code, diagnosis_desc
