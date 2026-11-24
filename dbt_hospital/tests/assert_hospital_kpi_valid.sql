select *
from {{ ref('gold_hospital_disease_kpi') }}
where
    total_visits <= 0
    or total_readmissions < 0
    or total_readmissions > total_visits
    or readmission_rate < 0
    or readmission_rate > 1
    or total_cost < 0
