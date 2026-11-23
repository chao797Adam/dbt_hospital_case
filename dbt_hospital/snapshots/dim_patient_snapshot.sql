{% snapshot dim_patient_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='patient_id',
        strategy='check',
        check_cols=[
            'city',
            'gender',
            'dob',
            'patient_first_last_name_masked'
        ]
    )
}}

    select patient_id, gender, dob, city, patient_first_last_name_masked, _ingested_at
    from {{ ref('dim_patient') }}

{% endsnapshot %}
