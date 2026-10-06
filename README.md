# Healthcare Data Pipeline with Databricks + dbt

An end-to-end data pipeline for a healthcare dataset, built on the **Medallion architecture** (Source → Bronze → Silver → Gold).

The pipeline:
- Ingests 4 CSV sources (diagnosis, hospital, patient, visit) from a Databricks Volume.
- Loads them into a `source` schema using **Auto Loader**.
- Amends schema and data types in a **Bronze** layer (dbt).
- Builds dimensional models (SCD1) in a **Silver** layer (dbt).
- Produces a KPI table for 30-day readmission analysis in a **Gold** layer (dbt).

---

## 1. Data Sources

Four CSVs are stored in a Databricks Volume:

```
/Volumes/z_dbt_hospital/source/raw_data_source/
├── diagnosis/    ← diagnosis.csv
├── hospital/     ← hospital.csv
├── patient/      ← patient.csv
└── visit/        ← visit.csv
```

| Source | Rows | Key | Description |
| :--- | :--- | :--- | :--- |
| diagnosis | 5 | diagnosis_code (D001–D005) | Diagnosis code + description |
| hospital | 5 | hospital_id (H001–H005) | Hospital name, city, bed count |
| patient | 5 | patient_id (P001–P005) | Patient name, gender, DOB, city |
| visit | 8 | visit_id (V1001–V1008) | Visit details: patient, hospital, diagnosis, cost, dates |

---

## 2. Source Layer: Auto Loader Ingestion

Each CSV is ingested into the `z_dbt_hospital.source` schema using **Auto Loader** (`cloudFiles`).

### Auto Loader Code

```python
from pyspark.sql.functions import current_timestamp

sources = [
    {"name": "diagnosis", "src": "/Volumes/z_dbt_hospital/source/raw_data_source/diagnosis/"},
    {"name": "hospital",  "src": "/Volumes/z_dbt_hospital/source/raw_data_source/hospital/"},
    {"name": "patient",   "src": "/Volumes/z_dbt_hospital/source/raw_data_source/patient/"},
    {"name": "visit",     "src": "/Volumes/z_dbt_hospital/source/raw_data_source/visit/"},
]

meta_base = "/Volumes/z_dbt_hospital/source/ingestion_metadata"

for s in sources:
    table_name = f"z_dbt_hospital.source.{s['name']}"
    checkpoint = f"{meta_base}/{s['name']}/checkpoint/"
    schema_loc = f"{meta_base}/{s['name']}/schema/"

    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .load(s["src"])
    )

    (df
        .withColumn("_ingested_at", current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table_name)
    )
```

### Key Points

| Option | Purpose |
| :--- | :--- |
| `cloudFiles.format = "csv"` | Read CSV |
| `header = "true"` | Use first row as header |
| `inferSchema = "true"` | Auto-infer types (mostly string due to CSV quirks) |
| `cloudFiles.schemaLocation` | Stores schema evolution history |
| `checkpointLocation` | Tracks ingestion progress |
| `trigger(availableNow=True)` | Process once, then stop |
| `_ingested_at` | Ingestion timestamp (added at Bronze write time) |

### Resulting Source Tables

```
z_dbt_hospital.source
├── diagnosis
├── hospital
├── patient
└── visit
```

All columns are `string`. `_ingested_at` is a `timestamp`. `_rescued_data` is present but not used downstream.

---

## 3. Bronze Layer: Schema Amendment (dbt)

The Bronze layer (`z_dbt_hospital.bronze`) amends the raw `string` columns into proper types.

### Model: `brz_diagnosis`

```sql
SELECT
    diagnosis_code,
    diagnosis_desc,
    _ingested_at
FROM {{ source('hospital_source', 'diagnosis') }}
```

### Model: `brz_hospital`

```sql
SELECT
    hospital_id,
    hospital_name,
    city,
    CAST(bed_count AS INT) AS bed_count,
    _ingested_at
FROM {{ source('hospital_source', 'hospital') }}
```

### Model: `brz_patient`

```sql
SELECT
    patient_id,
    first_name,
    last_name,
    gender,
    CAST(dob AS DATE) AS dob,
    city,
    _ingested_at
FROM {{ source('hospital_source', 'patient') }}
```

### Model: `brz_visit`

```sql
SELECT
    visit_id,
    patient_id,
    hospital_id,
    CAST(admission_date AS DATE) AS admission_date,
    CAST(discharge_date AS DATE) AS discharge_date,
    diagnosis_code,
    CAST(cost AS DOUBLE) AS cost,
    _ingested_at
FROM {{ source('hospital_source', 'visit') }}
```

### Key Points

- **`_rescued_data` is dropped** (Auto Loader fault-tolerance column).
- **`_ingested_at` is preserved** (audit column).
- **IDs remain `STRING`** because they contain prefixes (V, P, H, D).
- **Only numeric fields and dates are `CAST`.**

### `sources.yml`

```yaml
version: 2

sources:
  - name: hospital_source
    catalog: z_dbt_hospital
    schema: source
    tables:
      - name: diagnosis
      - name: hospital
      - name: patient
      - name: visit
```

---

## 4. Silver Layer: Dimensional Models (SCD1)

The Silver layer (`z_dbt_hospital.silver`) contains **3 dimensions + 1 fact table**, built with **dbt incremental models using SCD1** semantics.

### SCD1 Implementation

All Silver models use:

```sql
{{ config(
    materialized='incremental',
    unique_key='<primary_key>',
    incremental_strategy='merge'
) }}
```

- **`materialized='incremental'`**: Process only new data.
- **`unique_key`**: Identifies which row is "the same entity".
- **`incremental_strategy='merge'`**: **SCD1** — matched rows updated, unmatched rows inserted.

### Incremental Filter

```sql
{% if is_incremental() %}
WHERE _ingested_at > (
    SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }}
)
{% endif %}
```

### Deduplication

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY <primary_key>
    ORDER BY _ingested_at DESC
) = 1
```

### `dim_diagnosis`

```sql
{{ config(
    materialized='incremental',
    unique_key='diagnosis_code',
    incremental_strategy='merge'
) }}

SELECT
    diagnosis_code,
    diagnosis_desc,
    _ingested_at
FROM {{ ref('brz_diagnosis') }}
{% if is_incremental() %}
WHERE _ingested_at > (
    SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }}
)
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY diagnosis_code ORDER BY _ingested_at DESC) = 1
```

### `dim_hospital`

```sql
{{ config(
    materialized='incremental',
    unique_key='hospital_id',
    incremental_strategy='merge'
) }}

SELECT
    hospital_id,
    hospital_name,
    city,
    bed_count,
    _ingested_at
FROM {{ ref('brz_hospital') }}
{% if is_incremental() %}
WHERE _ingested_at > (
    SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }}
)
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY hospital_id ORDER BY _ingested_at DESC) = 1
```

### `dim_patient`

PII is masked: `first_name` and `last_name` are concatenated and hashed with SHA-256.

```sql
{{ config(
    materialized='incremental',
    unique_key='patient_id',
    incremental_strategy='merge'
) }}

SELECT
    patient_id,
    gender,
    dob,
    city,
    SHA2(CONCAT_WS('|', first_name, last_name), 256) AS patient_first_last_name_masked,
    _ingested_at
FROM {{ ref('brz_patient') }}
{% if is_incremental() %}
WHERE _ingested_at > (
    SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }}
)
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY _ingested_at DESC) = 1
```

**Note:** `first_name` and `last_name` are NOT selected — they are replaced by `patient_first_last_name_masked`.

### `fact_visit`

Joins `visit` with the 3 dimensions.

```sql
{{ config(
    materialized='incremental',
    unique_key='visit_id',
    incremental_strategy='merge'
) }}

WITH
    visit AS (
        SELECT *
        FROM {{ ref('brz_visit') }}
        {% if is_incremental() %}
        WHERE _ingested_at > (
            SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }}
        )
        {% endif %}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY visit_id ORDER BY _ingested_at DESC) = 1
    ),
    patient   AS (SELECT * FROM {{ ref('dim_patient') }}),
    hospital  AS (SELECT * FROM {{ ref('dim_hospital') }}),
    diagnosis AS (SELECT * FROM {{ ref('dim_diagnosis') }})

SELECT
    v.visit_id,
    v.patient_id,
    v.hospital_id,
    v.admission_date,
    v.discharge_date,
    v.diagnosis_code,
    v.cost,
    v._ingested_at,

    p.patient_first_last_name_masked,
    p.city AS patient_city,
    p.gender,
    p.dob,

    h.hospital_name,
    h.city AS hospital_city,
    h.bed_count,

    d.diagnosis_desc,
    CURRENT_TIMESTAMP() AS silver_load_timestamp
FROM visit v
LEFT JOIN patient   p ON v.patient_id    = p.patient_id
LEFT JOIN hospital  h ON v.hospital_id   = h.hospital_id
LEFT JOIN diagnosis d ON v.diagnosis_code = d.diagnosis_code
```

---

## 5. Gold Layer: Hospital Disease KPI

The Gold layer (`z_dbt_hospital.gold`) contains one KPI table: `hospital_disease_kpi`.

### Model: `hospital_disease_kpi`

Built from `fact_visit`. Computes a **30-day readmission rate** per hospital and diagnosis.

### Readmission Logic

A readmission is defined as a patient being admitted **within 30 days of a previous discharge** for the same diagnosis (CMS / Medicare standard).

Implementation:
1. For each patient (ordered by `admission_date`), use `LAG(discharge_date)` to get the previous discharge date.
2. Compute `days_since_last_visit = DATEDIFF(admission_date, previous_discharge)`.
3. Flag `is_readmission_30d = 1` when `days_since_last_visit <= 30`, otherwise `0`.

### Model Code

```sql
{{ config(materialized='table') }}

WITH visit_with_prev AS (
    SELECT
        visit_id,
        patient_id,
        hospital_id,
        admission_date,
        discharge_date,
        LAG(discharge_date) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date
        ) AS previous_discharge,
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
    FROM {{ ref('fact_visit') }}
),

visit_flagged AS (
    SELECT
        *,
        DATEDIFF(admission_date, previous_discharge) AS days_since_last_visit,
        CASE
            WHEN DATEDIFF(admission_date, previous_discharge) <= 30 THEN 1
            ELSE 0
        END AS is_readmission_30d
    FROM visit_with_prev
)

SELECT
    hospital_id,
    hospital_name,
    diagnosis_desc,
    COUNT(*) AS total_visits,
    SUM(is_readmission_30d) AS total_readmissions,
    ROUND(SUM(is_readmission_30d) * 1.0 / COUNT(*), 3) AS readmission_rate,
    SUM(cost) AS total_cost,
    AVG(cost) AS avg_cost,
    CURRENT_TIMESTAMP() AS gold_load_timestamp
FROM visit_flagged
GROUP BY hospital_id, hospital_name, diagnosis_desc
```

### Output Schema

| Column | Description |
| :--- | :--- |
| `hospital_id` | Hospital identifier |
| `hospital_name` | Hospital name |
| `diagnosis_desc` | Diagnosis description |
| `total_visits` | Number of visits for this (hospital, diagnosis) pair |
| `total_readmissions` | Visits flagged as 30-day readmissions |
| `readmission_rate` | `total_readmissions / total_visits` |
| `total_cost` | Sum of costs |
| `avg_cost` | Average cost per visit |
| `gold_load_timestamp` | Gold table generation time |

### BI Questions Answered

| # | Question | Query |
| :--- | :--- | :--- |
| 1 | Which hospital has the highest readmission rate? | `SELECT hospital_name, readmission_rate FROM hospital_disease_kpi ORDER BY readmission_rate DESC` |
| 2 | Which disease causes the most readmissions? | `SELECT diagnosis_desc, SUM(total_readmissions) FROM hospital_disease_kpi GROUP BY diagnosis_desc ORDER BY 2 DESC` |
| 3 | Which hospital performs worst for a given disease? | `SELECT * FROM hospital_disease_kpi WHERE diagnosis_desc = '<disease>' ORDER BY readmission_rate DESC` |
| 4 | Which hospital spends the most? | `SELECT hospital_name, diagnosis_desc, total_cost FROM hospital_disease_kpi ORDER BY total_cost DESC` |

---

## 6. Architecture Summary

```
Volume: /Volumes/z_dbt_hospital/source/raw_data_source/
    │
    │ Auto Loader (readStream → writeStream, availableNow=True)
    ▼
Schema: z_dbt_hospital.source      ← raw data, all string, has _ingested_at
    │
    │ dbt Bronze (brz_*)
    ▼
Schema: z_dbt_hospital.bronze      ← typed raw data
    │
    │ dbt Silver (dim_*, fact_*)
    ▼
Schema: z_dbt_hospital.silver      ← 3 dims + 1 fact
    ├── dim_diagnosis
    ├── dim_hospital
    ├── dim_patient
    └── fact_visit
    │
    │ dbt Gold (hospital_disease_kpi)
    ▼
Schema: z_dbt_hospital.gold        ← KPI table
    └── hospital_disease_kpi
```

---

## 7. Key Design Decisions

| Decision | Rationale |
| :--- | :--- |
| **Auto Loader writes to `source`, not `bronze`** | Keeps "raw ingestion" separate from "typed/amended data" |
| **`_ingested_at` added at Bronze write time** | Audit column + deduplication ordering key |
| **`_rescued_data` dropped** | Auto Loader fault-tolerance column, no business value |
| **IDs remain `STRING`** | Contain prefixes (V/P/H/D), not pure numbers |
| **dbt Bronze = type casting only** | Separation of concerns |
| **Silver uses SCD1 (merge)** | No history required for this dataset |
| **`incremental_strategy='merge'`** | Makes it SCD1, not append |
| **Patient name hashed with SHA-256** | PII protection |
| **Gold uses LAG + DATEDIFF** | CMS-standard 30-day readmission window |

---

## 8. Differences from the Reference Implementation

This project follows the same Medallion architecture as the reference tutorial (Databricks notebooks + MERGE), but applies **dbt** for the transformation layer.

### 8.1 Transformation Tool: dbt instead of Databricks Notebooks

| Aspect | Reference | This Project |
| :--- | :--- | :--- |
| **Transform language** | PySpark + DeltaTable.merge() | SQL + dbt incremental models |
| **Version control** | Notebook (JSON) | Plain `.sql` files (Git-friendly) |
| **Testing** | Manual | `dbt test` (built-in) |
| **Docs** | Manual | `dbt docs generate` |
| **Dependency management** | Manual | Automatic (`{{ ref(...) }}`) |
| **Incremental logic** | `foreachBatch` + checkpoint | `incremental_strategy='merge'` |

### 8.2 SCD1 Implementation

| Aspect | Reference | This Project |
| :--- | :--- | :--- |
| **Dedup method** | `dropDuplicates([key])` | `ROW_NUMBER() OVER (PARTITION BY key ORDER BY _ingested_at DESC) = 1` |
| **Upsert method** | `DeltaTable.merge(...)` | dbt `incremental_strategy='merge'` |
| **Incremental trigger** | `readStream` (streaming) | `{% if is_incremental() %}` (batch) |

> **Under the hood:** dbt's `incremental_strategy='merge'` compiles to a Delta Lake `MERGE INTO` statement — the same primitive used in the reference implementation (via `DeltaTable.merge()`). The difference is that dbt *declares* this behavior once via config, rather than writing the merge logic imperatively per model.
>
> Reference: [Databricks docs — Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)

### 8.3 Improvement: Fact Table Deduplication

**Reference:** `fact_visit` is built by joining `visit` with the 3 dimensions, then MERGEd. **No explicit deduplication** on `visit_id`.

**This project:** adds:

```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY visit_id ORDER BY _ingested_at DESC) = 1
```

**Why:** if Auto Loader re-runs, `brz_visit` may contain duplicate `visit_id`. Without dedup, `fact_visit` would have duplicates.

**Conclusion:** the reference is **pedagogically simplified**. This project adopts a **production-oriented** approach.

---

## 9. Tools Used

| Tool | Purpose |
| :--- | :--- |
| **Databricks Volumes** | Store raw CSV files |
| **Auto Loader (cloudFiles)** | Incremental CSV ingestion |
| **Delta Lake** | Storage format |
| **dbt (dbt-databricks)** | SQL transformations, SCD1, tests, docs |
| **Databricks SQL Warehouse** | Query engine for dbt |

---

## 10. How to Run

```bash
# 1. Ingest raw CSVs into `source` schema
# (Run the Auto Loader notebook once)

# 2. Run dbt Bronze models
dbt run --select bronze

# 3. Run dbt Silver models
dbt run --select silver

# 4. Run dbt Gold models
dbt run --select gold

# 5. Test
dbt test

# 6. Generate docs
dbt docs generate
dbt docs serve
```

---

## Reference Tutorial

- [Healthcare End-to-End Data Engineering Project](https://www.youtube.com/watch?v=sNCaDZZZmAs&t=6186s)