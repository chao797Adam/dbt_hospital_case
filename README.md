# Healthcare Data Pipeline with Databricks + dbt

An end-to-end data pipeline for a healthcare dataset, built on the **Medallion architecture** (Source → Bronze → Silver → Gold).

The pipeline:
- Ingests 4 CSV sources (diagnosis, hospital, patient, visit) from a Databricks Volume.
- Loads them into a `source` schema using **Auto Loader**.
- Amends schema and data types in a **Bronze** layer (dbt).
- Builds dimensional models (SCD1) in a **Silver** layer (dbt).
- Maintains patient attribute history using a **dbt Snapshot (SCD2)** alongside the current-state Silver dimension.
- Produces a KPI table for 30-day readmission analysis in a **Gold** layer (dbt).

---

## Business Context

**Organization:** ApexLife Hospitals — a multi-hospital network across several cities, serving thousands of patients every month.

**Problem:** The network has observed a growing issue of patients being **readmitted within 30 days of discharge**. These readmissions create multiple challenges:

1. **Financial Impact**
   Some readmissions may affect reimbursement under applicable payment and quality programs; not every readmission is avoidable or a treatment failure.

2. **Operational Pressure**
   Readmissions reduce bed availability for new patients.

3. **Patient Outcomes & Experience**
   High readmission rates impact patient trust in the hospital.

**Goal of this project:** Build an end-to-end data pipeline that identifies **30-day readmissions** across hospitals and diagnoses, so that the hospital network can monitor and reduce them.

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
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", schema_loc)
        .load(s["src"])
    )

    query = (df
        .withColumn("_ingested_at", current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table_name)
    )
    query.awaitTermination()
```

### Key Points

| Option | Purpose |
| :--- | :--- |
| `cloudFiles.format = "csv"` | Read CSV |
| `header = "true"` | Use first row as header |
| `cloudFiles.inferColumnTypes = "false"` | Keep CSV fields as strings for explicit downstream casting |
| `cloudFiles.schemaLocation` | Stores schema evolution history |
| `checkpointLocation` | Tracks ingestion progress |
| `trigger(availableNow=True)` | Process once, then stop |
| `_ingested_at` | Ingestion timestamp (added at Source ingestion time) |

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
    catalog: "{{ target.catalog }}"
    schema: source
    tables:
      - name: diagnosis
      - name: hospital
      - name: patient
      - name: visit
```

### Bronze Layer Validation — Dynamic Table Inspection

A reusable PySpark utility is used to dynamically discover and inspect all tables within a given catalog and schema, without manually specifying table names.

```python
catalog = "z_dbt_hospital"
schema = "source"

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

for table in tables:
    full_name = f"{catalog}.{schema}.{table.tableName}"
    df = spark.table(full_name)

    print("=" * 80)
    print(f"Table: {full_name}")
    print(f"Row Count: {df.count()}")
    print("=" * 80)

    display(spark.sql(f"DESCRIBE TABLE {full_name}"))
    display(df)
```

#### Validation Objectives

| Validation | Purpose |
| :--- | :--- |
| Dynamic Table Discovery | Automatically discovers tables without hardcoded table names |
| Row Count | Supports row count reconciliation between layers |
| Schema Inspection | Verifies data types after Bronze transformations |
| Data Preview | Supports manual inspection of transformed records |
| Reusability | Works across different catalogs, schemas and projects |

#### Cross-Layer Reusability

The same utility can be reused by changing the `schema` parameter.

| Schema | Validation Purpose |
| :--- | :--- |
| `source` | Raw ingestion inspection |
| `bronze` | Data type validation |
| `silver` | Dimensional model inspection |
| `gold` | Business KPI inspection |
| `snapshots` | SCD2 historical version inspection |

The `catalog` parameter can also be changed between development and production environments.

#### Execution Evidence

The following screenshot demonstrates the dynamic table inspection utility running against the Source layer in Databricks.

![Dynamic Table Inspection](assets/dynamic_table_inspection.png)

The inspection of `source.diagnosis` confirms:

- Row count: 5 records.
- Raw business columns are stored as STRING.
- `_ingested_at` is stored as TIMESTAMP.
- `_rescued_data` is NULL for all five records.
- Table metadata, schema and data preview are displayed automatically.

The same utility is reused to inspect the Bronze, Silver, Gold and Snapshot layers.

**Design Rationale:** This metadata-driven approach reduces repetitive SQL queries and provides a consistent inspection method across pipeline layers and environments.

**Limitation:** This utility is intended for interactive development and manual inspection. For large production tables, full row counts can be expensive. Data previews should be limited using `display(df.limit(20))`. Automated data quality enforcement is handled separately through dbt tests.

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

- **`materialized='incremental'`**: Enables incremental execution; the explicit filter selects incoming rows.
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

Patient names are pseudonymized: `first_name` and `last_name` are concatenated and hashed with SHA-256.

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

The Gold layer (`z_dbt_hospital.gold`) contains one KPI table: `gold_hospital_disease_kpi`.

### Model: `gold_hospital_disease_kpi`

Built from `fact_visit`. Computes a **30-day readmission rate** per hospital and diagnosis.

### Readmission Logic

For this demonstration, a readmission is a visit starting 0–30 days after the immediately preceding visit for the same patient and diagnosis. This is a simplified visit-based indicator, not an official CMS risk-adjusted readmission measure. Cross-hospital readmissions are included and attributed to the receiving hospital.

Implementation:
1. For each patient and diagnosis (ordered by `admission_date`, then `visit_id`), use `LAG(discharge_date)` to get the previous discharge date.
2. Compute `days_since_last_visit = DATEDIFF(admission_date, previous_discharge)`.
3. Flag `is_readmission_30d = 1` when `days_since_last_visit` is between 0 and 30 inclusive, otherwise `0`.

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
            PARTITION BY patient_id, diagnosis_code
            ORDER BY admission_date, visit_id
        ) AS previous_discharge,
        diagnosis_code,
        cost,
        _ingested_at,
        patient_first_last_name_masked,
        patient_city,
        gender,
        dob,
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
            WHEN DATEDIFF(admission_date, previous_discharge)
            BETWEEN 0 AND 30
            THEN 1
            ELSE 0
        END AS is_readmission_30d
    FROM visit_with_prev
)

SELECT
    hospital_id,
    hospital_name,
    diagnosis_code,
    diagnosis_desc,
    COUNT(*) AS total_visits,
    SUM(is_readmission_30d) AS total_readmissions,
    SUM(is_readmission_30d) * 1.0 / COUNT(*) AS readmission_rate,
    SUM(cost) AS total_cost,
    AVG(cost) AS avg_cost,
    CURRENT_TIMESTAMP() AS gold_load_timestamp
FROM visit_flagged
GROUP BY hospital_id, hospital_name, diagnosis_code, diagnosis_desc
```

### Output Schema

| Column | Description |
| :--- | :--- |
| `hospital_id` | Hospital identifier |
| `hospital_name` | Hospital name |
| `diagnosis_code` | Diagnosis identifier |
| `diagnosis_desc` | Diagnosis description |
| `total_visits` | Number of visits for this (hospital, diagnosis) pair |
| `total_readmissions` | Visits flagged as 30-day readmissions |
| `readmission_rate` | `total_readmissions / total_visits` |
| `total_cost` | Sum of costs |
| `avg_cost` | Average cost per visit |
| `gold_load_timestamp` | Gold table generation time |

### BI Questions Answered

The Gold grain is hospital and diagnosis (assuming consistent dimension labels). Hospital-level rates must be calculated from summed counts, not averaged diagnosis-level rates. The following revised queries should be rerun against the updated Gold model; previous sample results are not treated as verified results for the revised logic.

#### Q1: Which hospital has the highest readmission rate?

```sql
SELECT hospital_id, hospital_name,
       SUM(total_readmissions) AS total_readmissions,
       SUM(total_visits) AS total_visits,
       ROUND(SUM(total_readmissions) * 1.0 / NULLIF(SUM(total_visits), 0), 3) AS readmission_rate
FROM z_dbt_hospital.gold.gold_hospital_disease_kpi
GROUP BY hospital_id, hospital_name
ORDER BY readmission_rate DESC;
```

#### Q2: Which disease has the most readmissions?

```sql
SELECT diagnosis_code, diagnosis_desc, SUM(total_readmissions) AS total_readm
FROM z_dbt_hospital.gold.gold_hospital_disease_kpi
GROUP BY diagnosis_code, diagnosis_desc
ORDER BY total_readm DESC;
```

#### Q3: Which hospital has the highest rate for Chest Pain?

```sql
SELECT *
FROM z_dbt_hospital.gold.gold_hospital_disease_kpi
WHERE diagnosis_desc = 'Chest Pain'
ORDER BY readmission_rate DESC;
```

#### Q4: Which hospital spends the most?

```sql
SELECT hospital_id, hospital_name, SUM(total_cost) AS total_cost
FROM z_dbt_hospital.gold.gold_hospital_disease_kpi
GROUP BY hospital_id, hospital_name
ORDER BY total_cost DESC;
```

The reference tutorial uses `diagnosis_category`, which is not present in this dataset. This project uses diagnosis codes and descriptions. With only eight original visits, these results illustrate the pipeline rather than establish clinical performance.

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
    ├── dim_patient ── dbt snapshot ──► snapshots.dim_patient_snapshot (SCD2)
    └── fact_visit
    │
    │ dbt Gold (gold_hospital_disease_kpi)
    ▼
Schema: z_dbt_hospital.gold        ← KPI table
    └── gold_hospital_disease_kpi
```

---

## 7. Key Design Decisions

| Decision | Rationale |
| :--- | :--- |
| **Auto Loader writes to `source`, not `bronze`** | Keeps "raw ingestion" separate from "typed/amended data" |
| **`_ingested_at` added at Source ingestion time** | Audit column + deduplication ordering key |
| **`_rescued_data` dropped** | Not selected downstream; retain in Source for investigation |
| **IDs remain `STRING`** | Contain prefixes (V/P/H/D), not pure numbers |
| **dbt Bronze = type casting only** | Separation of concerns |
| **Silver uses SCD1 (merge)** | Current-state dimensions; patient history is maintained separately |
| **`incremental_strategy='merge'`** | Makes it SCD1, not append |
| **Patient name hashed with SHA-256** | Pseudonymization, not anonymization |
| **Patient SCD2 snapshot** | Track selected attribute changes without changing the current-state fact pipeline |
| **Gold uses LAG + DATEDIFF** | Project-specific 0–30-day readmission window |

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

#### SCD1 and SCD2 coexist in this project

The three Silver dimensions retain one current row per business key using incremental MERGE. `fact_visit` retains one row per visit and supports corrections through upsert. The patient snapshot separately records historical versions of selected patient attributes. Hospital and diagnosis history are outside the current snapshot scope.

#### SCD2 in dbt: `dim_patient_snapshot`

The snapshot reads the deduplicated, pseudonymized `dim_patient` rather than the append-only raw source. This prevents multiple source rows per patient from reaching the snapshot in one run.

| Strategy | Requirement | Change detection |
| :--- | :--- | :--- |
| `timestamp` | Reliable update timestamp | Newer update timestamp |
| `check` | Selected attributes | Comparison of attribute values |

The source has no business `updated_at`, so the project uses `check`. `_ingested_at` is retained for lineage but excluded from change detection.

**File:** `snapshots/dim_patient_snapshot.sql`

```sql
{% snapshot dim_patient_snapshot %}

{{ config(
    target_database=target.catalog,
    target_schema='snapshots',
    unique_key='patient_id',
    strategy='check',
    check_cols=['city', 'gender', 'dob', 'patient_first_last_name_masked']
) }}

SELECT
    patient_id,
    gender,
    dob,
    city,
    patient_first_last_name_masked,
    _ingested_at
FROM {{ ref('dim_patient') }}

{% endsnapshot %}
```

The SQL snapshot uses the legacy target configuration syntax; confirm the resolved catalog and schema with the installed dbt-databricks version before running in prod.

```bash
dbt run --target dev --select +dim_patient
dbt snapshot --target dev --select dim_patient_snapshot
```

| Column | Purpose |
| :--- | :--- |
| `dbt_scd_id` | Identifier for a historical version |
| `dbt_updated_at` | Snapshot change timestamp |
| `dbt_valid_from` | Start of observed version |
| `dbt_valid_to` | End of observed version; NULL means current |

New patients are inserted; changed attributes close the previous version and create a new current version. Unchanged attributes do not create a new version.

#### SCD2 validation procedure

Run the initial snapshot, then append a controlled patient-city change in dev. Use explicit column names because the source may contain `_rescued_data`.

```sql
INSERT INTO z_dbt_hospital.source.patient
    (patient_id, first_name, last_name, gender, dob, city, _ingested_at)
SELECT patient_id, first_name, last_name, gender, dob,
       CONCAT(city, ' - SCD2 test'), CURRENT_TIMESTAMP()
FROM z_dbt_hospital.source.patient
WHERE patient_id = 'P001'
QUALIFY ROW_NUMBER() OVER (ORDER BY _ingested_at DESC) = 1;
```

```bash
dbt run --target dev --select +dim_patient
dbt snapshot --target dev --select dim_patient_snapshot
```

```sql
SELECT patient_id, city, dbt_valid_from, dbt_valid_to
FROM z_dbt_hospital.snapshots.dim_patient_snapshot
WHERE patient_id = 'P001'
ORDER BY dbt_valid_from;
```

Expected result for a clean initial test: one closed version and one current version. Rerunning without attribute changes should not create another version. Actual execution results and screenshots should be captured during the next rerun.

#### Snapshot limitations

- Snapshot validity reflects observation time, not the actual business-effective time.
- Intermediate changes overwritten in SCD1 between snapshot runs are not captured. Historical backfill from retained Source observations would be a separate process.
- The fact does not join to historical patient versions; this snapshot is an independent history table, not a point-in-time fact enrichment.
- Hashing names does not anonymize patient IDs, DOB or other identifying attributes. Restrict access to Source, Bronze and snapshots; use synthetic data for public demonstrations.


### 8.3 Improvement: Fact Table Deduplication

**Reference:** `fact_visit` is built by joining `visit` with the 3 dimensions, then MERGEd. **No explicit deduplication** on `visit_id`.

**This project:** adds:

```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY visit_id ORDER BY _ingested_at DESC) = 1
```

**Why:** different files may contain repeated or corrected visit IDs. Auto Loader checkpointing tracks files, not business-key uniqueness. Multiple source rows for one key can cause ambiguous MERGE matches or duplicate inserts.

**Conclusion:** the reference is **pedagogically simplified**. This project explicitly deduplicates incoming business keys, although equal timestamps still require a reliable tie-breaker.

### 8.4 Job Orchestration: dbt run vs. Asset Bundle

The reference implementation uses **Databricks Asset Bundle** (`reference_bundle_job_file.yml`) to define an end-to-end Job with 8 Notebook tasks and `depends_on` dependencies.

This project uses **dbt CLI and its dependency DAG** for transformation execution. External scheduling of ingestion and dbt is separate:

```bash
dbt run --select bronze
dbt run --select silver
dbt snapshot
dbt run --select gold
dbt test
```

| Aspect | Reference (Asset Bundle) | This Project (dbt run) |
| :--- | :--- | :--- |
| **Definition** | YAML (`reference_bundle_job_file.yml`) | CLI commands |
| **Dependencies** | `depends_on` in YAML | Implicit in `{{ ref() }}` DAG |
| **Deployment** | `databricks bundle deploy` | Target-specific dbt CLI; automated CI/CD is not demonstrated here |
| **Testing** | Separate Notebook | `dbt test` (built-in) |

**Both approaches are valid.** Asset Bundle is suitable for teams managing many Notebook-based Jobs. dbt is suitable for SQL-heavy transformations with built-in lineage and testing.

## 9. Tools Used

| Tool | Purpose |
| :--- | :--- |
| **Databricks Volumes** | Store raw CSV files |
| **Auto Loader (cloudFiles)** | Incremental CSV ingestion |
| **Delta Lake** | Storage format |
| **dbt (dbt-databricks)** | SQL transformations, SCD1, SCD2 snapshots, tests, docs |
| **Databricks SQL Warehouse** | Query engine for dbt |

## 10. How to Run

```bash
# 1. Ingest raw CSVs into `source` schema
# (Run the Auto Loader notebook once)

# 2. Run dbt Bronze models
dbt run --select bronze

# 3. Run dbt Silver models
dbt run --select silver

# 4. Capture patient history
dbt snapshot --select dim_patient_snapshot

# 5. Run dbt Gold models
dbt run --select gold

# 6. Test
dbt test

# 7. Generate docs
dbt docs generate
dbt docs serve
```

## 11. Deployment

### 11.1 Multi-Environment Setup

The project uses **two catalogs** for isolation:

| Environment | Catalog | Purpose |
| :--- | :--- | :--- |
| **dev** | `z_dbt_hospital` | Development, ad-hoc runs |
| **prod** | `z_dbt_hospital_prod` | Scheduled / production runs |

Both targets are defined in `profiles.yml`:

```yaml
dbt_hospital:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: z_dbt_hospital
      schema: default
      ...
    prod:
      type: databricks
      catalog: z_dbt_hospital_prod
      schema: default
      ...
```

Run against a target:

```bash
dbt run --target dev
dbt run --target prod
```

### 11.2 Source Tables in prod

Since dbt's `source` tables are created by **Auto Loader** (not by dbt), the `prod` catalog requires its own `source` schema. For a one-time migration, CTAS was used:

```sql
CREATE SCHEMA IF NOT EXISTS z_dbt_hospital_prod.source;

CREATE TABLE z_dbt_hospital_prod.source.diagnosis AS SELECT * FROM z_dbt_hospital.source.diagnosis;
CREATE TABLE z_dbt_hospital_prod.source.hospital  AS SELECT * FROM z_dbt_hospital.source.hospital;
CREATE TABLE z_dbt_hospital_prod.source.patient   AS SELECT * FROM z_dbt_hospital.source.patient;
CREATE TABLE z_dbt_hospital_prod.source.visit     AS SELECT * FROM z_dbt_hospital.source.visit;
```

> **Note:** For a production-grade setup, Auto Loader should be parameterized to write to `{catalog}.source.*` and run separately per environment. CTAS is a one-time shortcut for demonstration.

### 11.3 Environment Isolation Test

To verify that `dev` and `prod` are truly isolated, a test row was inserted only into `prod`:

```sql
INSERT INTO z_dbt_hospital_prod.source.diagnosis
    (diagnosis_code, diagnosis_desc, _ingested_at)
VALUES ('D999', 'Test Disease', CURRENT_TIMESTAMP());
```

Then `dim_diagnosis` was rebuilt in `prod`:

```bash
dbt run --target prod --select +dim_diagnosis
```

**Verification:**

```sql
-- prod: contains the test row
SELECT * FROM z_dbt_hospital_prod.bronze.brz_diagnosis WHERE diagnosis_code = 'D999';
-- → 1 row

-- dev: does not contain the test row
SELECT * FROM z_dbt_hospital.bronze.brz_diagnosis WHERE diagnosis_code = 'D999';
-- → 0 rows
```

**Conclusion:** The test demonstrates separate catalog data for the tested model path. Full isolation also depends on target-aware model and snapshot configuration, permissions and ingestion paths.

### 11.4 `sources.yml` Uses `target.catalog`

To ensure dbt reads from the correct catalog per environment, `sources.yml` uses a Jinja variable:

```yaml
version: 2

sources:
  - name: hospital_source
    catalog: "{{ target.catalog }}"
    schema: source
    tables:
      - name: diagnosis
      - name: hospital
      - name: patient
      - name: visit
```

| Target | `target.catalog` | Reads from |
| :--- | :--- | :--- |
| `dev` | `z_dbt_hospital` | `z_dbt_hospital.source.*` |
| `prod` | `z_dbt_hospital_prod` | `z_dbt_hospital_prod.source.*` |

### 11.5 Deployment Commands

```bash
# Deploy to dev
dbt run --target dev
dbt snapshot --target dev
dbt test --target dev

# Deploy to prod
dbt run  --target prod
dbt snapshot --target prod
dbt test --target prod

# Optional rebuild of incremental models (not required for first run)
dbt run --target prod --full-refresh
```

## 12. Genie: Natural Language BI

The Gold table `gold_hospital_disease_kpi` and the Silver table `fact_visit` are exposed to **Databricks Genie** for natural-language BI.

### Questions & Results

Four questions were asked via Genie in the earlier implementation. The captured results are stored in the `assets/` folder; rerun and validate them against the revised KPI definition:

| # | Question | Result file |
| :--- | :--- | :--- |
| Q1 | Which hospitals have a readmission rate above 20%? | `assets/Two Hospitals Have Readmi.pdf` |
| Q2 | What is the average length of stay? | `assets/Average Length of Stay_4.pdf` |
| Q3 | Which patient had the longest stay and then was readmitted? | `assets/Kabir Menon Had the Longe.pdf` |
| Q4 | Are we improving monthly based on readmission rate? | `assets/Currently Improving_Apri.pdf` |

### Notes

- Genie auto-generates SQL from natural language, lowering the barrier for business users.
- Q3 and Q4 require visit-level dates. Q3 should use patient ID rather than raw patient names; review the existing PDF for identifying information before publishing. Genie-generated SQL must be checked against the same readmission definition.

## 13. Dashboard

A Databricks Dashboard was built on top of `gold_hospital_disease_kpi` with four visualizations:

| # | Visualization | Chart Type |
| :--- | :--- | :--- |
| 1 | Total visits per hospital | Bar chart |
| 2 | Total readmissions per hospital (by diagnosis) | Bar chart |
| 3 | Readmission rate per hospital | Bar chart |
| 4 | Total cost per hospital (by diagnosis) | Bar chart |

The dashboard is stored in the Databricks Workspace and is scheduled to send daily email snapshots.

Snapshot: `assets/dashboard.png` (earlier implementation). Refresh after rerunning the revised models. Hospital-level rates must use summed numerators and denominators.

## 14. Data Quality and Known Limitations

The original dataset contains 5 diagnosis, 5 hospital, 5 patient and 8 visit records. These are baseline source counts, not post-test counts.

Recommended dbt tests to configure and execute:

- `unique` and `not_null` on each Silver business key.
- `relationships` from fact keys to the corresponding dimensions.
- Nonnegative cost and discharge date on or after admission date.
- Exactly one current snapshot row per patient and nonoverlapping version intervals.
- Reconcile Gold `SUM(total_visits)` and `SUM(total_cost)` to Silver fact counts and costs.

The current incremental filter uses `_ingested_at > MAX(_ingested_at)`. Rows arriving at the same watermark can be missed; equal timestamps also make deduplication ambiguous. A bounded lookback with deterministic ordering and protection against older versions overwriting newer rows is a possible enhancement. Ingestion time is not a reliable business change sequence.

`fact_visit` embeds dimension attributes and only processes incoming visits. A dimension-only update therefore does not refresh existing fact rows. For current-state reporting, a future normalized fact with query-time dimension joins or explicit affected-key reprocessing would resolve this. Until then, changed dimension labels can split Gold groups. A full refresh can rebuild current-state enrichment but cannot recover historical effective attributes.

The readmission metric uses the immediately preceding same-diagnosis visit and assumes nonoverlapping episodes. It does not model transfers, planned admissions, eligibility, risk adjustment or follow-up censoring. The denominator is all observed visits, not eligible index discharges. Validate overlapping visits separately.

`_rescued_data` is retained in Source but not monitored in the current transformation. Production pipelines should inspect and quarantine malformed records. Explicit casts can fail on invalid input. The current cost type is DOUBLE; DECIMAL is preferable for exact monetary reconciliation.

The dev/prod setup demonstrates catalog separation using CTAS, not independent scheduled production ingestion. The snapshot and revised Gold SQL are documented for the updated implementation; runtime validation is to be completed during the rerun.

## Reference Tutorial

- [Healthcare End-to-End Data Engineering Project](https://www.youtube.com/watch?v=sNCaDZZZmAs&t=6186s)