# Healthcare Data Pipeline with Databricks + dbt

An end-to-end data pipeline for a healthcare dataset, built on the **Medallion architecture** (Source → Bronze → Silver).

The pipeline:
- Ingests 4 CSV sources (diagnosis, hospital, patient, visit) from a Databricks Volume.
- Loads them into a `source` schema using **Auto Loader**.
- Amends schema and data types in a **Bronze** layer (dbt).
- Builds dimensional models (SCD1) in a **Silver** layer (dbt).

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
| visit | 10 | visit_id (V1001–V1010) | Visit details: patient, hospital, diagnosis, cost, dates |

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

All columns are `string` (Auto Loader keeps CSV as strings). `_ingested_at` is a `timestamp`. `_rescued_data` is present but not used downstream.

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

- **`_rescued_data` is dropped** (it is Auto Loader's fault-tolerance column).
- **`_ingested_at` is preserved** (audit column).
- **IDs (`visit_id`, `patient_id`, `hospital_id`, `diagnosis_code`) remain `STRING`** because they contain prefixes (V, P, H, D).
- **Only numeric fields (`bed_count`, `cost`) and dates are `CAST`.**

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
- **`incremental_strategy='merge'`**: **This is what makes it SCD1** — matched rows are updated, unmatched rows are inserted.

### Incremental Filter

```sql
{% if is_incremental() %}
WHERE _ingested_at > (
    SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }}
)
{% endif %}
```

Only rows with `_ingested_at` newer than the last processed are considered.

### Deduplication

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY <primary_key>
    ORDER BY _ingested_at DESC
) = 1
```

If the same key appears multiple times in Bronze, only the latest record is kept.

### `slv_dim_diagnosis`

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

### `slv_dim_hospital`

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

### `slv_dim_patient`

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

### `slv_fact_visit`

Joins `visit` with the 3 dimensions. Only masked patient name is exposed.

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
    patient   AS (SELECT * FROM {{ ref('slv_dim_patient') }}),
    hospital  AS (SELECT * FROM {{ ref('slv_dim_hospital') }}),
    diagnosis AS (SELECT * FROM {{ ref('slv_dim_diagnosis') }})

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

## 5. Architecture Summary

```
Volume: /Volumes/z_dbt_hospital/source/raw_data_source/
    │
    │ Auto Loader (readStream → writeStream, availableNow=True)
    ▼
Schema: z_dbt_hospital.source      ← raw data, all string, has _ingested_at
    │
    │ dbt Bronze (brz_*)
    │ - Drop _rescued_data
    │ - Cast types (dates, numbers)
    ▼
Schema: z_dbt_hospital.bronze      ← typed raw data
    │
    │ dbt Silver (slv_*)
    │ - incremental + merge (SCD1)
    │ - dedupe with ROW_NUMBER
    │ - PII masking (SHA-256)
    │ - dimensional joins
    ▼
Schema: z_dbt_hospital.silver      ← 3 dims + 1 fact
    ├── slv_dim_diagnosis
    ├── slv_dim_hospital
    ├── slv_dim_patient
    └── slv_fact_visit
```

---

## 6. Key Design Decisions

| Decision | Rationale |
| :--- | :--- |
| **Auto Loader writes to `source`, not `bronze`** | Keeps "raw ingestion" separate from "typed/amended data" |
| **`_ingested_at` added at Bronze write time** | Provides an audit column and deduplication ordering key |
| **`_rescued_data` dropped** | It's an Auto Loader fault-tolerance column with no business value |
| **IDs remain `STRING`** | They contain prefixes (V/P/H/D), not pure numbers |
| **dbt Bronze = type casting only** | Separation of concerns: Bronze amends, Silver models |
| **Silver uses SCD1 (merge)** | Historical tracking is not required for this dataset |
| **`incremental_strategy='merge'`** | This is what makes it SCD1, not append |
| **Patient name is hashed with SHA-256** | PII protection |

---

## 7. Differences from the Reference Implementation

This project follows the same Medallion architecture as the reference tutorial (Databricks notebooks + MERGE), but applies **dbt** for the transformation layer and makes a few deliberate improvements.

### 7.1 Transformation Tool: dbt instead of Databricks Notebooks

| Aspect | Reference (Databricks Notebooks) | This Project (dbt) |
| :--- | :--- | :--- |
| **Transform language** | PySpark + DeltaTable.merge() | SQL + dbt incremental models |
| **Version control** | Notebook (JSON) | Plain `.sql` files (Git-friendly) |
| **Testing** | Manual / ad-hoc | `dbt test` (built-in) |
| **Docs** | Manual | `dbt docs generate` (auto-generated) |
| **Dependency management** | Manual (`spark.read.table(...)`) | Automatic (`{{ ref(...) }}`) |
| **Incremental logic** | `foreachBatch` + checkpoint | `incremental_strategy='merge'` |

### 7.2 SCD1 Implementation: MERGE Strategy

Both implementations use **SCD1** (last-write-wins):

| Aspect | Reference | This Project |
| :--- | :--- | :--- |
| **Dedup method** | `dropDuplicates([key])` | `ROW_NUMBER() OVER (PARTITION BY key ORDER BY _ingested_at DESC) = 1` |
| **Upsert method** | `DeltaTable.merge(...).whenMatchedUpdateAll().whenNotMatchedInsertAll()` | dbt `incremental_strategy='merge'` |
| **Incremental trigger** | `readStream` (streaming) | `{% if is_incremental() %}` (batch) |

### 7.3 Improvement: Fact Table Deduplication

**Reference implementation:** `fact_visit` is built by joining `visit` with the 3 dimensions, then MERGEd into the target. **No explicit deduplication** is performed on `visit_id`.

**This project:** an explicit deduplication step is added:

```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY visit_id ORDER BY _ingested_at DESC) = 1
```

**Why this matters:**
- If Auto Loader re-runs (e.g., checkpoint reset), the Bronze `visit_raw` table may contain duplicate `visit_id` rows.
- Without deduplication, the Silver `fact_visit` table would contain duplicates.
- The reference implementation implicitly assumes the source is clean; this project defends against it.

**Conclusion:** The reference implementation is **pedagogically simplified** (small, clean dataset). This project adopts a **production-oriented** approach by adding deduplication even on the fact table.

---

## 8. Tools Used

| Tool | Purpose |
| :--- | :--- |
| **Databricks Volumes** | Store raw CSV files |
| **Auto Loader (cloudFiles)** | Incremental CSV ingestion |
| **Delta Lake** | Storage format |
| **dbt (dbt-databricks)** | SQL transformations, SCD1, tests, docs |
| **Databricks SQL Warehouse** | Query engine for dbt |

---

## 9. How to Run

```bash
# 1. Ingest raw CSVs into `source` schema
# (Run the Auto Loader notebook once)

# 2. Run dbt Bronze models
dbt run --select bronze

# 3. Run dbt Silver models
dbt run --select silver

# 4. Test
dbt test

# 5. Generate docs
dbt docs generate
dbt docs serve
```

---

## Reference Tutorial

This project is based on / inspired by the following YouTube tutorial:

- [Healthcare End-to-End Data Engineering Project](https://www.youtube.com/watch?v=sNCaDZZZmAs&t=6186s)