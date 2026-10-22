# LLM / agent context — CRISP hybrid search

Read this before changing the repo. Human-facing goals live in [README.md](README.md). This file is the working memory for later agents.

## Mission

Build a Databricks demo: synthetic clinical data lands in Unity Catalog Delta, embedding **source text** is prepared, vectors are generated later, a **subset** is served in Lakebase Postgres, and the app does **keyword + semantic (hybrid)** search.

Do not treat this as a production EHR. All data is synthetic. Catalog name `crisp` is a Maryland HIE-style demo brand, not a connection to real CRISP.

## Non-negotiables

- Databricks profile: **`DEFAULT`** (`https://adb-3011697725699826.6.azuredatabricks.net`). Do not auto-pick another profile.
- Unity Catalog: **`crisp.bronze`** (catalog and schema already created in this workspace).
- Notebooks run **in Databricks**. Use injected `spark` and `dbutils`. Never create `SparkSession` / `DatabricksSession`.
- Serverless-safe Spark only: no `.cache()`, `.persist()`, or `spark.sparkContext.broadcast()`.
- At most **one** `embedding` column per table. It is **STRING source text**, not `ARRAY<FLOAT>`. Concatenate multiple source fields into that one column.
- Do not embed `patients` or `orders`. Those are filters / keyword fields.
- Lakehouse is source of truth. Lakebase gets a **curated subset**, not 20M lab rows by default.
- Lakebase today is **Autoscaling** (`databricks postgres` / `w.postgres`). Do not create retired Provisioned instances (`databricks database`).
- Synced tables in Postgres are **read-only**. Do not UPDATE/DELETE them; copy into an app-owned table if you need `vector` columns and custom indexes.
- Enable **Change Data Feed** on Delta tables that may later sync to Lakebase (already set on generated tables).
- Keep local notebook and workspace notebook in sync: import with `--format SOURCE --language PYTHON --overwrite`.
- Do not commit secrets. Do not invent catalog/schema names.

## Repo layout

```text
README.md                 # human demo narrative
README.llm.md             # this file
notebooks/
  01_generate_bronze_clinical_data.py   # Databricks SOURCE notebook
```

Workspace copy (already imported):

`/Users/scott.black@databricks.com/hybrid_search/01_generate_bronze_clinical_data`

Local path:

`/Users/scott.black/git/hybrid_search/notebooks/01_generate_bronze_clinical_data.py`

Re-import after edits:

```bash
databricks workspace import \
  "/Users/scott.black@databricks.com/hybrid_search/01_generate_bronze_clinical_data" \
  --file "/Users/scott.black/git/hybrid_search/notebooks/01_generate_bronze_clinical_data.py" \
  --format SOURCE --language PYTHON --overwrite --profile DEFAULT
```

## What already exists

| Item | Status |
|---|---|
| UC catalog `crisp`, schema `bronze` | Created via CLI on DEFAULT |
| Generator notebook | Written + imported; may not have been **run** yet |
| Delta tables | Created only after the notebook is executed |
| Numeric embedding vectors | **Not** generated |
| Lakebase project | **Not** provisioned |
| Search app | **Not** started |

If tables are missing, run the notebook before building Vector Search or Lakebase sync. Full scale is ~10–20 minutes on serverless.

## Data model (after notebook run)

Grain and FKs:

```text
patients (patient_id)
  └── encounters (encounter_id, patient_id)
        ├── diagnoses (diagnosis_id, encounter_id, patient_id)
        └── orders (order_id, encounter_id, patient_id)
              └── order_details (order_detail_id, order_id, encounter_id, patient_id)
```

Default volumes (widgets can shrink for smoke tests):

| Table | Default rows | Notes |
|---|---:|---|
| `crisp.bronze.patients` | 2,000,000 | Includes `last_visited`, `next_scheduled_visit` |
| `crisp.bronze.encounters` | 8,000,000 | 80% of rows map to 20% high-utilizer patients |
| `crisp.bronze.orders` | 8,000,000 | Lab orders; 200 ordering providers |
| `crisp.bronze.order_details` | 20,000,000 | 75 lab tests (LOINC catalog) |
| `crisp.bronze.diagnoses` | 12,000,000 | ICD-10 + `clinical_note` |
| `crisp.bronze.v_frederick_delayed_labs` | view | Outreach slice for the demo story |

`patient_idx` is written on `patients` and `encounters` as a generator helper; it is not required for search serving.

### `patients` columns

`patient_id`, `first_name`, `last_name`, `full_name`, `date_of_birth`, `sex`, `zip_code`, `city`, `state`, `insurance_plan`, `primary_facility`, `last_visited`, `next_scheduled_visit`, `is_high_utilizer`, `patient_idx`

- `last_visited`: DATE in last 5 years; **50% in last 3 years**, 50% in years 3–5.
- `AS_OF` in the notebook is **2026-10-21**. Keep date logic consistent with that unless the user changes it.
- Maryland geography (zips/cities) and Maryland-area facilities.

### `encounters` columns

`encounter_id`, `patient_id`, `encounter_date`, `encounter_facility`, `encounter_type`, `chief_complaint`, `discharge_disposition`, `embedding`, `encounter_idx`, `patient_idx`

`embedding` = `chief_complaint`

### `orders` columns

`order_id`, `encounter_id`, `patient_id`, `order_datetime`, `ordering_provider`, `order_type`, `order_status`, `ordering_facility`

No embedding column. `ordering_provider` is picked from a 200-name list (80 hand-seeded names, then filled from `FIRST_NAMES`/`LAST_NAMES`).

### `order_details` columns

`order_detail_id`, `order_id`, `patient_id`, `encounter_id`, `loinc_code`, `test_name`, `specimen_type`, `sample_id`, `collected_at`, `resulted_at`, `result_value`, `result_unit`, `result_flag`, `result_comment`, `embedding`

`embedding` = `concat_ws(". ", test_name, result_comment)`

Lab catalog: 75 tests (`N_LAB_TESTS == 75`). Common metabolic/cardiac tests are over-weighted; remaining ~32% sample the full catalog.

### `diagnoses` columns

`diagnosis_id`, `encounter_id`, `patient_id`, `diagnosis_date`, `icd10_code`, `diagnosis_description`, `diagnosis_type`, `dx_family`, `clinical_note`, `embedding`

`embedding` = `concat_ws(". ", diagnosis_description, clinical_note)`

## Demo story (keep it in the data)

Frederick Health Hospital, **2026-03-01 through 2026-03-21**: lab courier disruption.

- Orders at that facility in the window have longer TAT, more `Pending`, and `result_comment` text containing **courier disruption**.
- Diagnoses with ICD `E11*` at Frederick mention delayed A1c/glucose.
- High utilizers are older, drive ~80% of child-table rows, and have inflated A1c/glucose/creatinine/BNP-style results.
- Use this for outreach queries: delayed labs + upcoming `next_scheduled_visit`.

Do not flatten distributions or remove this incident without an explicit user request.

## Search architecture (intended, not all built)

**Keyword / BM25 / Postgres FTS / trigram:** names, ICD-10, LOINC, IDs, facilities, exact phrases.

**Semantic:** vectors from `embedding` text on encounters, order_details, diagnoses.

**Filters (do not embed):** dates, facilities, `result_flag`, `order_status`, `dx_family`, `patient_id`, insurance, `is_high_utilizer`.

**Hybrid:** filter first, retrieve keyword + vector candidates, fuse scores, return patient-joined hits.

Possible serving grains (choose explicitly with the user before generating 40M vectors):

1. Diagnosis documents (best clinical narrative).
2. Lab-result documents (delay / outreach story).
3. Encounter complaints (optional).

Do **not** default to embedding every `order_details` row at full 20M scale. Prefer a subset: delayed Frederick labs, abnormal/critical flags, recent visits, or a sampled gold set plus the incident cohort.

## Suggested next work (in order)

1. Confirm DEFAULT profile; run generator notebook; validate row counts and `last_visited` 50%/5y checks.
2. Choose serving subset SQL (start from `v_frederick_delayed_labs` + upcoming visits, plus a broader recent-abnormal sample if needed).
3. Materialize a gold/search schema table with: PK, `patient_id`, source table, filter columns, `embedding` text, later `embedding_vector`.
4. Generate vectors (Databricks embedding endpoint, e.g. `databricks-gte-large-en`, or batch `ai_query`). Store `ARRAY<FLOAT>` or sync text and let Vector Search manage embeddings — pick one path and document it.
5. Create Lakebase Autoscaling project (`databricks postgres create-project`). Enable `vector` extension.
6. Sync or copy the **subset** into Lakebase. If using synced tables, copy into an owned table before adding pgvector indexes.
7. Indexes: GIN/FTS and/or `pg_trgm` on text; `ivfflat` or `hnsw` on vector.
8. Hybrid query function + thin API or Databricks App.

## Databricks / CLI notes for agents

- CLI floor: unified Databricks CLI ≥ 1.0 (this machine has been using 1.21.0).
- Lakebase CLI group: `databricks postgres` (not `database`).
- UC create: `databricks catalogs create --json '{"name":"crisp"}'` / schemas via `--json`.
- SQL from CLI: `databricks experimental aitools tools query` if present; else warehouse + SDK.
- Always pass `--profile DEFAULT`.

## Coding conventions in this repo

- Databricks SOURCE notebooks: `# Databricks notebook source`, cells split by `# COMMAND ----------`, markdown via `# MAGIC %md`.
- Fully qualify `crisp.bronze.table`; do not rely on `USE CATALOG`.
- Deterministic hashes + `F.rand(integer_seed)` — **integer seeds only**, never a Column.
- A `F.rand`/`F.randn` draw referenced more than once (multiple `.when` branches, or reused across columns) must be materialized with `.withColumn("_x_r", F.rand(n))` and read via `F.col`. Inline reuse re-evaluates per reference and short-circuited `CASE` branches drift (verified on serverless: 50/25/25 became 50/37.5/12.5).
- Timestamps: use `F.timestamp_seconds(...)`, not `F.from_unixtime` (returns STRING).
- `pick_from` is 1-based `element_at` on a Python list via `F.lit(values)`.
- Overwrite Delta with `.option("overwriteSchema", "true")`.
- After write: CDF `delta.enableChangeDataFeed = true`, then `CLUSTER BY` patient-oriented keys.

## Do not regress

- Do not drop `embedding` source columns or split them into multiple vector-source columns.
- Do not add Spark session setup “for local runs” unless the user asks for Databricks Connect.
- Do not sync unsanitized STRING columns to Lakebase if they could contain NUL bytes (`0x00`).
- Do not delete the `crisp` catalog or Lakebase project without explicit user permission.
- Do not force-push or commit `.env` / tokens.
