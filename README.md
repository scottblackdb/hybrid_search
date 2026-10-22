# CRISP Hybrid Search Demo

This project demonstrates how Databricks Lakehouse and Lakebase can power hybrid
search over synthetic healthcare data.

The demo combines:

- Delta tables as the governed system of record
- Embeddings generated from clinical text
- A search-serving subset copied to Lakebase
- PostgreSQL keyword search for exact clinical terms and codes
- `pgvector` semantic search for conceptually similar text
- Hybrid ranking that combines keyword and semantic relevance

All patient and clinical data in this repository is synthetic.

Agents continuing this work should read [README.llm.md](README.llm.md) for workspace
paths, schema details, constraints, and the planned implementation order.

## Goal

The target experience is a low-latency clinical search application that can answer
queries such as:

- Find patients with delayed diabetes labs who have an upcoming visit.
- Find records related to poor glycemic control, even when the exact words differ.
- Find ICD-10 `E11.22`, LOINC `4548-4`, a patient name, or a specimen ID exactly.
- Find clinically similar diagnoses and lab comments while filtering by facility,
  result flag, date, or patient.

Keyword search handles exact identifiers and terminology. Semantic search handles
meaning and paraphrases. Hybrid search combines both result sets.

## Intended Data Flow

```text
Synthetic clinical data
        |
        v
Unity Catalog Delta tables (crisp.bronze)
        |
        +--> Build one embedding source string per searchable record
        |
        +--> Generate embedding vectors
        |
        v
Curated search-serving subset
        |
        v
Lakebase Postgres
        |
        +--> Keyword index (PostgreSQL full-text / trigram)
        +--> Vector index (pgvector)
        |
        v
Hybrid retrieval and ranking
```

Delta remains the authoritative data layer. Lakebase receives only the fields and
rows needed for online search, keeping the serving model smaller than the complete
Lakehouse dataset.

If Lakebase synced tables are used, they should be treated as read-only staging
tables. The searchable data can then be materialized into an application-owned
Lakebase table with a `vector` column and the required PostgreSQL indexes.

## Lakehouse Data Model

The notebook writes managed Delta tables to `crisp.bronze`:

- `patients` — 2 million patient profiles, including DOB, last visit, next
  scheduled visit, insurance, and primary facility.
- `encounters` — 8 million facility encounters connected to patients.
- `orders` — 8 million laboratory orders connected to patients and encounters.
- `order_details` — 20 million test results, specimens, result flags, and comments.
- `diagnoses` — 12 million ICD-10 diagnoses and synthetic clinical notes.

Every clinical table is directly or indirectly connected to `patients` through
`patient_id`.

## Embedding Sources

The current `embedding` columns contain **source text**, not numeric vectors.
There is at most one source column per searchable table:

- `encounters.embedding` = `chief_complaint`
- `order_details.embedding` = `test_name` + `result_comment`
- `diagnoses.embedding` = `diagnosis_description` + `clinical_note`

`patients` and `orders` do not have embedding text because their useful fields are
primarily identifiers, dates, names, and categorical filters.

A later processing step will send each source string to an embedding model and
store the resulting numeric vector in the curated serving dataset.

## Search Design

### Keyword search

Use PostgreSQL full-text search and, where useful, trigram indexes for:

- Patient and provider names
- ICD-10 and LOINC codes
- Specimen, order, and encounter IDs
- Facilities and test names
- Exact phrases in clinical notes and result comments

### Semantic search

Use `pgvector` similarity search over generated vectors for:

- Chief complaints with similar meaning
- Related diagnosis descriptions and clinical notes
- Similar lab-result narratives
- Queries whose wording differs from the stored text

### Hybrid ranking

For each query:

1. Apply structured filters such as date, facility, result flag, or patient.
2. Retrieve keyword candidates.
3. Retrieve semantic candidates.
4. Normalize and combine both scores.
5. Return the highest-ranked records with patient and encounter context.

## Demo Story

The synthetic dataset includes a March 2026 laboratory courier disruption at
Frederick Health Hospital. Some HbA1c, glucose, BNP, and related results are delayed.
This supports a demo where hybrid search identifies affected diabetes and
heart-failure patients who have upcoming visits and may require outreach.

The notebook also creates `crisp.bronze.v_frederick_delayed_labs` as a focused
Lakehouse view for this scenario.

## Repository

```text
notebooks/
  01_generate_bronze_clinical_data.py
```

`01_generate_bronze_clinical_data.py` is a Databricks source notebook. It uses the
Databricks-provided `spark` and `dbutils` objects and can run on serverless compute
or a Databricks cluster.

## Current Status

Implemented:

- Unity Catalog catalog `crisp` and schema `bronze`
- Synthetic, relational clinical data generator
- Change Data Feed enabled on generated Delta tables
- Source text columns for downstream embeddings
- Search-oriented incident and validation data

Planned:

- Generate numeric embedding vectors
- Create the curated Lakebase serving subset
- Provision Lakebase Postgres and enable `pgvector`
- Add PostgreSQL keyword and vector indexes
- Implement hybrid retrieval and score fusion
- Add a search application or API

## Running the Generator

Import or open `notebooks/01_generate_bronze_clinical_data.py` as a Databricks
notebook, attach serverless compute or a suitable cluster, review its widgets, and
run all cells.

The defaults generate the full-scale dataset in `crisp.bronze`. Reduce the row-count
widgets for a faster development smoke test.
