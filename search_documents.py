"""Builds the Lakebase hybrid search documents from CRISP bronze tables.

Shared by the full load (02_load_lakebase_search) and future CDC loads, so both produce identical rows.

- Full load: call build_search_documents(spark, source, cutoff) to rebuild from the current tables.
- CDC: pass the changed rows per source table, e.g. a Change Data Feed batch:

      changes = {"diagnoses": spark.read.option("readChangeFeed", "true")
                                   .option("startingVersion", v).table("crisp.bronze.diagnoses")
                                   .filter("_change_type != 'update_preimage'")}
      build_search_documents(spark, source, cutoff, changes=changes)

  CDF columns (_change_type, _commit_version, _commit_timestamp) are carried through so the writer can upsert
  inserts / update_postimage rows and delete `delete` rows by doc_id. Lookups (patients, orders, encounter
  facility) are always read from the current tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLES = ("encounters", "order_details", "diagnoses", "clinical_notes")
CDF_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")

# (column, Postgres type); order is the column order of the search table.
POSTGRES_COLUMNS = [
    ("doc_id", "TEXT NOT NULL"),
    ("source_table", "TEXT NOT NULL"),
    ("patient_id", "TEXT NOT NULL"),
    ("encounter_id", "TEXT"),
    ("full_name", "TEXT"),
    ("date_of_birth", "DATE"),
    ("sex", "TEXT"),
    ("zip_code", "TEXT"),
    ("insurance_plan", "TEXT"),
    ("primary_facility", "TEXT"),
    ("last_visited", "DATE"),
    ("next_scheduled_visit", "TIMESTAMPTZ"),
    ("is_high_utilizer", "BOOLEAN"),
    ("event_at", "TIMESTAMPTZ"),
    ("facility", "TEXT"),
    ("doc_type", "TEXT"),
    ("title", "TEXT"),
    ("author", "TEXT"),
    ("icd10_code", "TEXT"),
    ("dx_family", "TEXT"),
    ("loinc_code", "TEXT"),
    ("result_flag", "TEXT"),
    ("order_status", "TEXT"),
    ("content", "TEXT NOT NULL"),
    ("embedding", "vector(1024) NOT NULL"),
]
DOC_COLUMNS = [c for c, _ in POSTGRES_COLUMNS]

PATIENT_COLUMNS = [
    "patient_id",
    "full_name",
    "date_of_birth",
    "sex",
    "zip_code",
    "insurance_plan",
    "primary_facility",
    "last_visited",
    "next_scheduled_visit",
    "is_high_utilizer",
]


def create_table_sql(pg_table):
    columns = ",\n  ".join(f"{c} {t}" for c, t in POSTGRES_COLUMNS)
    return f"""
CREATE TABLE {pg_table} (
  {columns},
  content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, '') || ' ' || content)) STORED
)"""


def _null():
    return F.lit(None).cast("string")


def _cdf(df):
    return [c for c in CDF_COLUMNS if c in df.columns]


def _encounter_docs(encounters):
    return encounters.select(
        F.col("encounter_id").alias("doc_id"),
        F.lit("encounters").alias("source_table"),
        "patient_id",
        "encounter_id",
        F.col("encounter_date").alias("event_at"),
        F.col("encounter_facility").alias("facility"),
        F.col("encounter_type").alias("doc_type"),
        F.col("chief_complaint").alias("title"),
        _null().alias("author"),
        _null().alias("icd10_code"),
        _null().alias("dx_family"),
        _null().alias("loinc_code"),
        _null().alias("result_flag"),
        _null().alias("order_status"),
        F.col("embedding").alias("content"),
        "embedding_vector",
        *_cdf(encounters),
    )


def _lab_docs(order_details, orders):
    return order_details.join(
        orders.select("order_id", "ordering_facility", "ordering_provider", "order_status"), "order_id", "inner"
    ).select(
        F.col("order_detail_id").alias("doc_id"),
        F.lit("order_details").alias("source_table"),
        "patient_id",
        "encounter_id",
        F.col("collected_at").alias("event_at"),
        F.col("ordering_facility").alias("facility"),
        F.lit("Lab result").alias("doc_type"),
        F.col("test_name").alias("title"),
        F.col("ordering_provider").alias("author"),
        _null().alias("icd10_code"),
        _null().alias("dx_family"),
        "loinc_code",
        "result_flag",
        "order_status",
        F.col("embedding").alias("content"),
        "embedding_vector",
        *_cdf(order_details),
    )


def _diagnosis_docs(diagnoses, encounters):
    return diagnoses.join(
        encounters.select("encounter_id", "encounter_facility"), "encounter_id", "inner"
    ).select(
        F.col("diagnosis_id").alias("doc_id"),
        F.lit("diagnoses").alias("source_table"),
        "patient_id",
        "encounter_id",
        F.col("diagnosis_date").cast("timestamp").alias("event_at"),
        F.col("encounter_facility").alias("facility"),
        F.col("diagnosis_type").alias("doc_type"),
        F.col("diagnosis_description").alias("title"),
        _null().alias("author"),
        "icd10_code",
        "dx_family",
        _null().alias("loinc_code"),
        _null().alias("result_flag"),
        _null().alias("order_status"),
        F.col("embedding").alias("content"),
        "embedding_vector",
        *_cdf(diagnoses),
    )


def _note_docs(clinical_notes):
    return clinical_notes.select(
        F.col("note_id").alias("doc_id"),
        F.lit("clinical_notes").alias("source_table"),
        "patient_id",
        "encounter_id",
        F.col("note_datetime").alias("event_at"),
        F.col("note_facility").alias("facility"),
        F.col("note_type").alias("doc_type"),
        F.col("note_type").alias("title"),
        "author",
        _null().alias("icd10_code"),
        _null().alias("dx_family"),
        _null().alias("loinc_code"),
        _null().alias("result_flag"),
        _null().alias("order_status"),
        F.col("embedding").alias("content"),
        "embedding_vector",
        *_cdf(clinical_notes),
    )


def build_search_documents(spark, source, cutoff, changes=None) -> DataFrame:
    """Search documents for patients with last_visited >= cutoff, built on the fly (nothing is materialized).

    source: "catalog.schema" of the bronze tables.
    cutoff: date; patients whose last_visited is earlier are excluded.
    changes: optional {source_table: DataFrame} of rows to rebuild (e.g. CDF). Defaults to every source table in full;
        when given, only the listed sources are built.
    """
    rows = changes if changes is not None else {t: spark.table(f"{source}.{t}") for t in SOURCE_TABLES}
    unknown = set(rows) - set(SOURCE_TABLES)
    assert not unknown, f"unknown source tables: {unknown}"

    builders = {
        "encounters": lambda df: _encounter_docs(df),
        "order_details": lambda df: _lab_docs(df, spark.table(f"{source}.orders")),
        "diagnoses": lambda df: _diagnosis_docs(df, spark.table(f"{source}.encounters")),
        "clinical_notes": lambda df: _note_docs(df),
    }
    parts = [builders[t](df) for t, df in rows.items()]
    assert parts, "no source rows to build"
    docs = parts[0]
    for part in parts[1:]:
        docs = docs.unionByName(part, allowMissingColumns=True)

    patients = spark.table(f"{source}.patients").filter(F.col("last_visited") >= F.lit(cutoff)).select(*PATIENT_COLUMNS)
    cdf = [c for c in CDF_COLUMNS if c in docs.columns]
    return (
        docs.filter(F.col("embedding_vector").isNotNull())
        .join(patients, "patient_id", "inner")
        .select(*[c for c in DOC_COLUMNS if c != "embedding"], "embedding_vector", *cdf)
    )


def to_postgres_rows(docs) -> DataFrame:
    """Format documents for the Postgres search table (vector as pgvector text, no NUL bytes)."""
    cdf = [c for c in CDF_COLUMNS if c in docs.columns]
    return (
        docs.withColumn("content", F.regexp_replace("content", "\u0000", ""))
        # pgvector text input '[x1,x2,...]'; JDBC stringtype=unspecified lets Postgres cast it to vector.
        .withColumn(
            "embedding",
            F.concat(F.lit("["), F.array_join(F.transform("embedding_vector", lambda x: x.cast("string")), ","), F.lit("]")),
        )
        .select(*DOC_COLUMNS, *cdf)
    )
