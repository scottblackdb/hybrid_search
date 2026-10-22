# Databricks notebook source
# MAGIC %md
# MAGIC # Load CRISP hybrid search table into Lakebase
# MAGIC
# MAGIC Combines every row that has an `embedding_vector` from `encounters`, `order_details`, `diagnoses`, and
# MAGIC `clinical_notes` into one app-owned Postgres table, `search.search_documents`, on the Lakebase Autoscaling
# MAGIC project `crisp-search` (display name `crisp_search`, Postgres 18).
# MAGIC
# MAGIC Only patients whose `last_visited` is within `last_visited_days` (default 365) of `AS_OF` are loaded.
# MAGIC
# MAGIC Each row carries patient filter columns, source-specific keyword columns (ICD-10, LOINC, facility, flags),
# MAGIC the `embedding` source text as `content`, a generated `content_tsv` for full-text search, and
# MAGIC `embedding vector(1024)` for pgvector. Indexes (FTS, trigram, HNSW) are built after the load.
# MAGIC
# MAGIC Full reload: the Postgres table is dropped and recreated on every run.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1" "databricks-sdk>=0.81.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timedelta

import psycopg
from databricks.sdk import WorkspaceClient
from search_documents import build_search_documents, create_table_sql, to_postgres_rows

dbutils.widgets.text("catalog", "crisp")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("last_visited_days", "365")
dbutils.widgets.text("lakebase_project", "crisp-search")
dbutils.widgets.text("lakebase_branch", "production")
dbutils.widgets.text("lakebase_endpoint", "primary")
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("pg_schema", "search")
dbutils.widgets.text("pg_table", "search_documents")
dbutils.widgets.text("jdbc_partitions", "32")
dbutils.widgets.dropdown("create_vector_index", "true", ["true", "false"])

SOURCE = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
LAST_VISITED_DAYS = int(dbutils.widgets.get("last_visited_days"))
ENDPOINT_NAME = (
    f"projects/{dbutils.widgets.get('lakebase_project')}"
    f"/branches/{dbutils.widgets.get('lakebase_branch')}"
    f"/endpoints/{dbutils.widgets.get('lakebase_endpoint')}"
)
PG_DATABASE = dbutils.widgets.get("lakebase_database")
PG_SCHEMA = dbutils.widgets.get("pg_schema")
PG_TABLE = f"{PG_SCHEMA}.{dbutils.widgets.get('pg_table')}"
JDBC_PARTITIONS = int(dbutils.widgets.get("jdbc_partitions"))
CREATE_VECTOR_INDEX = dbutils.widgets.get("create_vector_index") == "true"

# Same AS_OF as 01_generate_bronze_clinical_data so "past year" lines up with the generated dates.
AS_OF = datetime(2026, 10, 21)
CUTOFF = (AS_OF - timedelta(days=LAST_VISITED_DAYS)).date()

w = WorkspaceClient()
PG_HOST = w.postgres.get_endpoint(name=ENDPOINT_NAME).status.hosts.host
PG_USER = w.current_user.me().user_name

print(f"{SOURCE} -> {ENDPOINT_NAME} ({PG_HOST}) {PG_DATABASE}.{PG_TABLE}")
print(f"patients with last_visited >= {CUTOFF}")


def pg_connect():
    # OAuth tokens are valid for 1 hour; mint a fresh one per connection.
    token = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME).token
    return psycopg.connect(
        host=PG_HOST, dbname=PG_DATABASE, user=PG_USER, password=token, sslmode="require", autocommit=True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the combined documents in Spark
# MAGIC
# MAGIC Logic lives in `search_documents.py` (next to this notebook) so full loads and future CDC loads share it.

# COMMAND ----------

# Built on the fly from the bronze tables; the same function takes Change Data Feed rows for CDC loads.
documents = to_postgres_rows(build_search_documents(spark, SOURCE, CUTOFF))

expected = {r["source_table"]: r["count"] for r in documents.groupBy("source_table").count().collect()}
print(f"documents to load: {expected}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Postgres table
# MAGIC
# MAGIC No primary key or indexes yet; they are added after the bulk load, which is much faster.

# COMMAND ----------

with pg_connect() as conn:
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}")
    conn.execute(f"DROP TABLE IF EXISTS {PG_TABLE}")
    conn.execute(create_table_sql(PG_TABLE))
print(f"created {PG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bulk load over JDBC
# MAGIC
# MAGIC All partitions connect at the start with one OAuth token (valid 1 hour). Keep `jdbc_partitions` high enough
# MAGIC that every task starts in the first wave; scale the Lakebase compute up for the load if it is slow.

# COMMAND ----------

(
    documents.repartition(JDBC_PARTITIONS)
    .write.format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://{PG_HOST}:5432/{PG_DATABASE}?sslmode=require&stringtype=unspecified&reWriteBatchedInserts=true",
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME).token)
    .option("batchsize", 1000)
    .mode("append")
    .save()
)
print("load complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keys and search indexes
# MAGIC
# MAGIC - B-tree: primary key, patient, source, event time, facility, codes
# MAGIC - GIN: `content_tsv` (keyword / FTS) and trigram on `full_name` (fuzzy name search)
# MAGIC - HNSW (`vector_cosine_ops`) on `embedding`. This is the slow one on millions of 1024-dim rows; it needs a large
# MAGIC   `maintenance_work_mem`, so scale the compute up first. Set `create_vector_index = false` to skip.

# COMMAND ----------

name = dbutils.widgets.get("pg_table")
with pg_connect() as conn:
    conn.execute(f"ALTER TABLE {PG_TABLE} ADD PRIMARY KEY (doc_id)")
    for cols, suffix in [
        ("patient_id", "patient"),
        ("source_table, event_at", "source_event"),
        ("facility, event_at", "facility_event"),
        ("icd10_code", "icd10"),
        ("loinc_code", "loinc"),
        ("next_scheduled_visit", "next_visit"),
    ]:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {name}_{suffix}_idx ON {PG_TABLE} ({cols})")
    conn.execute(f"CREATE INDEX IF NOT EXISTS {name}_tsv_idx ON {PG_TABLE} USING gin (content_tsv)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS {name}_name_trgm_idx ON {PG_TABLE} USING gin (full_name gin_trgm_ops)")
    print("b-tree, FTS, and trigram indexes created")

    if CREATE_VECTOR_INDEX:
        conn.execute("SET maintenance_work_mem = '2GB'")
        conn.execute("SET max_parallel_maintenance_workers = 7")
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {name}_embedding_hnsw_idx ON {PG_TABLE} "
            "USING hnsw (embedding vector_cosine_ops)"
        )
        print("HNSW vector index created")
    conn.execute(f"ANALYZE {PG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------

with pg_connect() as conn:
    loaded = conn.execute(
        f"SELECT source_table, COUNT(*) FROM {PG_TABLE} GROUP BY source_table ORDER BY source_table"
    ).fetchall()
    print("Postgres row counts:")
    for source_table, n in loaded:
        print(f"  {source_table}: {n:,}")
    print(f"Spark expected: {expected}")

    print("keyword: 'courier disruption' hits by source")
    for row in conn.execute(
        f"""
        SELECT source_table, COUNT(*)
        FROM {PG_TABLE}
        WHERE content_tsv @@ websearch_to_tsquery('english', 'courier disruption')
        GROUP BY source_table ORDER BY source_table
        """
    ).fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    print("semantic: nearest neighbours of a Frederick outage document")
    for row in conn.execute(
        f"""
        WITH q AS (
          SELECT embedding FROM {PG_TABLE}
          WHERE facility = 'Frederick Health Hospital' AND content ILIKE '%courier disruption%'
          LIMIT 1
        )
        SELECT d.source_table, d.patient_id, d.title, round((1 - (d.embedding <=> q.embedding))::numeric, 3) AS similarity
        FROM {PG_TABLE} d, q
        ORDER BY d.embedding <=> q.embedding
        LIMIT 5
        """
    ).fetchall():
        print(f"  {row}")