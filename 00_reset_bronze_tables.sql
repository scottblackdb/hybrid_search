-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reset CRISP bronze tables
-- MAGIC
-- MAGIC Drops every object created by `01_generate_bronze_clinical_data` so the generator can reload from a clean state
-- MAGIC (fresh schema, table properties, clustering, and Change Data Feed history).
-- MAGIC
-- MAGIC Does **not** drop the `crisp` catalog or `crisp.bronze` schema. Dropped managed tables can be restored for
-- MAGIC 7 days with `UNDROP TABLE crisp.bronze.<table>`.

-- COMMAND ----------

-- View first: it depends on the tables below.
DROP VIEW IF EXISTS crisp.bronze.v_frederick_delayed_labs;

-- COMMAND ----------

DROP TABLE IF EXISTS crisp.bronze.embedding_vectors;
DROP TABLE IF EXISTS crisp.bronze.clinical_notes;
DROP TABLE IF EXISTS crisp.bronze.order_details;
DROP TABLE IF EXISTS crisp.bronze.orders;
DROP TABLE IF EXISTS crisp.bronze.diagnoses;
DROP TABLE IF EXISTS crisp.bronze.encounters;
DROP TABLE IF EXISTS crisp.bronze.patients;

-- COMMAND ----------

-- Expect no generator tables left.
SHOW TABLES IN crisp.bronze;
