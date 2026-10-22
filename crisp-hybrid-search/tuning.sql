-- Tuning for search.search_documents. Run as the table owner, outside a transaction (VACUUM cannot run in one):
--   databricks psql --project crisp-search --profile DEFAULT -- -f crisp-hybrid-search/tuning.sql
-- The same maintenance step is built into notebooks/02_load_lakebase_search.py for future reloads.

-- 1. Planner statistics on the columns the search filters and matches on.
ALTER TABLE search.search_documents ALTER COLUMN content_tsv SET STATISTICS 1000;
ALTER TABLE search.search_documents ALTER COLUMN full_name SET STATISTICS 1000;
ALTER TABLE search.search_documents ALTER COLUMN source_table SET STATISTICS 1000;
ALTER TABLE search.search_documents ALTER COLUMN facility SET STATISTICS 1000;

-- 2. Hint bits + visibility map after the bulk load, then fresh statistics.
VACUUM (ANALYZE, VERBOSE) search.search_documents;

-- 3. Partial HNSW index for the sparse source. A clinical_notes-only search otherwise skips the big HNSW index and
--    exact-sorts every note row, reading each TOASTed 4 KB vector (measured: ~18 s for 13.5k rows).
--    Small (~13.5k vectors), builds quickly. Run with enough maintenance_work_mem.
SET maintenance_work_mem = '1GB';
CREATE INDEX IF NOT EXISTS search_documents_embedding_hnsw_notes_idx
  ON search.search_documents USING hnsw (embedding vector_cosine_ops)
  WHERE source_table = 'clinical_notes';
ANALYZE search.search_documents;

-- 4. Optional: warm the search indexes so the first query after a scale-to-zero wake-up is not cold.
--    Needs the pg_prewarm extension; skip if CREATE EXTENSION is not permitted on Lakebase.
-- CREATE EXTENSION IF NOT EXISTS pg_prewarm;
-- SELECT pg_prewarm('search.search_documents_tsv_idx');
-- SELECT pg_prewarm('search.search_documents_name_trgm_idx');
-- SELECT pg_prewarm('search.search_documents_embedding_hnsw_idx');

-- ---------------------------------------------------------------------------------------------------------------
-- Diagnostics. Run these and compare before/after. Replace the literals with a real query.
-- ---------------------------------------------------------------------------------------------------------------

-- Index sizes (is the working set larger than the compute's memory?) and whether each index is used.
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size, idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'search'
ORDER BY pg_relation_size(indexrelid) DESC;

SELECT pg_size_pretty(pg_table_size('search.search_documents')) AS table_size,
       (SELECT count(*) FROM search.search_documents) AS docs;

-- Keyword leg for a long phrase (full-text only, no trigram/exact). Look for a Bitmap Index Scan on
-- search_documents_tsv_idx, "Heap Blocks: exact" (not lossy), and where the time goes (ts_rank_cd over all matches).
EXPLAIN (ANALYZE, BUFFERS, SETTINGS)
SELECT doc_id,
       ts_rank_cd(content_tsv, websearch_to_tsquery('english', 'delayed HbA1c courier disruption')) AS score
FROM search.search_documents
WHERE content_tsv @@ websearch_to_tsquery('english', 'delayed HbA1c courier disruption')
ORDER BY score DESC NULLS LAST
LIMIT 100;

-- A very common term: the number of matches (rows ranked) drives the cost here.
EXPLAIN (ANALYZE, BUFFERS)
SELECT doc_id,
       ts_rank_cd(content_tsv, websearch_to_tsquery('english', 'diabetes')) AS score
FROM search.search_documents
WHERE content_tsv @@ websearch_to_tsquery('english', 'diabetes')
ORDER BY score DESC NULLS LAST
LIMIT 100;

-- Short name query: should use search_documents_name_trgm_idx.
EXPLAIN (ANALYZE, BUFFERS)
SELECT doc_id, similarity(full_name, 'maria garcia') AS score
FROM search.search_documents
WHERE full_name % 'maria garcia'
ORDER BY score DESC NULLS LAST
LIMIT 100;
