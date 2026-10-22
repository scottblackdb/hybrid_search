-- Grants the crisp-hybrid-search app (service principal app-48srh1) read access to the search table.
--
-- Run as the role that owns search.search_documents (the user who runs 02_load_lakebase_search), after the app
-- has been deployed so its Postgres role exists. Connect with, for example:
--   databricks psql --project crisp-search --profile DEFAULT -- -f crisp-hybrid-search/grants.sql
--
-- The Postgres role is named after the service principal's client ID:
--   databricks apps get crisp-hybrid-search --profile DEFAULT   -> service_principal_client_id

GRANT USAGE ON SCHEMA search TO "2498ef28-6dc6-4c0d-a910-7a0d6b84f1ca";
GRANT SELECT ON search.search_documents TO "2498ef28-6dc6-4c0d-a910-7a0d6b84f1ca";

-- 02_load_lakebase_search drops and recreates search.search_documents on every run, which discards the grant above.
-- This makes tables created later in the schema by the role running this script readable by the app automatically.
ALTER DEFAULT PRIVILEGES IN SCHEMA search
  GRANT SELECT ON TABLES TO "2498ef28-6dc6-4c0d-a910-7a0d6b84f1ca";
