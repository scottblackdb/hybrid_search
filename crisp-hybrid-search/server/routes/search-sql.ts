export const TABLE = 'search.search_documents';

export type SearchMode = 'hybrid' | 'keyword' | 'semantic';

export interface SearchFilters {
  sources: string[];
}

export interface BuiltQuery {
  text: string;
  params: unknown[];
  /** Same length as params; what to show in the SQL pane (long vectors abbreviated). */
  displayParams: string[];
  steps: { name: string; description: string }[];
}

const RRF_K = 60;
/** Every search returns at most this many results. */
export const MAX_RESULTS = 200;
// Each leg must gather at least MAX_RESULTS candidates or a hybrid search could not fill the page.
const CANDIDATES = MAX_RESULTS;
// Max keyword matches that get ranked (override with KEYWORD_MATCH_CAP). A common term ("diabetes") matches ~11% of the
// table, so collecting N matches reads ~N/1.5 heap blocks (~0.18 ms each from Lakebase storage): 20000 took ~2 s.
// Terms with fewer matches than the cap are ranked exactly; for more common terms the top results come from the first
// N matches found. Lower is faster and more approximate.
const DEFAULT_MATCH_CAP = 5000;
const parsedCap = Number(process.env.KEYWORD_MATCH_CAP);
const capIsValid = Number.isInteger(parsedCap) && parsedCap >= CANDIDATES;
if (process.env.KEYWORD_MATCH_CAP !== undefined && !capIsValid) {
  console.warn(
    `[search] Ignoring KEYWORD_MATCH_CAP=${process.env.KEYWORD_MATCH_CAP}: it must be an integer >= ${CANDIDATES}; using ${DEFAULT_MATCH_CAP}`
  );
}
const MATCH_CAP = capIsValid ? parsedCap : DEFAULT_MATCH_CAP;

/**
 * Builds the hybrid query. Parameters are numbered in order: query text (not in semantic mode), query embedding
 * (pgvector text, not in keyword mode), result limit, then optional filters. Filters are applied inside each candidate leg, before ranking.
 */
export function buildSearchQuery(
  q: string,
  vector: number[],
  mode: SearchMode,
  limit: number,
  filters: SearchFilters
): BuiltQuery {
  // Only parameters the chosen mode references are sent; Postgres rejects unused ones ("could not determine data type").
  const params: unknown[] = [];
  const displayParams: string[] = [];
  const add = (value: unknown, display: string) => {
    params.push(value);
    displayParams.push(display);
    return `$${params.length}`;
  };
  const useKeyword = mode !== 'semantic';
  const useSemantic = mode !== 'keyword';
  const txt = useKeyword ? add(q, `'${q.replace(/'/g, "''")}'`) : '';
  const vec = !useSemantic
    ? ''
    : add(
        `[${vector.join(',')}]`,
        `'[${vector
          .slice(0, 3)
          .map((v) => v.toFixed(4))
          .join(', ')}, … ${vector.length} dims]'`
      );
  const lim = add(limit, String(limit));

  const sourceFilter = filters.sources.length
    ? `source_table = ANY(${add(filters.sources, `ARRAY[${filters.sources.map((s) => `'${s}'`).join(', ')}]`)})`
    : '';

  // The exact-code/ID and trigram-name conditions only help short queries. Adding them to a long clinical phrase
  // OR-s a wide trigram index scan into every search and keeps the planner from using the full-text index alone.
  const words = q.trim().split(/\s+/).filter(Boolean);
  const exactLeg = words.length === 1; // ICD-10, LOINC, patient or document ID are single tokens
  const nameLeg = words.length <= 3 && q.trim().length <= 40; // patient names
  const exact = `(icd10_code = upper(${txt}) OR loinc_code = ${txt} OR patient_id = ${txt} OR doc_id = ${txt})`;
  const fts = `websearch_to_tsquery('english', ${txt})`;
  const matches = [`content_tsv @@ ${fts}`];
  const scores = [`ts_rank_cd(content_tsv, ${fts})`];
  if (exactLeg) {
    matches.push(exact);
    scores.push(`coalesce(${exact}, false)::int`);
  }
  if (nameLeg) {
    matches.push(`full_name % ${txt}`);
    scores.push(`coalesce(similarity(full_name, ${txt}), 0)`);
  }
  const keywordWhat = ['full-text (GIN on content_tsv)'];
  if (exactLeg) keywordWhat.push('exact codes/IDs');
  if (nameLeg) keywordWhat.push('fuzzy patient name (GIN trigram)');
  const keywordCte = `keyword AS (
  -- Keyword leg: ${keywordWhat.join(', ')}
  -- Only the first ${MATCH_CAP} matches are ranked, so very common terms do not scan and rank the whole table
  SELECT doc_id,
         row_number() OVER (ORDER BY score DESC NULLS LAST) AS rank,
         score
  FROM (
    SELECT doc_id,
           ${scores.join('\n             + ')} AS score
    FROM (
      SELECT doc_id, content_tsv, icd10_code, loinc_code, patient_id, full_name
      FROM ${TABLE}
      WHERE (${matches.join('\n             OR ')})
            ${sourceFilter && `AND ${sourceFilter}`}
      LIMIT ${MATCH_CAP}
    ) m
    ORDER BY score DESC NULLS LAST
    LIMIT ${CANDIDATES}
  ) k
)`;
  const semanticCte = `semantic AS (
  -- Semantic leg: pgvector cosine distance (HNSW on embedding)
  SELECT doc_id,
         row_number() OVER (ORDER BY embedding <=> ${vec}::vector) AS rank,
         1 - (embedding <=> ${vec}::vector) AS score
  FROM ${TABLE}
  ${sourceFilter && `WHERE ${sourceFilter}`}
  ORDER BY embedding <=> ${vec}::vector
  LIMIT ${CANDIDATES}
)`;

  const select = `SELECT d.doc_id, d.source_table, d.patient_id, d.full_name, d.event_at, d.facility,
       d.doc_type, d.title, d.author, d.icd10_code, d.loinc_code, d.result_flag,
       left(d.content, 280) AS snippet,
       f.rrf::float8 AS rrf, f.keyword_rank::int AS keyword_rank, f.semantic_rank::int AS semantic_rank,
       f.keyword_score::float8 AS keyword_score, f.semantic_score::float8 AS semantic_score
FROM fused f
JOIN ${TABLE} d USING (doc_id)
ORDER BY f.rrf DESC
LIMIT ${lim}`;

  // With one leg, the fused CTE passes it through and leaves the other leg's columns NULL.
  const fusedCte =
    mode === 'hybrid'
      ? `fused AS (
  -- Reciprocal rank fusion
  SELECT coalesce(k.doc_id, s.doc_id) AS doc_id,
         coalesce(1.0 / (${RRF_K} + k.rank), 0) + coalesce(1.0 / (${RRF_K} + s.rank), 0) AS rrf,
         k.rank AS keyword_rank, s.rank AS semantic_rank,
         k.score AS keyword_score, s.score AS semantic_score
  FROM keyword k
  FULL JOIN semantic s USING (doc_id)
)`
      : `fused AS (
  SELECT doc_id, 1.0 / (${RRF_K} + rank) AS rrf, ${useKeyword ? 'rank' : 'NULL::bigint'} AS keyword_rank, ${useSemantic ? 'rank' : 'NULL::bigint'} AS semantic_rank,
         ${useKeyword ? 'score' : 'NULL::float8'} AS keyword_score, ${useSemantic ? 'score' : 'NULL::float8'} AS semantic_score
  FROM ${mode}
)`;
  const ctes = [useKeyword && keywordCte, useSemantic && semanticCte, fusedCte].filter(Boolean);
  const text = `WITH ${ctes.join(',\n')}\n${select}`;

  const steps: BuiltQuery['steps'] = [];
  if (useSemantic) {
    steps.push({
      name: 'Embed query',
      description: `The query text is sent to databricks-gte-large-en to get a 1024-dim vector (${vec}).`,
    });
  }
  if (sourceFilter) {
    steps.push({
      name: 'Filter',
      description: `Restrict to source_table in (${filters.sources.join(', ')}) inside each leg, before ranking.`,
    });
  }
  if (useKeyword) {
    steps.push({
      name: 'Keyword search',
      description: `Top ${CANDIDATES} by ${keywordWhat.join(' + ')}, ranking at most ${MATCH_CAP} matches.`,
    });
  }
  if (useSemantic) {
    steps.push({
      name: 'Semantic search',
      description: `Top ${CANDIDATES} nearest neighbours by cosine distance (HNSW).`,
    });
  }
  if (mode === 'hybrid') {
    steps.push({
      name: 'Fuse',
      description: `Reciprocal rank fusion: rrf = 1/(${RRF_K}+keyword_rank) + 1/(${RRF_K}+semantic_rank). A document found by both legs rises to the top.`,
    });
  }
  steps.push({ name: 'Join', description: 'Fetch display columns for the fused doc_ids and return the top results.' });

  return { text, params, displayParams, steps };
}

/** Session settings applied (SET LOCAL) before the search so filtered HNSW scans still return enough rows. */
export const SESSION_SETTINGS = [
  "SET LOCAL work_mem = '64MB'", // keeps the keyword bitmap exact instead of lossy
  `SET LOCAL hnsw.ef_search = ${Math.min(CANDIDATES, 1000)}`, // tied to candidates per leg; pgvector maximum is 1000
  'SET LOCAL hnsw.iterative_scan = relaxed_order',
  // Default is 20000; a source filter that matches few nearest neighbours otherwise returns 0 rows.
  'SET LOCAL hnsw.max_scan_tuples = 100000',
];

export const DETAIL_SQL = `SELECT doc_id, source_table, patient_id, encounter_id, full_name, date_of_birth, sex, zip_code,
       insurance_plan, primary_facility, last_visited, next_scheduled_visit, is_high_utilizer,
       event_at, facility, doc_type, title, author, icd10_code, dx_family, loinc_code, result_flag,
       order_status, content
FROM ${TABLE}
WHERE doc_id = $1`;

export const RELATED_SQL = `SELECT doc_id, source_table, event_at, doc_type, title, icd10_code, result_flag
FROM ${TABLE}
WHERE patient_id = $1 AND doc_id <> $2
ORDER BY event_at DESC
LIMIT 15`;
