import { WorkspaceClient } from '@databricks/sdk-experimental';
import type { Application } from 'express';
import type { PoolClient } from 'pg';
import { z } from 'zod';
import { buildSearchQuery, DETAIL_SQL, MAX_RESULTS, RELATED_SQL, SESSION_SETTINGS } from './search-sql';

interface AppKitWithLakebase {
  lakebase: {
    pool: { connect(): Promise<PoolClient> };
    query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
  };
  server: {
    extend(fn: (app: Application) => void): void;
  };
}

const SOURCES = ['encounters', 'order_details', 'diagnoses', 'clinical_notes'] as const;

const SearchBody = z.object({
  query: z.string().trim().min(1).max(500),
  mode: z.enum(['hybrid', 'keyword', 'semantic']).default('hybrid'),
  limit: z.number().int().min(1).max(MAX_RESULTS).default(MAX_RESULTS),
  sources: z.array(z.enum(SOURCES)).default([]),
});

const EMBEDDING_ENDPOINT = process.env.DATABRICKS_SERVING_ENDPOINT_NAME ?? 'databricks-gte-large-en';

let workspaceClient: WorkspaceClient | undefined;

async function embed(text: string): Promise<number[]> {
  workspaceClient ??= new WorkspaceClient({});
  const res = await workspaceClient.servingEndpoints.query({ name: EMBEDDING_ENDPOINT, input: [text] });
  const vector = res.data?.[0]?.embedding;
  if (!vector || vector.length === 0) {
    throw new Error(`No embedding returned from ${EMBEDDING_ENDPOINT}`);
  }
  return vector;
}

export function setupSearchRoutes(appkit: AppKitWithLakebase) {
  appkit.server.extend((app) => {
    app.post('/api/search', async (req, res) => {
      const parsed = SearchBody.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: 'A non-empty query is required' });
        return;
      }
      const { query, mode, limit, sources } = parsed.data;

      try {
        const t0 = performance.now();
        // Keyword mode needs no embedding, so skip the endpoint call.
        const vector = mode === 'keyword' ? [] : await embed(query);
        const embedMs = performance.now() - t0;

        const built = buildSearchQuery(query, vector, mode, limit, { sources });
        const client = await appkit.lakebase.pool.connect();
        const t1 = performance.now();
        let rows: Record<string, unknown>[];
        try {
          await client.query('BEGIN');
          for (const s of SESSION_SETTINGS) await client.query(s);
          const result = await client.query(built.text, built.params);
          await client.query('COMMIT');
          rows = result.rows as Record<string, unknown>[];
        } catch (err) {
          await client.query('ROLLBACK').catch(() => undefined);
          throw err;
        } finally {
          client.release();
        }
        const queryMs = performance.now() - t1;

        res.json({
          results: rows,
          sql: built.text,
          params: built.displayParams,
          sessionSettings: SESSION_SETTINGS,
          steps: built.steps,
          timings: { embedMs: Math.round(embedMs), queryMs: Math.round(queryMs) },
          mode,
        });
      } catch (err) {
        console.error('Search failed:', err);
        res.status(500).json({ error: (err as Error).message });
      }
    });

    app.get('/api/documents/:docId', async (req, res) => {
      try {
        const doc = await appkit.lakebase.query(DETAIL_SQL, [req.params.docId]);
        if (doc.rows.length === 0) {
          res.status(404).json({ error: 'Document not found' });
          return;
        }
        const related = await appkit.lakebase.query(RELATED_SQL, [doc.rows[0].patient_id, req.params.docId]);
        res.json({
          document: doc.rows[0],
          related: related.rows,
          sql: [DETAIL_SQL, RELATED_SQL],
        });
      } catch (err) {
        console.error('Document fetch failed:', err);
        res.status(500).json({ error: (err as Error).message });
      }
    });
  });
}
