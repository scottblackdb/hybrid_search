export type SearchMode = 'hybrid' | 'keyword' | 'semantic';

export const SOURCES = [
  { value: 'diagnoses', label: 'Diagnoses' },
  { value: 'order_details', label: 'Lab results' },
  { value: 'encounters', label: 'Encounters' },
  { value: 'clinical_notes', label: 'Clinical notes' },
] as const;

export interface SearchHit {
  doc_id: string;
  source_table: string;
  patient_id: string;
  full_name: string | null;
  event_at: string | null;
  facility: string | null;
  doc_type: string | null;
  title: string | null;
  author: string | null;
  icd10_code: string | null;
  loinc_code: string | null;
  result_flag: string | null;
  snippet: string;
  rrf: number;
  keyword_rank: number | null;
  semantic_rank: number | null;
  keyword_score: number | null;
  semantic_score: number | null;
}

export interface SearchResponse {
  results: SearchHit[];
  sql: string;
  params: string[];
  sessionSettings: string[];
  steps: { name: string; description: string }[];
  timings: { embedMs: number; queryMs: number };
  mode: SearchMode;
}

export interface DocumentDetail {
  document: {
    doc_id: string;
    source_table: string;
    patient_id: string;
    encounter_id: string | null;
    full_name: string | null;
    date_of_birth: string | null;
    sex: string | null;
    zip_code: string | null;
    insurance_plan: string | null;
    primary_facility: string | null;
    last_visited: string | null;
    next_scheduled_visit: string | null;
    is_high_utilizer: boolean | null;
    event_at: string | null;
    facility: string | null;
    doc_type: string | null;
    title: string | null;
    author: string | null;
    icd10_code: string | null;
    dx_family: string | null;
    loinc_code: string | null;
    result_flag: string | null;
    order_status: string | null;
    content: string;
  };
  related: {
    doc_id: string;
    source_table: string;
    event_at: string | null;
    doc_type: string | null;
    title: string | null;
    icd10_code: string | null;
    result_flag: string | null;
  }[];
  sql: string[];
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  const body: unknown = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((body as { error?: string }).error ?? `Request failed (${res.status})`);
  return body as T;
}

/** The server returns its maximum number of results (200) when no limit is sent. */
export function search(query: string, mode: SearchMode, sources: string[]) {
  return request<SearchResponse>('/api/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, mode, sources }),
  });
}

export function getDocument(docId: string) {
  return request<DocumentDetail>(`/api/documents/${encodeURIComponent(docId)}`);
}

export function sourceLabel(source: string) {
  return SOURCES.find((s) => s.value === source)?.label ?? source;
}

export function formatDate(value: string | null | undefined, withTime = false) {
  if (!value) return '—';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return withTime ? d.toLocaleString() : d.toLocaleDateString();
}

/** Milliseconds below 1000, otherwise seconds with two decimals (e.g. 1234 -> "1.23 s"). */
export function formatLatency(ms: number) {
  return ms > 1000 ? `${(ms / 1000).toFixed(2)} s` : `${ms} ms`;
}
