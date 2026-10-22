import { formatDate, sourceLabel, type SearchResponse } from '../api';

interface Props {
  response: SearchResponse | null;
  searching: boolean;
  error: string | null;
  selectedId: string | null;
  onSelect: (docId: string) => void;
}

export function ResultsPane({ response, searching, error, selectedId, onSelect }: Props) {
  return (
    <aside className="pane results" aria-label="Search results">
      <div className="pane-title">
        Results{response ? <span className="count">{response.results.length}</span> : null}
      </div>
      <div className="pane-body">
        {error && <div className="notice error">{error}</div>}
        {!response && !error && !searching && <div className="notice">Run a search to see matching records.</div>}
        {searching && <div className="notice">Searching…</div>}
        {response && response.results.length === 0 && <div className="notice">No matching records.</div>}
        <ul className="result-list">
          {response?.results.map((hit) => (
            <li key={hit.doc_id}>
              <button
                type="button"
                className={hit.doc_id === selectedId ? 'result active' : 'result'}
                onClick={() => onSelect(hit.doc_id)}
              >
                <div className="result-top">
                  <span className="tag">{sourceLabel(hit.source_table)}</span>
                  {hit.result_flag && (
                    <span className={`tag flag flag-${hit.result_flag.toLowerCase()}`}>{hit.result_flag}</span>
                  )}
                  <span className="result-date">{formatDate(hit.event_at)}</span>
                </div>
                <div className="result-title">{hit.title ?? hit.doc_type ?? hit.doc_id}</div>
                <div className="result-patient">
                  {hit.full_name ?? hit.patient_id}
                  {hit.facility ? ` · ${hit.facility}` : ''}
                </div>
                <div className="result-snippet">{hit.snippet}</div>
                <div className="result-ranks">
                  <span title="Keyword rank">K {hit.keyword_rank ?? '–'}</span>
                  <span title="Semantic rank">S {hit.semantic_rank ?? '–'}</span>
                  <span title="Reciprocal rank fusion score">RRF {hit.rrf.toFixed(4)}</span>
                </div>
              </button>
            </li>
          ))}
        </ul>
      </div>
    </aside>
  );
}
