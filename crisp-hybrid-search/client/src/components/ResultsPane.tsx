import type { KeyboardEvent } from 'react';
import { Layers, User, AlertCircle, Loader2 } from 'lucide-react';
import { formatDate, sourceLabel, type SearchResponse } from '../api';

interface Props {
  response: SearchResponse | null;
  searching: boolean;
  error: string | null;
  selectedId: string | null;
  onSelect: (docId: string) => void;
}

export function ResultsPane({ response, searching, error, selectedId, onSelect }: Props) {
  const results = response?.results ?? [];

  // Arrow keys move the selection, wrapping at either end.
  const handleKeyDown = (e: KeyboardEvent<HTMLUListElement>) => {
    const step = e.key === 'ArrowDown' ? 1 : e.key === 'ArrowUp' ? -1 : 0;
    if (!step || !results.length) return;
    e.preventDefault();
    const n = results.length;
    const i = results.findIndex((r) => r.doc_id === selectedId);
    const next = i === -1 ? (step > 0 ? 0 : n - 1) : (i + step + n) % n;
    onSelect(results[next].doc_id);
  };

  return (
    <aside className="pane results" aria-label="Search results">
      <div className="pane-title">
        <div className="pane-title-left">
          <Layers size={14} />
          <span>Results</span>
        </div>
        {response && <span className="count-badge">{results.length}</span>}
      </div>

      <div className="pane-body">
        {error && (
          <div className="notice error">
            <AlertCircle size={16} />
            <span>{error}</span>
          </div>
        )}
        {!response && !error && !searching && (
          <div className="notice">
            <p>Run a search to see matching clinical records.</p>
          </div>
        )}
        {searching && (
          <div className="notice">
            <Loader2 size={18} className="animate-spin" />
            <span>Searching records…</span>
          </div>
        )}
        {response && results.length === 0 && (
          <div className="notice">
            <p>No matching clinical records found.</p>
          </div>
        )}

        <ul
          className="result-list"
          role="listbox"
          aria-label="Clinical search results"
          tabIndex={0}
          onKeyDown={handleKeyDown}
        >
          {results.map((hit) => {
            const isSelected = hit.doc_id === selectedId;
            return (
              <li key={hit.doc_id} role="option" aria-selected={isSelected}>
                <button
                  type="button"
                  className={isSelected ? 'result active' : 'result'}
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
                    <User size={12} strokeWidth={2} />
                    <span>{hit.full_name ?? hit.patient_id}</span>
                    {hit.facility && <span>· {hit.facility}</span>}
                  </div>

                  <div className="result-snippet">{hit.snippet}</div>

                  <div className="result-ranks">
                    <span className="rank-badge" title="Keyword Rank">
                      K {hit.keyword_rank ?? '–'}
                    </span>
                    <span className="rank-badge" title="Semantic Rank">
                      S {hit.semantic_rank ?? '–'}
                    </span>
                    <span className="rank-badge rrf" title="Reciprocal Rank Fusion Score">
                      RRF {hit.rrf.toFixed(4)}
                    </span>
                  </div>
                </button>
              </li>
            );
          })}
        </ul>
      </div>
    </aside>
  );
}
