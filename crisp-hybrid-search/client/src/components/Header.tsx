import { Search, Sparkles, Layers } from 'lucide-react';
import { SOURCES, type SearchMode } from '../api';

const MODES: { value: SearchMode; label: string }[] = [
  { value: 'hybrid', label: 'Hybrid' },
  { value: 'keyword', label: 'Keyword' },
  { value: 'semantic', label: 'Semantic' },
];

interface Props {
  query: string;
  onQuery: (q: string) => void;
  mode: SearchMode;
  onMode: (m: SearchMode) => void;
  sources: string[];
  onSources: (s: string[]) => void;
  onSubmit: () => void;
  searching: boolean;
}

export function Header({ query, onQuery, mode, onMode, sources, onSources, onSubmit, searching }: Props) {
  const toggle = (value: string) =>
    onSources(sources.includes(value) ? sources.filter((s) => s !== value) : [...sources, value]);

  return (
    <header className="header">
      <div className="header-top">
        <div className="brand">
          <div className="brand-icon">
            <Sparkles size={16} strokeWidth={2.2} />
          </div>
          <div className="brand-text">
            <span className="brand-mark">CRISP</span>
            <span className="brand-sub">Clinical Search</span>
          </div>
        </div>

        <form
          className="search-form"
          onSubmit={(e) => {
            e.preventDefault();
            onSubmit();
          }}
        >
          <div className="search-icon-wrapper">
            <Search size={16} strokeWidth={2} />
          </div>
          <input
            className="search-input"
            type="search"
            value={query}
            onChange={(e) => onQuery(e.target.value)}
            placeholder="Search patients, diagnoses, labs and clinical notes (e.g., “delayed HbA1c” or E11.22)..."
            aria-label="Search"
            autoFocus
          />
          <button className="search-submit-btn" type="submit" disabled={searching || !query.trim()}>
            {searching ? 'Searching…' : 'Search'}
          </button>
        </form>
      </div>

      <div className="header-bottom">
        <div className="segmented" role="radiogroup" aria-label="Search mode">
          {MODES.map((m) => (
            <button
              key={m.value}
              type="button"
              role="radio"
              aria-checked={mode === m.value}
              className={mode === m.value ? 'seg active' : 'seg'}
              onClick={() => onMode(m.value)}
            >
              {m.label}
            </button>
          ))}
        </div>

        <div className="chips-group" aria-label="Sources">
          <span className="chips-label">
            <Layers size={12} style={{ display: 'inline', marginRight: 4, verticalAlign: -1 }} />
            Sources
          </span>
          <div className="chips">
            {SOURCES.map((s) => {
              const active = sources.includes(s.value);
              return (
                <button
                  key={s.value}
                  type="button"
                  aria-pressed={active}
                  className={active ? 'chip active' : 'chip'}
                  onClick={() => toggle(s.value)}
                >
                  {s.label}
                </button>
              );
            })}
          </div>
        </div>
      </div>
    </header>
  );
}
