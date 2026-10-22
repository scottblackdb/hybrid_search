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
      <div className="brand">
        <span className="brand-mark">CRISP</span>
        <span className="brand-sub">Clinical Search</span>
      </div>
      <form
        className="search-form"
        onSubmit={(e) => {
          e.preventDefault();
          onSubmit();
        }}
      >
        <input
          className="search-input"
          type="search"
          value={query}
          onChange={(e) => onQuery(e.target.value)}
          placeholder="Search patients, diagnoses, labs and notes — try “delayed HbA1c courier disruption” or E11.22"
          aria-label="Search"
          autoFocus
        />
        <button className="btn btn-primary" type="submit" disabled={searching || !query.trim()}>
          {searching ? 'Searching…' : 'Search'}
        </button>
      </form>
      <div className="header-options">
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
        <div className="chips" aria-label="Sources">
          {SOURCES.map((s) => (
            <button
              key={s.value}
              type="button"
              aria-pressed={sources.includes(s.value)}
              className={sources.includes(s.value) ? 'chip active' : 'chip'}
              onClick={() => toggle(s.value)}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>
    </header>
  );
}
