import { Fragment, useState } from 'react';
import { Terminal, Copy, Check } from 'lucide-react';
import { formatLatency, type DocumentDetail, type SearchResponse } from '../api';

const KEYWORDS =
  /\b(WITH|AS|SELECT|FROM|WHERE|AND|OR|ORDER BY|OVER|LIMIT|JOIN|FULL JOIN|USING|ON|ANY|ARRAY|NULL|DESC|IS|SET LOCAL|BEGIN|COMMIT)\b/g;

function highlight(sql: string) {
  /* eslint-disable react/no-array-index-key -- SQL code tokenization creates static read-only syntax fragments where token index is safe */
  return sql.split('\n').map((line, lineIndex) => {
    const idx = line.indexOf('--');
    const code = idx >= 0 ? line.slice(0, idx) : line;
    const comment = idx >= 0 ? line.slice(idx) : '';
    const parts = code.split(KEYWORDS).map((part, partIndex) =>
      partIndex % 2 === 1 ? (
        <span key={`kw-${partIndex}`} className="sql-kw">
          {part}
        </span>
      ) : (
        <Fragment key={`p-${partIndex}`}>
          {part.split(/(\$\d+)/).map((p, pIndex) =>
            /^\$\d+$/.test(p) ? (
              <span key={`param-${pIndex}`} className="sql-param">
                {p}
              </span>
            ) : (
              p
            )
          )}
        </Fragment>
      )
    );
    return (
      <div key={`line-${lineIndex}`} className="sql-line">
        {parts}
        {comment && <span className="sql-comment">{comment}</span>}
        {line === '' && '\u00a0'}
      </div>
    );
  });
  /* eslint-enable react/no-array-index-key */
}

function CodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Ignore copy error
    }
  };

  return (
    <div className="code-wrapper">
      <button type="button" className="copy-btn" onClick={() => void handleCopy()} title="Copy to clipboard">
        {copied ? (
          <>
            <Check size={11} />
            <span>Copied</span>
          </>
        ) : (
          <>
            <Copy size={11} />
            <span>Copy</span>
          </>
        )}
      </button>
      <pre className="sql-code">{highlight(code)}</pre>
    </div>
  );
}

interface Props {
  response: SearchResponse | null;
  detail: DocumentDetail | null;
  searching: boolean;
}

export function SqlPane({ response, detail, searching }: Props) {
  return (
    <aside className="pane sql" aria-label="Search logic">
      <div className="pane-title">
        <div className="pane-title-left">
          <Terminal size={14} />
          <span>Execution & SQL Logic</span>
        </div>
      </div>

      <div className="pane-body">
        {!response && !searching && (
          <div className="notice dark">
            <p>Executed SQL, session configuration, and RRF fusion plans appear here upon running a search.</p>
          </div>
        )}
        {searching && (
          <div className="notice dark">
            <p>Executing query plan against Databricks Lakebase…</p>
          </div>
        )}

        {response && (
          <>
            <div className="metric-strip">
              <div className="metric-box">
                <span className="metric-label">Embedding</span>
                <span className="metric-val">
                  {response.mode === 'keyword' ? 'Skipped' : formatLatency(response.timings.embedMs)}
                </span>
              </div>
              <div className="metric-box">
                <span className="metric-label">Lakebase Query</span>
                <span className="metric-val">{formatLatency(response.timings.queryMs)}</span>
              </div>
              <div className="metric-box">
                <span className="metric-label">Mode</span>
                <span className="metric-val">{response.mode}</span>
              </div>
            </div>

            <div className="section-label">Execution Pipeline</div>
            <ol className="steps-list">
              {response.steps.map((s) => (
                <li key={s.name}>
                  <strong>{s.name}</strong>
                  <span>{s.description}</span>
                </li>
              ))}
            </ol>

            <div className="section-label">Session Settings</div>
            <div className="code-wrapper">
              <pre className="sql-code">{response.sessionSettings.map((s) => `${s};`).join('\n')}</pre>
            </div>

            <div className="section-label">Main Search Query</div>
            <CodeBlock code={response.sql} />

            <div className="section-label">Bound Parameters</div>
            <dl className="params-grid">
              {response.params.map((p, i) => (
                <Fragment key={`param-${i + 1}-${String(p)}`}>
                  <dt>${i + 1}</dt>
                  <dd>{p}</dd>
                </Fragment>
              ))}
            </dl>
          </>
        )}

        {detail && detail.sql && detail.sql.length > 0 && (
          <>
            <div className="section-label" style={{ marginTop: 24 }}>
              Selected Record Queries
            </div>
            {detail.sql.map((q) => (
              <CodeBlock key={q} code={q} />
            ))}
          </>
        )}
      </div>
    </aside>
  );
}
