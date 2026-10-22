import { Fragment } from 'react';
import { formatLatency, type DocumentDetail, type SearchResponse } from '../api';

const KEYWORDS =
  /\b(WITH|AS|SELECT|FROM|WHERE|AND|OR|ORDER BY|OVER|LIMIT|JOIN|FULL JOIN|USING|ON|ANY|ARRAY|NULL|DESC|IS|SET LOCAL|BEGIN|COMMIT)\b/g;

function highlight(sql: string) {
  // Comments first, then keywords / parameters in the remaining text.
  return sql.split('\n').map((line, i) => {
    const idx = line.indexOf('--');
    const code = idx >= 0 ? line.slice(0, idx) : line;
    const comment = idx >= 0 ? line.slice(idx) : '';
    const parts = code.split(KEYWORDS).map((part, j) =>
      j % 2 === 1 ? (
        <span key={j} className="sql-kw">
          {part}
        </span>
      ) : (
        <Fragment key={j}>
          {part.split(/(\$\d+)/).map((p, k) =>
            /^\$\d+$/.test(p) ? (
              <span key={k} className="sql-param">
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
      <div key={i} className="sql-line">
        {parts}
        {comment && <span className="sql-comment">{comment}</span>}
        {line === '' && '\u00a0'}
      </div>
    );
  });
}

interface Props {
  response: SearchResponse | null;
  detail: DocumentDetail | null;
  searching: boolean;
}

export function SqlPane({ response, detail, searching }: Props) {
  return (
    <aside className="pane sql" aria-label="Search logic">
      <div className="pane-title">Search logic</div>
      <div className="pane-body">
        {!response && !searching && (
          <div className="notice dark">The SQL executed against Lakebase appears here after you search.</div>
        )}
        {searching && <div className="notice dark">Running query…</div>}
        {response && (
          <>
            <div className="timings">
              <div>
                <span>Embedding</span>
                <strong>{response.mode === 'keyword' ? 'skipped' : formatLatency(response.timings.embedMs)}</strong>
              </div>
              <div>
                <span>Lakebase query</span>
                <strong>{formatLatency(response.timings.queryMs)}</strong>
              </div>
              <div>
                <span>Mode</span>
                <strong>{response.mode}</strong>
              </div>
            </div>

            <h4>Steps</h4>
            <ol className="steps">
              {response.steps.map((s) => (
                <li key={s.name}>
                  <strong>{s.name}</strong>
                  <span>{s.description}</span>
                </li>
              ))}
            </ol>

            <h4>Session settings</h4>
            <pre className="sql-code">{response.sessionSettings.map((s) => `${s};`).join('\n')}</pre>

            <h4>Query</h4>
            <pre className="sql-code">{highlight(response.sql)}</pre>

            <h4>Parameters</h4>
            <dl className="params">
              {response.params.map((p, i) => (
                <Fragment key={i}>
                  <dt>${i + 1}</dt>
                  <dd>{p}</dd>
                </Fragment>
              ))}
            </dl>
          </>
        )}

        {detail && (
          <>
            <h4>Selected record queries</h4>
            {detail.sql.map((q, i) => (
              <pre key={i} className="sql-code">
                {highlight(q)}
              </pre>
            ))}
          </>
        )}
      </div>
    </aside>
  );
}
