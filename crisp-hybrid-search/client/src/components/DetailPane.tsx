import { formatDate, sourceLabel, type DocumentDetail } from '../api';

interface Props {
  detail: DocumentDetail | null;
  loading: boolean;
  error: string | null;
  hasResults: boolean;
  onSelect: (docId: string) => void;
}

function Field({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <div className="field">
      <dt>{label}</dt>
      <dd>{value || '—'}</dd>
    </div>
  );
}

export function DetailPane({ detail, loading, error, hasResults, onSelect }: Props) {
  return (
    <main className="pane detail" aria-label="Selected record">
      {loading && <div className="notice">Loading…</div>}
      {error && <div className="notice error">{error}</div>}
      {!detail && !loading && !error && (
        <div className="empty">
          <h2>{hasResults ? 'Select a result' : 'Hybrid clinical search'}</h2>
          <p>
            {hasResults
              ? 'Choose a record on the left to read it here.'
              : 'Keyword search finds exact terms, codes and names. Semantic search finds records with similar meaning. Hybrid fuses both.'}
          </p>
        </div>
      )}
      {detail && !loading && (
        <article>
          <div className="detail-head">
            <div className="tags">
              <span className="tag">{sourceLabel(detail.document.source_table)}</span>
              {detail.document.doc_type && <span className="tag">{detail.document.doc_type}</span>}
              {detail.document.result_flag && (
                <span className={`tag flag flag-${detail.document.result_flag.toLowerCase()}`}>
                  {detail.document.result_flag}
                </span>
              )}
            </div>
            <h1>{detail.document.title ?? detail.document.doc_id}</h1>
            <p className="muted">
              {formatDate(detail.document.event_at, true)}
              {detail.document.facility ? ` · ${detail.document.facility}` : ''}
              {detail.document.author ? ` · ${detail.document.author}` : ''}
            </p>
          </div>

          <section className="card">
            <h3>Record</h3>
            <p className="content">{detail.document.content}</p>
            <dl className="fields">
              <Field label="Document ID" value={detail.document.doc_id} />
              <Field label="Encounter" value={detail.document.encounter_id} />
              <Field label="ICD-10" value={detail.document.icd10_code} />
              <Field label="Diagnosis family" value={detail.document.dx_family} />
              <Field label="LOINC" value={detail.document.loinc_code} />
              <Field label="Order status" value={detail.document.order_status} />
            </dl>
          </section>

          <section className="card">
            <h3>Patient</h3>
            <dl className="fields">
              <Field label="Name" value={detail.document.full_name} />
              <Field label="Patient ID" value={detail.document.patient_id} />
              <Field label="Date of birth" value={formatDate(detail.document.date_of_birth)} />
              <Field label="Sex" value={detail.document.sex} />
              <Field label="ZIP" value={detail.document.zip_code} />
              <Field label="Insurance" value={detail.document.insurance_plan} />
              <Field label="Primary facility" value={detail.document.primary_facility} />
              <Field label="Last visited" value={formatDate(detail.document.last_visited)} />
              <Field label="Next scheduled visit" value={formatDate(detail.document.next_scheduled_visit, true)} />
              <Field label="High utilizer" value={detail.document.is_high_utilizer ? 'Yes' : 'No'} />
            </dl>
          </section>

          {detail.related.length > 0 && (
            <section className="card">
              <h3>Other records for this patient</h3>
              <ul className="related">
                {detail.related.map((r) => (
                  <li key={r.doc_id}>
                    <button type="button" onClick={() => onSelect(r.doc_id)}>
                      <span className="tag">{sourceLabel(r.source_table)}</span>
                      <span className="related-title">{r.title ?? r.doc_type ?? r.doc_id}</span>
                      <span className="muted">{formatDate(r.event_at)}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </article>
      )}
    </main>
  );
}
