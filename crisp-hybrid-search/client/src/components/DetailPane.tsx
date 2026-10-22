import { FileText, User, History, Search, AlertCircle, Loader2 } from 'lucide-react';
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
      {loading && (
        <div className="notice">
          <Loader2 size={20} className="animate-spin" />
          <span>Loading record details…</span>
        </div>
      )}
      {error && (
        <div className="notice error">
          <AlertCircle size={16} />
          <span>{error}</span>
        </div>
      )}
      {!detail && !loading && !error && (
        <div className="empty-state">
          <div className="empty-icon-circle">
            <Search size={24} />
          </div>
          <h2>{hasResults ? 'Select a record' : 'Hybrid Clinical Search'}</h2>
          <p>
            {hasResults
              ? 'Select a result from the list to inspect clinical notes, demographics, and coded metrics.'
              : 'Combine lexical keyword search with vector semantic similarity to explore longitudinal patient encounters and lab anomalies.'}
          </p>
        </div>
      )}
      {detail && !loading && (
        <article>
          <div className="detail-header-card">
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
            <div className="meta-row">
              <span>{formatDate(detail.document.event_at, true)}</span>
              {detail.document.facility && <span>· {detail.document.facility}</span>}
              {detail.document.author && <span>· {detail.document.author}</span>}
            </div>
          </div>

          <section className="card">
            <div className="card-title">
              <FileText size={13} />
              <span>Clinical Record</span>
            </div>
            {detail.document.content && <div className="content-block">{detail.document.content}</div>}
            <dl className="fields">
              <Field label="Document ID" value={detail.document.doc_id} />
              <Field label="Encounter" value={detail.document.encounter_id} />
              <Field label="ICD-10" value={detail.document.icd10_code} />
              <Field label="Diagnosis Family" value={detail.document.dx_family} />
              <Field label="LOINC" value={detail.document.loinc_code} />
              <Field label="Order Status" value={detail.document.order_status} />
            </dl>
          </section>

          <section className="card">
            <div className="card-title">
              <User size={13} />
              <span>Patient Information</span>
            </div>
            <dl className="fields">
              <Field label="Name" value={detail.document.full_name} />
              <Field label="Patient ID" value={detail.document.patient_id} />
              <Field label="Date of Birth" value={formatDate(detail.document.date_of_birth)} />
              <Field label="Sex" value={detail.document.sex} />
              <Field label="ZIP Code" value={detail.document.zip_code} />
              <Field label="Insurance Plan" value={detail.document.insurance_plan} />
              <Field label="Primary Facility" value={detail.document.primary_facility} />
              <Field label="Last Visited" value={formatDate(detail.document.last_visited)} />
              <Field label="Next Scheduled" value={formatDate(detail.document.next_scheduled_visit, true)} />
              <Field label="High Utilizer" value={detail.document.is_high_utilizer ? 'Yes' : 'No'} />
            </dl>
          </section>

          {detail.related.length > 0 && (
            <section className="card">
              <div className="card-title">
                <History size={13} />
                <span>Other Records For This Patient</span>
              </div>
              <ul className="related-list">
                {detail.related.map((r) => (
                  <li key={r.doc_id}>
                    <button type="button" className="related-btn" onClick={() => onSelect(r.doc_id)}>
                      <span className="tag">{sourceLabel(r.source_table)}</span>
                      <span className="related-title">{r.title ?? r.doc_type ?? r.doc_id}</span>
                      <span className="result-date">{formatDate(r.event_at)}</span>
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
