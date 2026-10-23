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

function RecordView({ detail, onSelect }: { detail: DocumentDetail; onSelect: (docId: string) => void }) {
  const { document: doc, related } = detail;
  return (
    <article>
      <div className="detail-header-card">
        <div className="tags">
          <span className="tag">{sourceLabel(doc.source_table)}</span>
          {doc.doc_type && <span className="tag">{doc.doc_type}</span>}
          {doc.result_flag && (
            <span className={`tag flag flag-${doc.result_flag.toLowerCase()}`}>{doc.result_flag}</span>
          )}
        </div>
        <h1>{doc.title ?? doc.doc_id}</h1>
        <div className="meta-row">
          <span>{formatDate(doc.event_at, true)}</span>
          {doc.facility && <span>· {doc.facility}</span>}
          {doc.author && <span>· {doc.author}</span>}
        </div>
      </div>

      <section className="card">
        <div className="card-title">
          <FileText size={13} />
          <span>Clinical Record</span>
        </div>
        {doc.content && <div className="content-block">{doc.content}</div>}
        <dl className="fields">
          <Field label="Document ID" value={doc.doc_id} />
          <Field label="Encounter" value={doc.encounter_id} />
          <Field label="ICD-10" value={doc.icd10_code} />
          <Field label="Diagnosis Family" value={doc.dx_family} />
          <Field label="LOINC" value={doc.loinc_code} />
          <Field label="Order Status" value={doc.order_status} />
        </dl>
      </section>

      <section className="card">
        <div className="card-title">
          <User size={13} />
          <span>Patient Information</span>
        </div>
        <dl className="fields">
          <Field label="Name" value={doc.full_name} />
          <Field label="Patient ID" value={doc.patient_id} />
          <Field label="Date of Birth" value={formatDate(doc.date_of_birth)} />
          <Field label="Sex" value={doc.sex} />
          <Field label="ZIP Code" value={doc.zip_code} />
          <Field label="Insurance Plan" value={doc.insurance_plan} />
          <Field label="Primary Facility" value={doc.primary_facility} />
          <Field label="Last Visited" value={formatDate(doc.last_visited)} />
          <Field label="Next Scheduled" value={formatDate(doc.next_scheduled_visit, true)} />
          <Field label="High Utilizer" value={doc.is_high_utilizer ? 'Yes' : 'No'} />
        </dl>
      </section>

      {related.length > 0 && (
        <section className="card">
          <div className="card-title">
            <History size={13} />
            <span>Other Records For This Patient</span>
          </div>
          <ul className="related-list">
            {related.map((r) => (
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
      {detail && !loading && <RecordView detail={detail} onSelect={onSelect} />}
    </main>
  );
}
