import { useCallback, useRef, useState } from 'react';
import { getDocument, search, type DocumentDetail, type SearchMode, type SearchResponse } from './api';
import { DetailPane } from './components/DetailPane';
import { Header } from './components/Header';
import { ResultsPane } from './components/ResultsPane';
import { SqlPane } from './components/SqlPane';

const SQL_COLLAPSED_KEY = 'crisp.sqlPaneCollapsed';

export default function App() {
  const [query, setQuery] = useState('');
  const [mode, setMode] = useState<SearchMode>('hybrid');
  const [sources, setSources] = useState<string[]>([]);

  const [response, setResponse] = useState<SearchResponse | null>(null);
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<DocumentDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  const [sqlCollapsed, setSqlCollapsed] = useState(() => localStorage.getItem(SQL_COLLAPSED_KEY) === 'true');
  const toggleSql = () => {
    setSqlCollapsed(!sqlCollapsed);
    localStorage.setItem(SQL_COLLAPSED_KEY, String(!sqlCollapsed));
  };

  // Request counters: only the latest search / record request may update state, so a slow earlier response
  // cannot overwrite a newer one or resurrect a record after a new search has cleared the panes.
  const searchSeq = useRef(0);
  const detailSeq = useRef(0);

  const select = useCallback(async (docId: string) => {
    const seq = ++detailSeq.current;
    setSelectedId(docId);
    setLoadingDetail(true);
    setDetailError(null);
    try {
      const result = await getDocument(docId);
      if (seq === detailSeq.current) setDetail(result);
    } catch (err) {
      if (seq === detailSeq.current) {
        setDetail(null);
        setDetailError((err as Error).message);
      }
    } finally {
      if (seq === detailSeq.current) setLoadingDetail(false);
    }
  }, []);

  const runSearch = useCallback(async () => {
    if (!query.trim()) return;
    const seq = ++searchSeq.current;
    detailSeq.current++; // abandon any record request still in flight
    // Clear the previous search first so stale results, record and SQL are not shown while the new search runs.
    setResponse(null);
    setSelectedId(null);
    setDetail(null);
    setDetailError(null);
    setLoadingDetail(false);
    setSearchError(null);
    setSearching(true);
    try {
      const result = await search(query.trim(), mode, sources);
      if (seq === searchSeq.current) setResponse(result);
    } catch (err) {
      if (seq === searchSeq.current) setSearchError((err as Error).message);
    } finally {
      if (seq === searchSeq.current) setSearching(false);
    }
  }, [query, mode, sources]);

  return (
    <div className={sqlCollapsed ? 'app sql-collapsed' : 'app'}>
      <Header
        query={query}
        onQuery={setQuery}
        mode={mode}
        onMode={setMode}
        sources={sources}
        onSources={setSources}
        onSubmit={() => void runSearch()}
        searching={searching}
      />
      <ResultsPane
        response={response}
        searching={searching}
        error={searchError}
        selectedId={selectedId}
        onSelect={(id) => void select(id)}
      />
      <DetailPane
        detail={detail}
        loading={loadingDetail}
        error={detailError}
        hasResults={response !== null}
        onSelect={(id) => void select(id)}
      />
      <SqlPane
        response={response}
        detail={detail}
        searching={searching}
        collapsed={sqlCollapsed}
        onToggle={toggleSql}
      />
    </div>
  );
}
