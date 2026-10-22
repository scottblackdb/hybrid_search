import { useCallback, useState } from 'react';
import { getDocument, search, type DocumentDetail, type SearchMode, type SearchResponse } from './api';
import { DetailPane } from './components/DetailPane';
import { Header } from './components/Header';
import { ResultsPane } from './components/ResultsPane';
import { SqlPane } from './components/SqlPane';

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

  const select = useCallback(async (docId: string) => {
    setSelectedId(docId);
    setLoadingDetail(true);
    setDetailError(null);
    try {
      setDetail(await getDocument(docId));
    } catch (err) {
      setDetail(null);
      setDetailError((err as Error).message);
    } finally {
      setLoadingDetail(false);
    }
  }, []);

  const runSearch = useCallback(async () => {
    if (!query.trim()) return;
    // Clear the previous search first so stale results, record and SQL are not shown while the new search runs.
    setResponse(null);
    setSelectedId(null);
    setDetail(null);
    setDetailError(null);
    setSearchError(null);
    setSearching(true);
    try {
      setResponse(await search(query.trim(), mode, sources));
    } catch (err) {
      setSearchError((err as Error).message);
    } finally {
      setSearching(false);
    }
  }, [query, mode, sources]);

  return (
    <div className="app">
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
      <SqlPane response={response} detail={detail} searching={searching} />
    </div>
  );
}
