// The app chrome: the persistent frame every route renders inside. It owns the app state that
// outlives a view switch — the live growing issue window, the active search, the current user, the
// rejection toast — and lays out the panes. The SELECTED issue and the CREATE pane live in the URL
// (`?issue=` / `?new=`), so they survive reload + are shareable; the view itself is the route path
// (`/`, `/board`, `/activity`), rendered through `children` (the matched route's <Outlet> output).
// The per-view bodies live in src/routes/*; the shared window they read is handed down via
// IssueViewProvider; the pure helpers live in ./lib.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { useRoot } from "@rindle/react";

import { MAX_ISSUES, PAGE_SIZE, normalizeOwner } from "../shared/app-def.ts";
import { IssueCardFragment, issuesPageQuery } from "./components/IssueListItem.queries.ts";
import { tagOptionsQuery } from "./components/TagChip.queries.ts";
import { usersQuery } from "./components/UserBadge.queries.ts";
import { SSR_USER, currentUser, onRejection, setCurrentUser } from "./rindle-client.ts";
import { IssueViewProvider } from "./app-context.ts";
import type { AppSearch } from "./app-context.ts";
import { CreateIssueDetail } from "./components/CreateIssueDetail.tsx";
import type { ComboItem } from "./components/Combobox.tsx";
import { IssueDetail } from "./components/IssueDetail.tsx";
import { MetricsBar } from "./components/MetricsBar.tsx";
import { TokenSearch } from "./components/TokenSearch.tsx";
import { TopBar } from "./components/TopBar.tsx";
import { ViewSwitcher } from "./components/ViewSwitcher.tsx";
import type { IssueRow } from "./lib/issue.ts";
import { tokensToFilter } from "./lib/search.ts";
import type { SearchToken } from "./lib/search.ts";

export function AppChrome({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  // The selection + the create pane are URL state (validated on the root route), so they survive a
  // reload and are deep-linkable. The view path stays put — only the search params change.
  const search = useSearch({ strict: false }) as AppSearch;
  const selectedId = search.issue ?? null;
  const creatingIssue = search.new === true;

  const setSelection = useCallback(
    (next: AppSearch) => {
      void navigate({ to: ".", search: (prev) => ({ ...(prev as AppSearch), ...next }) });
    },
    [navigate],
  );
  // Open a selected issue's detail / open the create form — shared by every view (list rows, board
  // cards, feed items all route here), and it stays on the current view path.
  const openIssue = useCallback((id: string) => setSelection({ issue: id, new: undefined }), [setSelection]);
  const openCreate = useCallback(() => setSelection({ issue: undefined, new: true }), [setSelection]);
  const closeDetail = useCallback(() => setSelection({ issue: undefined, new: undefined }), [setSelection]);

  // Infinite scroll = ONE named window whose `limit` ratchets up a page at a time (not a stack of
  // cursor windows folded together). Growing the limit re-keys the subscription, so the new lease is
  // `unknown` (loading) and reports empty/partial for a frame before the server hydrates it. We
  // bridge that frame with the prior rows (a valid prefix of the larger window) until the new window
  // is `complete` (server-authoritative) — so the list never blanks AND a short window is only read
  // as "the end" once it is trustworthy (no mid-hydration end-flash). An optimistic write turns the
  // SAME-limit window `unknown` too, but that is NOT bridged: `live` already holds the prediction.
  const [searchTokens, setSearchTokens] = useState<SearchToken[]>([]);
  // The faceted search is pushed to the SERVER: the daemon materializes an already-filtered window
  // (and the limit resets to one page on every filter change), instead of the client scanning a
  // loaded window. Equal filters share a subscription (the canonical, id-free filter is part of the
  // named query's identity).
  const filter = useMemo(() => tokensToFilter(searchTokens), [searchTokens]);
  const [limit, setLimit] = useState(PAGE_SIZE);
  const [live, { status }] = useRoot(issuesPageQuery, { limit, filter }, IssueCardFragment);
  const settled = useRef<{ limit: number; rows: readonly IssueRow[] }>({ limit, rows: live });
  const loadingNewWindow = limit !== settled.current.limit && status !== "complete";
  const issues = loadingNewWindow ? settled.current.rows : live;
  if (!loadingNewWindow) settled.current = { limit, rows: live };

  // `currentUser()` reads localStorage — absent during SSR, and even on the client the first
  // (hydration) render must byte-match the server's placeholder. So seed from SSR_USER on both
  // sides, then adopt this browser's real persisted identity right after hydration.
  const [user, setUser] = useState(SSR_USER);
  useEffect(() => setUser(currentUser()), []);
  const [toast, setToast] = useState<string | null>(null);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const unsubscribe = onRejection((envelope, reason) => {
      if (timer) clearTimeout(timer);
      setToast(`${envelope.name} was rejected: ${reason}`);
      timer = setTimeout(() => setToast(null), 4200);
    });
    return () => {
      unsubscribe();
      if (timer) clearTimeout(timer);
    };
  }, []);

  // Read the affordances off the SETTLED window (never a window still loading a grow). A full window
  // ⇒ there may be more, so offer "load more" (until the ceiling). A short settled window ⇒ the true
  // end (and the ceiling is its own kind of end). The first hydrate shows a spinner, not "no issues".
  const canLoadMore = !loadingNewWindow && issues.length >= limit && limit < MAX_ISSUES;
  const reachedEnd = !loadingNewWindow && (issues.length < limit || limit >= MAX_ISSUES);
  const loadingFirst = status !== "complete" && issues.length === 0;

  // Grow the window by a page. Stable + guarded so the scroll observer can fire it freely: a no-op
  // unless the current window is settled + full + below the ceiling (`canLoadMore`), which keeps a
  // burst of intersections from stacking limit jumps past the data or past a still-loading grow.
  const canLoadMoreRef = useRef(canLoadMore);
  canLoadMoreRef.current = canLoadMore;
  const loadMore = useCallback(() => {
    if (canLoadMoreRef.current) setLimit((prev) => Math.min(prev + PAGE_SIZE, MAX_ISSUES));
  }, []);

  // Changing the filter is a fresh read: reset the window back to one page so we don't carry a
  // large grown window (from a prior filter) into the new one.
  const changeSearch = useCallback((tokens: SearchToken[]) => {
    setSearchTokens(tokens);
    setLimit(PAGE_SIZE);
  }, []);

  // The whole (small) user table, live — powers the user switcher + every owner/tag picker.
  const [userRows] = useRoot(usersQuery);
  const userItems = useMemo<ComboItem[]>(
    () => (userRows ?? []).map((row) => ({ value: row.id, label: row.name })),
    [userRows],
  );
  const [tagRows] = useRoot(tagOptionsQuery);
  const tagOptions = useMemo(
    () => Array.from(new Set(tagRows.map((row) => row.name))).sort((a, b) => a.localeCompare(b)),
    [tagRows],
  );

  const detailOpen = creatingIssue || selectedId !== null;

  const viewValue = useMemo(
    () => ({ issues, filter, user, selectedId, loadingFirst, canLoadMore, reachedEnd, loadMore, onSelect: openIssue }),
    [issues, filter, user, selectedId, loadingFirst, canLoadMore, reachedEnd, loadMore, openIssue],
  );

  return (
    <main className="it-root">
      <TopBar
        user={user}
        users={userItems}
        onSelect={(value) => {
          const next = normalizeOwner(value);
          setUser(next);
          setCurrentUser(next);
        }}
      />

      {/* The live footprint strip — renders only when the bot swarm's /metrics tier is up. */}
      <MetricsBar />

      <section className="it-shell">
        <section className="it-toolbar">
          <div className="it-toolbar-top">
            <ViewSwitcher />
            <div className="it-toolbar-actions">
              <button type="button" className="it-quiet" onClick={() => changeSearch([])}>
                Clear filters
              </button>
              <button type="button" className="it-primary compact" onClick={openCreate}>
                New issue
              </button>
            </div>
          </div>
          <TokenSearch tokens={searchTokens} owners={userItems} tags={tagOptions} onChange={changeSearch} />
        </section>

        <div className={detailOpen ? "it-board has-detail" : "it-board"}>
          <IssueViewProvider value={viewValue}>{children}</IssueViewProvider>

          {creatingIssue ? (
            <CreateIssueDetail
              user={user}
              users={userItems}
              tagOptions={tagOptions}
              onCancel={closeDetail}
              onCreated={openIssue}
            />
          ) : (
            selectedId && (
              <IssueDetail
                issueId={selectedId}
                user={user}
                users={userItems}
                tagOptions={tagOptions}
                onClose={closeDetail}
              />
            )
          )}
        </div>
      </section>

      {toast && <div className="it-toast">{toast}</div>}
    </main>
  );
}
