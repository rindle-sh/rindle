// The list pane: header (title + clear/new actions), the issue rows, and the infinite-scroll
// machinery. This component owns the sentinel + IntersectionObserver and calls `onLoadMore` as it
// nears the viewport; the App owns the page state that `onLoadMore` actually advances.

import { useEffect, useRef } from "react";
import { fragmentKey } from "@rindle/react";

import { PAGE_SIZE } from "../../shared/app-def.ts";
import { IssueListItem } from "./IssueListItem.tsx";
import type { IssueRow } from "../lib/issue.ts";

export function IssueList({
  issues,
  selectedId,
  loading,
  canLoadMore,
  showEnd,
  onSelect,
  onLoadMore,
  eyebrow = "Issue list",
  title = "Issues",
  empty,
}: {
  issues: readonly IssueRow[];
  selectedId: string | null;
  /** The first window hasn't become server-authoritative yet (query `resultType` is not `complete`)
   *  and there's nothing to show — distinguishes "still loading" from a genuinely empty result. */
  loading: boolean;
  canLoadMore: boolean;
  showEnd: boolean;
  onSelect: (id: string) => void;
  onLoadMore: () => void;
  /** Pane header copy — overridden by per-view panes (e.g. the "Mine" view). */
  eyebrow?: string;
  title?: string;
  /** Copy for the genuinely-empty (not loading) state. Defaults to the list's "no matches" line. */
  empty?: { title: string; hint?: string };
}) {
  const sentinelRef = useRef<HTMLDivElement>(null);
  const loadMoreRef = useRef(onLoadMore);
  const observedOnceRef = useRef(false);
  const wasIntersectingRef = useRef(false);
  loadMoreRef.current = onLoadMore;

  // Wire the observer once against the always-mounted sentinel; it forwards to the latest
  // onLoadMore via a ref so this effect never has to re-run. The first observer notification is
  // just the page's initial layout state; ignoring it prevents SSR/hydration from immediately
  // growing the window when the sentinel starts inside a tall viewport or restored scroll position.
  useEffect(() => {
    const node = sentinelRef.current;
    if (!node) return;
    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        if (!entry) return;
        const isIntersecting = entry.isIntersecting;
        if (!observedOnceRef.current) {
          observedOnceRef.current = true;
          wasIntersectingRef.current = isIntersecting;
          return;
        }
        if (isIntersecting && !wasIntersectingRef.current) loadMoreRef.current();
        wasIntersectingRef.current = isIntersecting;
      },
      { rootMargin: "600px 0px" },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  return (
    <section className="it-list-pane" aria-label={title}>
      <div className="it-pane-head">
        <div>
          <p className="it-eyebrow">{eyebrow}</p>
          <h1>{title}</h1>
        </div>
      </div>

      <ul className="it-list">
        {issues.map((issue) => (
          <IssueListItem
            key={fragmentKey(issue)}
            issue={issue}
            selectedId={selectedId}
            onSelect={onSelect}
          />
        ))}
      </ul>
      {issues.length === 0 &&
        (loading ? (
          <p className="it-empty">Loading issues…</p>
        ) : (
          <div className="it-empty-state" role="status">
            <span className="it-empty-state-mark" aria-hidden="true" />
            <p className="it-empty-state-title">{empty?.title ?? "No matching issues."}</p>
            {empty?.hint && <p className="it-empty-state-hint">{empty.hint}</p>}
          </div>
        ))}
      <div ref={sentinelRef} className="it-sentinel" aria-hidden="true" />
      {canLoadMore && (
        <button className="it-more" type="button" onClick={onLoadMore}>
          Load {PAGE_SIZE} more
        </button>
      )}
      {showEnd && <p className="it-end">You&rsquo;ve reached the end.</p>}
    </section>
  );
}
