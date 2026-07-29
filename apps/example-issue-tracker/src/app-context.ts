// The seam between the app chrome (src/AppChrome.tsx — the toolbar, the live issue window, the
// detail pane) and the per-view route components (src/routes/{index,board,activity}.tsx). The chrome
// owns the shared, always-subscribed list window + the active filter + the URL-driven selection; each
// view route reads the slice it renders off this context instead of taking a long prop chain.

import { createContext, useContext } from "react";

import type { IssueFilter } from "../shared/app-def.ts";
import type { IssueRow } from "./lib/issue.ts";

export interface IssueViewValue {
  /** The live, paginated issue window (the list root query) — kept subscribed by the chrome so the
   *  tag picker + the board's facets stay populated across every view. */
  issues: readonly IssueRow[];
  /** The active faceted filter (the server-side window predicate) shared by the list + the board. */
  filter: IssueFilter;
  /** The current user — the session identity the chrome owns. The "Mine" view passes it as the
   *  CONTEXT of its `myIssues` query (the owner scope travels off-wire, not as a filter arg). */
  user: string;
  /** The currently open issue id, or `null` — driven by the `?issue=` search param. */
  selectedId: string | null;
  /** The first window is still loading (not yet server-authoritative) with nothing to show. */
  loadingFirst: boolean;
  canLoadMore: boolean;
  reachedEnd: boolean;
  loadMore: () => void;
  /** Open an issue's detail pane (writes the `?issue=` search param, preserving the view path). */
  onSelect: (id: string) => void;
}

/** The app's URL search params (validated on the root route, shared by every view). The selection
 *  and the create pane live here — so `/board?issue=42` and `/?new=true` are shareable deep links. */
export interface AppSearch {
  issue?: string;
  new?: boolean;
}

const IssueViewContext = createContext<IssueViewValue | null>(null);

export const IssueViewProvider = IssueViewContext.Provider;

export function useIssueView(): IssueViewValue {
  const value = useContext(IssueViewContext);
  if (!value) throw new Error("useIssueView must be used within <AppChrome>.");
  return value;
}
