// Deterministic dataset for the head-to-head. Issue-tracker shape (the same shape the
// in-repo `bench_graph_sources` benchmarks use), scaled up to a "large-ish" client dataset
// and shared byte-for-byte between both engines so the only variable is the store.
//
// Integer keys throughout: join/correlation keys are integers so neither engine pays a
// string-comparison tax the other avoids — the comparison is the dataflow, not key shape.

export interface UserRow {
  id: number;
  name: string;
  role: string;
}
export interface IssueRow {
  id: number;
  title: string;
  open: boolean;
  created: number;
  creatorID: number;
}
export interface CommentRow {
  id: number;
  issueID: number;
  body: string;
  creatorID: number;
}

export interface Dataset {
  users: UserRow[];
  issues: IssueRow[];
  comments: CommentRow[];
  counts: { users: number; issues: number; comments: number };
}

export interface Scale {
  name: string;
  users: number;
  issues: number;
  comments: number;
}

export const SCALES: Record<string, Scale> = {
  small: { name: "small", users: 100, issues: 1_000, comments: 5_000 },
  medium: { name: "medium", users: 300, issues: 5_000, comments: 25_000 },
  large: { name: "large", users: 1_000, issues: 10_000, comments: 50_000 },
  xl: { name: "xl", users: 2_000, issues: 20_000, comments: 100_000 },
};

export function resolveScale(): Scale {
  const requested = (process.env.SCALE ?? "medium").toLowerCase();
  const scale = SCALES[requested];
  if (scale) return scale;
  // explicit override: SCALE="users,issues,comments"
  const parts = requested.split(",").map((n) => Number(n.trim()));
  if (parts.length === 3 && parts.every((n) => Number.isFinite(n) && n > 0)) {
    return { name: requested, users: parts[0], issues: parts[1], comments: parts[2] };
  }
  throw new Error(`unknown SCALE "${requested}" (use small|medium|large|xl or "U,I,C")`);
}

/** Build the shared dataset. ~1/3 of issues are closed (`open = i % 3 !== 0`); `created`
 *  descends with id so "newest" is a stable, well-defined top-K; comments fan out evenly
 *  across issues (`issueID = i % issues`) so every issue has ~`comments/issues` children. */
export function generate(scale: Scale): Dataset {
  const users: UserRow[] = Array.from({ length: scale.users }, (_, i) => ({
    id: i,
    name: `User ${i}`,
    role: i % 10 === 0 ? "admin" : "member",
  }));
  const issues: IssueRow[] = Array.from({ length: scale.issues }, (_, i) => ({
    id: i,
    title: `Issue ${i}: a bug or a feature request`,
    open: i % 3 !== 0,
    created: scale.issues - i,
    creatorID: i % scale.users,
  }));
  const comments: CommentRow[] = Array.from({ length: scale.comments }, (_, i) => ({
    id: i,
    issueID: i % scale.issues,
    body: `Comment ${i} body text`,
    creatorID: i % scale.users,
  }));
  return {
    users,
    issues,
    comments,
    counts: { users: scale.users, issues: scale.issues, comments: scale.comments },
  };
}

// IDs used by the incremental path: chosen OUTSIDE the seeded ranges so an add+remove pair
// never collides with existing rows. `created` sorts the new issue to the front of any
// "newest" window so the limit case exercises the worst case (the window shifts).
export const probeIds = (scale: Scale) => ({
  newIssueId: scale.issues + 7,
  newIssueCreated: scale.issues + 1000,
  newCommentId: scale.comments + 7,
  // a valid, existing parent/creator the new rows attach to
  existingIssueId: 0,
  existingCreatorId: 0,
});

/** The viewport size for the realistic "UI views" ladder — how many rows a list pulls to
 *  fill a screen. Real client queries are bounded like this; they do not pull the table. */
export const VIEW_LIMIT = 50;
/** Comment-preview size for a list row ("issue → 3 recent comments"). */
export const PREVIEW_LIMIT = 3;

/** Probe rows + cursors for the bounded-view incremental path. Computed from the dataset so
 *  the page-2 insert lands inside page 2 on BOTH a cursor- (Rindle) and an offset-paginated
 *  (TanStack) query, keeping that comparison apples-to-apples. */
export function viewSetup(dataset: Dataset, scale: Scale) {
  const openByNewest = dataset.issues.filter((i) => i.open).sort((a, b) => b.created - a.created);
  const page1Last = openByNewest[VIEW_LIMIT - 1]; // cursor anchor: created of the 50th newest-open
  // a visible issue (in the newest-open viewport) whose children the incremental mutates
  const visibleIssueId = openByNewest[1]?.id ?? openByNewest[0].id;
  return {
    visibleIssueId,
    page1Cursor: page1Last.created,
    // a brand-new newest-open issue: lands at the TOP of every list view (the window shifts)
    newTopIssue: {
      id: scale.issues + 11,
      title: "incremental probe — new top issue",
      open: true,
      created: scale.issues + 5000,
      creatorID: 0,
    },
    // an issue whose `created` falls just past the page-1 cursor, so it enters page 2 at the top
    newPageIssue: {
      id: scale.issues + 12,
      title: "incremental probe — page-2 issue",
      open: true,
      created: page1Last.created - 0.5,
      creatorID: 0,
    },
    // a new comment on a visible issue (updates that row's count / preview / detail)
    newVisibleComment: {
      id: scale.comments + 11,
      issueID: visibleIssueId,
      body: "incremental probe comment",
      creatorID: 0,
    },
  };
}
