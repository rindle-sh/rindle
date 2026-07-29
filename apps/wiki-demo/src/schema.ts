// The ONE shared artifact for the live Wikipedia demo: schema + named queries, on pure
// `@rindle/client` (no native, no wasm). The Node backend (this package) and the product-page
// frontend BOTH import it — the server resolves the named query into the authoritative
// materialization, the browser builds the matching client view, and their result schemas agree
// by construction (same AST ⇒ same comparator/fingerprint at `hello`).

import {
  createSchema,
  defineQuery,
  gt,
  newQueryBuilder,
  number,
  string,
  table,
} from "@rindle/client";

// Three tables mirror the Wikimedia `recentchange` stream:
//   page   — a wiki page that's been edited (denormalized `edits` count + last-edit fields)
//   editor — a person editing (denormalized `edits` count + last-edit time)
//   edit   — one revision/change event, chained to its page by `page_id` AND its editor by `user`
// `edits` is a DENORMALIZED column on both `page` and `editor` (incremented by the ordered
// ingester); the boards rank by recency (`last_ts`), but the denormalized `edits` is still shown as
// each row's count and gates the editors board's `where edits > 1` filter — no count(child) aggregate
// needed, the same approach the prior demo used for HN `score`/`descendants`. The whole demo rides
// the ✅ shapes in docs/SUPPORTED_SHAPES.md: ordered-limit (Take) + a nested correlated relationship.
// The SAME `edit` stream feeds BOTH boards under two different correlations (page.id ← edit.page_id
// and editor.name ← edit.user), so the daemon keeps two deeply-nested materializations exact off one
// write stream — the newest pages and the newest editors, mirror images of each other.
export const page = table("page")
  .columns({
    id: string(), // stable key: `${wiki}:${title}` (primary key)
    wiki: string(), // e.g. "enwiki"
    title: string(), // page title
    url: string(), // canonical page url
    edits: number(), // edits seen in the retained window (denormalized, ingester-maintained)
    last_ts: number(), // unix seconds of the newest edit
    last_user: string(), // author of the newest edit
  })
  .primaryKey("id");

export const editor = table("editor")
  .columns({
    name: string(), // editor username / IP (primary key)
    edits: number(), // edits seen in the retained window (denormalized, ingester-maintained)
    last_ts: number(), // unix seconds of this editor's newest edit
  })
  .primaryKey("name");

export const edit = table("edit")
  .columns({
    id: string(), // revision id (unique per change)
    page_id: string(), // → page.id
    user: string(), // editor username → editor.name
    comment: string(), // edit summary (truncated)
    ts: number(), // unix seconds
    delta: number(), // byte change (new length − old length)
    bot: number(), // 0 | 1
  })
  .primaryKey("id");

export const schema = createSchema({ tables: [page, editor, edit] });

/** A wire row as it arrives in the view. `edits_recent` is present only on the top-N parent rows
 *  (the nested `.sub`). */
export type PageRow = {
  id: string;
  wiki: string;
  title: string;
  url: string;
  edits: number;
  last_ts: number;
  last_user: string;
  edits_recent?: EditRow[];
};

export type EditRow = {
  id: string;
  page_id: string;
  user: string;
  comment: string;
  ts: number;
  delta: number;
  bot: number;
};

/** A wire row for the "most-recent editors" board; `edits_recent` carries that editor's newest
 *  edits (the nested `.sub`, correlated by `editor.name ← edit.user`). */
export type EditorRow = {
  name: string;
  edits: number;
  last_ts: number;
  edits_recent?: EditRow[];
};

/** A single change as the source emits it (a page-upsert + the edit that caused it) — the shape
 *  the ingester writes. The ingester turns each into a `page` upsert + an `edit` insert. */
export interface WikiEvent {
  page: { id: string; wiki: string; title: string; url: string; ts: number; user: string };
  edit: { id: string; user: string; comment: string; ts: number; delta: number; bot: number };
}

export const TOP_LIMIT = 20;
export const EDIT_LIMIT = 5;

export type Rank = "active" | "latest";

const q = newQueryBuilder(schema);

/** Top pages with their few newest edits nested — the "live edit board" shape. `active` ranks by
 *  edit count (the most-churned pages float up); `latest` ranks by most-recently-edited. The
 *  correlation (page.id ← edit.page_id) is the same nested shape the prior demo shipped as
 *  story ← comments. Shared by the server and the browser so the two ASTs — and therefore the two
 *  view schemas — are identical. */
export function topEdited(rank: Rank) {
  const ranked =
    rank === "latest"
      ? q.page.orderBy("last_ts", "desc").limit(TOP_LIMIT)
      : q.page.orderBy("edits", "desc").limit(TOP_LIMIT);
  return ranked.sub(
    "edits_recent",
    edit,
    { parent: ["id"], child: ["page_id"] },
    (e) => e.orderBy("ts", "desc").limit(EDIT_LIMIT),
  );
}

/** The "most-recent editors" board: the editors who edited most recently, each with their newest
 *  edits nested (correlated by `editor.name ← edit.user`). Ordered by `last_ts` so it stays as live
 *  as the "just edited" page board — its mirror image: the SAME `edit` stream, grouped by WHO instead
 *  of by WHAT. The `.where.edits(gt(1))` drops drive-by one-off editors (anonymous IPs, single
 *  vandalisms) so the board is recurring contributors — a Filter operator riding under the Take, kept
 *  exact incrementally as editors cross the threshold and as their recency changes. */
export function topRecentEditors() {
  return q.editor
    .where.edits(gt(1))
    .orderBy("last_ts", "desc")
    .limit(TOP_LIMIT)
    .sub(
      "edits_recent",
      edit,
      { parent: ["name"], child: ["user"] },
      (e) => e.orderBy("ts", "desc").limit(EDIT_LIMIT),
    );
}

/** The browser-side named queries — the two boards the demo keeps prepared. `queries.latest()` is the
 *  pages edited just now; `queries.recentEditors()` is the editors editing just now (its mirror). The
 *  `RemoteBackend` sends only the name upstream and the server resolves the twin. (`topEdited("active")`
 *  — top pages by cumulative edit count — is still a valid shape, just not surfaced by this demo.) */
export const queries = {
  latest: defineQuery("latest", () => topEdited("latest")),
  recentEditors: defineQuery("recentEditors", () => topRecentEditors()),
};
