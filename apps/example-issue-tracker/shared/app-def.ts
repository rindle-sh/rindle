// The shared CONTRACT root: the NORMALIZED schema, the faceted-filter logic, normalization, and the
// client's PREDICTED mutators. Both tiers import it — the browser for everything, the API server for
// the schema + filter + normalization, and it holds the AUTHORITATIVE mutator twins (server/api.ts)
// with the same names.
//
// This module is the leaf of the contract DAG: it depends on nothing app-internal. The named root
// queries AND the per-component SELECTIONS they compose are co-located with their components in
// `src/components/*.queries.ts` (Relay-style co-location) — each a singular `defineQuery`/
// `defineFragment`. Keeping the schema here, free of those imports, is what keeps that graph acyclic.
//
// The data is normalized across four tables — `user`, `issue`, `tag`, `comment` — joined back
// together at query time with correlated subqueries (`sub`). The SQLite DDL (with the indices that
// make those `sub` joins fast in both directions) lives in `migrations/0001_init.sql`, and the
// `@rindle/client` table schema is GENERATED from it into `shared/schema.gen.ts` (re-exported below).

import {
  defineMutators,
  defineRelationships,
  eq,
  exists,
  fieldCondition,
  ilike,
  newQueryBuilder,
  or,
  refineSchema,
  refineTable,
  rel,
  SCHEMA,
  string,
} from "@rindle/client";
import type { Cond, IsoTxOf, MutationGen, MutationOp, MutatorCtx, Query, Row } from "@rindle/client";
import type { ClientRegistry } from "@rindle/optimistic";
import { z } from "zod";

// The normalized schema is GENERATED from the SQL migrations (migrations/*.sql) into ./schema.gen.ts
// by `rindle schema gen` — `pnpm dev` runs `rindle up --gen`, re-emitting it on every migration
// change, so the DDL is the single source of truth and the TS schema can't drift from it. We import
// the tables + `schema` here and re-export them below, keeping this contract root the one import for
// app code.
import { comment, issue as issueGen, schema as schemaGen, tag, user } from "./schema.gen.ts";

export const ISSUE_STATUSES = ["backlog", "todo", "in-progress", "review", "done"] as const;
export type IssueStatus = (typeof ISSUE_STATUSES)[number];

export const ISSUE_PRIORITIES = ["urgent", "high", "medium", "low"] as const;
export type IssuePriority = (typeof ISSUE_PRIORITIES)[number];

// --------------------------------------------------------------------------- tables (generated)
//
// Re-export the generated tables + `schema` so app code keeps importing the schema from this one
// contract root. The four normalized tables — `user`, `issue`, `tag`, `comment` — and their column
// kinds come straight from migrations/0001_init.sql (TEXT → string(), REAL → number()).
//
// SQL can't carry the `status`/`priority` LITERAL UNIONS, so the generated columns are plain
// `string()`. The refinement below narrows them ONCE, in this hand-written module, so it survives
// every regen of schema.gen.ts: `refineTable` re-types the generated def (narrowing `Issue`, the
// field conditions, and the `rels` anchored on it), and `refineSchema` swaps it into the schema
// (narrowing every `q.issue` root and projected row). Runtime-validated identity — the wire shape
// stays exactly the daemon's. The `ISSUE_STATUSES`/`ISSUE_PRIORITIES` arrays above stay the source
// of truth for the VALUES (the zod mutator enums below still validate them authoritatively).
export const issue = refineTable(issueGen, {
  status: string<IssueStatus>(),
  priority: string<IssuePriority>(),
});
export const schema = refineSchema(schemaGen, { tables: [issue] });
export { comment, tag, user };

// Bind the mutator authoring surface to THIS schema: `tx` in every `shared(...)` below is typed to
// the four tables, so `tx.insert`/`update`/`delete` check table + column names, value types (incl.
// the refined `status`/`priority` unions), nullable-omit, and the exact pk columns — at compile time.
const { shared } = defineMutators(schema);
/** The schema-typed `tx` a mutator body runs against — for helpers a body hands its `tx` to. */
type Tx = IsoTxOf<typeof schema>;

/** One schema-bound query builder, shared by every co-located `*.queries.ts`. Each `q.<table>`
 *  access mints a fresh builder, so sharing the single instance is safe. */
export const q = newQueryBuilder(schema);

// --------------------------------------------------------------- row types (schema-derived)

export type User = Row<typeof user>;
export type Issue = Row<typeof issue>;
export type Tag = Row<typeof tag>;
export type Comment = Row<typeof comment>;

// --------------------------------------------------------------- relationships (joins, declared once)
//
// Each join between two tables, declared ONCE as a typed value (FRAGMENT-COMPOSITION-DESIGN §4): the
// correlation (`parent.col → child.col`) lives here, not restated at every `sub`/`countAs`/`exists`.
// The co-located fragments import these to spread an edge, and `termCondition` below reuses the SAME
// values for the faceted search (the owner/tag/comment filters are `exists` over these very joins).
export const rels = defineRelationships({
  issueOwner: rel(issue, user, { ownerId: "id" }),
  issueTags: rel(issue, tag, { id: "issueId" }),
  issueComments: rel(issue, comment, { id: "issueId" }),
  commentAuthor: rel(comment, user, { authorId: "id" }),
  commentIssue: rel(comment, issue, { issueId: "id" }),
});

// Both the per-component SELECTIONS (fragments) and the named root queries that compose them are
// co-located with their components in `src/components/*.queries.ts`. See those modules for the
// projection/masking story.

// --------------------------------------------------------------- query knobs + the filter contract

/** How many rows the window grows by per "load more" / scroll. */
export const PAGE_SIZE = 50;

/** The growing window is bounded: a client can't ask the authority to materialize an unbounded
 *  result, and the UI stops ratcheting the limit here. Large enough to scroll the whole seeded
 *  corpus. */
export const MAX_ISSUES = 5000;

type IssueCols = (typeof issue)[typeof SCHEMA]["columns"];
type IssueQuery<R, O extends boolean> = Query<IssueCols, R, O>;

/** How many comments the activity feed shows (newest-first across the whole `comment` table). */
export const FEED_LIMIT = 30;

// --------------------------------------------------------------- server-pushed faceted filter

/** The bare search axes (the labels + parsing helpers live UI-side in `src/lib/search.ts`). */
export const FILTER_AXES = ["title", "status", "priority", "owner", "tag", "comment"] as const;
export type SearchAxis = (typeof FILTER_AXES)[number];

/** One canonical `axis:value` facet. Unlike the UI token it carries NO `id` — the filter is part
 *  of the named query's identity (its args + AST), so two equal filters must serialize identically
 *  to share one subscription. */
export interface IssueFilterTerm {
  axis: SearchAxis;
  value: string;
}
export type IssueFilter = readonly IssueFilterTerm[];

/** `col ILIKE %value%` (case-insensitive substring) over row-`R`'s column `col`, branded to that
 *  row so it can only land on a matching `.where()`. The value is a literal, so a stray `%`/`_`
 *  reads as a wildcard — fine for a demo search box. */
const contains = <R>(col: keyof R & string, value: string): Cond<R> =>
  fieldCondition(col, ilike(`%${value}%`));

/** Translate ONE facet into a WHERE condition over the issue. The relationship axes (owner / tag /
 *  comment) become correlated `EXISTS` subqueries so they filter against the WHOLE child table, not
 *  the handful of rows folded into the window. */
function termCondition(term: IssueFilterTerm): Cond<Issue> {
  const v = term.value;
  switch (term.axis) {
    case "title":
      return contains<Issue>("title", v);
    case "status":
      return fieldCondition("status", eq(v));
    case "priority":
      return fieldCondition("priority", eq(v));
    case "owner":
      return exists(rels.issueOwner, (u) => u.where(or(contains<User>("id", v), contains<User>("name", v))));
    case "tag":
      return exists(rels.issueTags, (t) => t.where(contains<Tag>("name", v)));
    case "comment":
      return exists(rels.issueComments, (c) => c.where(contains<Comment>("body", v)));
  }
}

/** AND every facet onto the issue query (an issue matches the search when it matches every term).
 *  Exported so `IssueListItem.queries.ts` can apply it while composing `issuesPageQuery` (which lives
 *  there, next to the `IssueCardFragment` it composes); its validator checks the facet list first. */
export function applyIssueFilter<R, O extends boolean>(qi: IssueQuery<R, O>, filter: IssueFilter): IssueQuery<R, O> {
  let out = qi;
  for (const term of filter) out = out.where(termCondition(term));
  return out;
}

// --------------------------------------------------------------------------- normalization

export function normalizeTitle(title: string): string {
  return title.trim().replace(/\s+/g, " ").slice(0, 180);
}

export function normalizeOwner(owner: string): string {
  return owner.trim().replace(/\s+/g, "-").slice(0, 40) || "unassigned";
}

export function normalizeTagName(raw: string): string {
  return raw.trim().replace(/^#/, "").replace(/\s+/g, "-").toLowerCase().slice(0, 28);
}

const tagInputSchema = z.object({ id: z.string(), name: z.string() });
export type TagInput = z.infer<typeof tagInputSchema>;

/** Normalize a set of tag applications: clean each name, drop blanks + duplicate names (first id
 *  wins), cap at 8. Ids are passed in (deterministic, replayable) rather than minted here. */
export function normalizeTagSet(tags: readonly TagInput[]): TagInput[] {
  const seen = new Set<string>();
  const out: TagInput[] = [];
  for (const raw of tags) {
    const name = normalizeTagName(raw.name);
    if (!name || seen.has(name)) continue;
    seen.add(name);
    out.push({ id: raw.id, name });
    if (out.length === 8) break;
  }
  return out;
}

export function normalizeCommentBody(body: string): string {
  return body.trim().slice(0, 4000);
}

// --------------------------------------------------------------------------- mutator args
//
// One zod schema per mutator. The SERVER parses the UNTRUSTED wire args through it (server/app-api.ts);
// BOTH tiers derive the arg TYPE from it (`z.infer`), so each shape is written exactly once. The client
// trusts its typed callsites and skips the parse (the predicted mutators run on the optimistic hot
// path). `status`/`priority` are validated as enums, so a bad value is rejected at the boundary rather
// than silently coerced.

// NB: the AUTHOR is NOT an arg — it is the acting principal (`ctx.user`), injected by each tier's
// driver (the client's local user for the prediction; the server's AUTHENTICATED user for the
// authoritative run). A shared mutator never trusts a client-supplied author.
export const createIssueArgs = z.object({
  id: z.string(),
  title: z.string(),
  status: z.enum(ISSUE_STATUSES),
  priority: z.enum(ISSUE_PRIORITIES),
  owner: z.string(),
  tags: z.array(tagInputSchema),
  description: z.string(),
  descriptionCommentId: z.string(),
  createdAt: z.number(),
});
export type CreateIssueArgs = z.infer<typeof createIssueArgs>;

export const issueIdArgs = z.object({ id: z.string() });
export type IssueIdArgs = z.infer<typeof issueIdArgs>;

export const editTitleArgs = z.object({ id: z.string(), title: z.string(), updatedAt: z.number() });
export type EditTitleArgs = z.infer<typeof editTitleArgs>;

export const setStatusArgs = z.object({ id: z.string(), status: z.enum(ISSUE_STATUSES), updatedAt: z.number() });
export type SetStatusArgs = z.infer<typeof setStatusArgs>;

export const setPriorityArgs = z.object({ id: z.string(), priority: z.enum(ISSUE_PRIORITIES), updatedAt: z.number() });
export type SetPriorityArgs = z.infer<typeof setPriorityArgs>;

export const setOwnerArgs = z.object({ id: z.string(), owner: z.string(), updatedAt: z.number() });
export type SetOwnerArgs = z.infer<typeof setOwnerArgs>;

export const addTagArgs = z.object({ id: z.string(), issueId: z.string(), name: z.string(), updatedAt: z.number() });
export type AddTagArgs = z.infer<typeof addTagArgs>;

export const removeTagArgs = z.object({ id: z.string(), issueId: z.string(), updatedAt: z.number() });
export type RemoveTagArgs = z.infer<typeof removeTagArgs>;

export const addCommentArgs = z.object({
  id: z.string(),
  issueId: z.string(),
  body: z.string(),
  createdAt: z.number(),
  updatedAt: z.number(),
});
export type AddCommentArgs = z.infer<typeof addCommentArgs>;

export const editCommentArgs = z.object({
  id: z.string(),
  issueId: z.string(),
  body: z.string(),
  updatedAt: z.number(),
});
export type EditCommentArgs = z.infer<typeof editCommentArgs>;

// --------------------------------------------------------------------------- mutators (ISOMORPHIC)
//
// ONE body per mutator, shared by both tiers (MUTATORS-ISOMORPHIC). Each is a GENERATOR: it `yield`s
// logical ops (`yield tx.insert(...)`) instead of being sync or async, so the SAME function runs
// synchronously against the browser's wasm engine (the optimistic prediction) AND asynchronously
// against the authoritative HCTree transaction on the server. The client registers these verbatim
// (below); the server imports them and wraps each with `runSharedMutation` after parsing untrusted
// args + injecting its authenticated principal (server/app-api.ts).
//
// Determinism/replayability is unchanged: no clock, no randomness (ids + timestamps arrive in args),
// so the client can RE-INVOKE on every rebase. The AUTHOR is `ctx.user` (the acting principal each
// tier injects), never a client-supplied arg.

/** Insert a user row if it isn't already present — the isomorphic `insertIgnore` (renders
 *  `ON CONFLICT DO NOTHING` server-side). A single op, so it just RETURNS one and is plain-`yield`ed
 *  (`yield ensureUser(...)`) — no generator, no `yield*`. Then a forgotten `yield` leaves an obvious
 *  dead statement (and is lint-catchable), where a forgotten `yield*` on a generator is a SILENT
 *  no-op. Multi-op / reading helpers stay generators + `yield*`. */
const ensureUser = (tx: Tx, id: string): MutationOp => tx.insertIgnore("user", { id, name: id });

/** PREDICTED/AUTHORITATIVE mutators — one shared body each, co-located with its arg schema via
 *  `shared(argsSchema, gen)`. The pairing lives at ONE site: the client registers the value verbatim
 *  and ignores `.args` (typed callsites), while the server parses untrusted wire args through that
 *  SAME `.args` before driving the body (`sharedApiMutators`). Each writes across as many of the four
 *  tables as the change touches. */
export const mutators = {
  createIssue: shared(createIssueArgs, function* (tx, a: CreateIssueArgs, ctx: MutatorCtx): MutationGen {
    const ownerId = normalizeOwner(a.owner);
    const authorId = normalizeOwner(ctx.user);
    yield ensureUser(tx, ownerId);
    yield ensureUser(tx, authorId);
    yield tx.insert("issue", {
      id: a.id,
      title: normalizeTitle(a.title),
      status: a.status,
      priority: a.priority,
      ownerId,
      createdAt: a.createdAt,
      updatedAt: a.createdAt,
    });
    yield tx.insert("comment", {
      id: a.descriptionCommentId,
      issueId: a.id,
      authorId,
      body: normalizeCommentBody(a.description),
      createdAt: a.createdAt,
    });
    for (const t of normalizeTagSet(a.tags)) {
      yield tx.insert("tag", { id: t.id, issueId: a.id, name: t.name });
    }
  }),
  editTitle: shared(editTitleArgs, function* (tx, a: EditTitleArgs): MutationGen {
    yield tx.update("issue", { id: a.id, title: normalizeTitle(a.title), updatedAt: a.updatedAt });
  }),
  setStatus: shared(setStatusArgs, function* (tx, a: SetStatusArgs): MutationGen {
    yield tx.update("issue", { id: a.id, status: a.status, updatedAt: a.updatedAt });
  }),
  setPriority: shared(setPriorityArgs, function* (tx, a: SetPriorityArgs): MutationGen {
    yield tx.update("issue", { id: a.id, priority: a.priority, updatedAt: a.updatedAt });
  }),
  setOwner: shared(setOwnerArgs, function* (tx, a: SetOwnerArgs): MutationGen {
    const ownerId = normalizeOwner(a.owner);
    yield ensureUser(tx, ownerId);
    yield tx.update("issue", { id: a.id, ownerId, updatedAt: a.updatedAt });
  }),
  addTag: shared(addTagArgs, function* (tx, a: AddTagArgs): MutationGen {
    const name = normalizeTagName(a.name);
    if (!name) return;
    yield tx.insert("tag", { id: a.id, issueId: a.issueId, name });
    yield tx.update("issue", { id: a.issueId, updatedAt: a.updatedAt });
  }),
  removeTag: shared(removeTagArgs, function* (tx, a: RemoveTagArgs): MutationGen {
    yield tx.delete("tag", { id: a.id });
    yield tx.update("issue", { id: a.issueId, updatedAt: a.updatedAt });
  }),
  addComment: shared(addCommentArgs, function* (tx, a: AddCommentArgs, ctx: MutatorCtx): MutationGen {
    const body = normalizeCommentBody(a.body);
    if (!body) return;
    const authorId = normalizeOwner(ctx.user);
    yield ensureUser(tx, authorId);
    yield tx.insert("comment", { id: a.id, issueId: a.issueId, authorId, body, createdAt: a.createdAt });
    yield tx.update("issue", { id: a.issueId, updatedAt: a.updatedAt });
  }),
  editComment: shared(editCommentArgs, function* (tx, a: EditCommentArgs): MutationGen {
    yield tx.update("comment", { id: a.id, body: normalizeCommentBody(a.body) });
    yield tx.update("issue", { id: a.issueId, updatedAt: a.updatedAt });
  }),
  // The owner check is enforced authoritatively (server/app-api.ts overrides this with an owner-gated
  // cascade); the client predicts the removal and snaps back if the authority kept the row. The
  // orphaned child rows fall out of every window with the issue, so per-query refcounts reclaim them.
  deleteIssue: shared(issueIdArgs, function* (tx, a: IssueIdArgs): MutationGen {
    yield tx.delete("issue", { id: a.id });
  }),
} satisfies ClientRegistry;
