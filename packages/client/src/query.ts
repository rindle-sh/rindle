// The fluent, type-safe query builder. It accumulates state immutably and compiles to the
// Zero-wire {@link Ast} via `.ast()` (which the wasm `Db.query`/a remote server parses).
//
// Two runtime Proxies give the requested ergonomics (WASM-CLIENT-DESIGN.md §6):
//   - `where` is callable (`where(or(...))`) AND a field proxy (`where.closed(false)`);
//   - `where<Field>(…)` camelCase sugar (`whereClosed(false)`) is intercepted too.
// Both are fully typed via mapped types + template-literal keys.

import type { Ast, Bound, Condition, CorrelatedSubquery, Dir, ExistsOp, LitValue, OrderPart, SimpleOp } from "./ast.ts";
import { stableKey } from "./key.ts";
import type { Arg, Cond } from "./operators.ts";
import { fieldCondition } from "./operators.ts";
import type { AnyCols, AnyRelationship, AnyTable, ColsMap, ColT, Relationship, RowOf, Schema, TableLike, TableMeta } from "./schema.ts";
import { isLocalTable, isRelationship, SCHEMA } from "./schema.ts";
import type { ArrayView, SingularArrayView } from "./view.ts";

// ----------------------------- the public Query type -----------------------------

// `One` tracks a top-level `.one()`: it flips `materialize()` from a plural `ArrayView`
// (`data: R[]`) to a `SingularArrayView` (`data: R | null`). It threads through every chained
// method so the unwrap survives `.one().where(…)`, etc. Defaults `false` (plural).

type FieldFn<C extends AnyCols, K extends keyof C, Rels, One extends boolean, Sel extends string, LocalRels> = (
  arg: Arg<ColT<C[K]>>,
) => Query<C, Rels, One, Sel, LocalRels>;

/** `where.closed(false)`, `where.priority(gt(3))`. */
type WhereProxy<C extends AnyCols, Rels, One extends boolean, Sel extends string, LocalRels> = {
  [K in keyof C]: FieldFn<C, K, Rels, One, Sel, LocalRels>;
};

/** `whereClosed(false)`, `wherePriority(gt(3))`. */
type WhereSugar<C extends AnyCols, Rels, One extends boolean, Sel extends string, LocalRels> = {
  [K in keyof C as `where${Capitalize<string & K>}`]: FieldFn<C, K, Rels, One, Sel, LocalRels>;
};

/** What `materialize()` returns: singular (`R | null`) for a top-level `.one()`, else plural. */
type MaterializedView<R, One extends boolean> = One extends true ? SingularArrayView<R> : ArrayView<R>;

/**
 * The result row type of a projection (masking, FRAGMENT-COMPOSITION-DESIGN.md §6 / §10.6).
 * `Sel` is the union of columns named via `select(...)`. With nothing selected (`Sel = never`)
 * it stays the full {@link RowOf} — the "no `select` ⇒ all columns" convention, kept and made
 * type-honest. Once any column is selected, the row is **masked** to exactly those columns, so a
 * component (its fragment) can read only what it declared. `Pick` only ever *removes* fields, so
 * narrowing is always sound versus a runtime row that may still carry more.
 */
type Projected<C extends AnyCols, Sel extends string> = [Sel] extends [never]
  ? RowOf<C>
  : Pick<RowOf<C>, Sel & keyof C>;

/**
 * The accumulating result row of a top-level aggregate (REDUCE-DESIGN.md §8). `Agg` is `false`
 * until {@link QueryBase.count}/{@link QueryBase.groupBy} reshapes the query; then it is the
 * group-by columns intersected with the synthetic `{ count: number }`. {@link AggAcc} treats the
 * `false` state as `{}` so the intersections in `count`/`groupBy` compose either way.
 */
type AggAcc<Agg> = [Agg] extends [false] ? {} : Agg;

/** `having`'s field binder: one accessor per aggregate-OUTPUT column (the group-by columns plus the
 *  synthetic numeric `count`), each producing a {@link Cond} over that aggregate row. It is the only
 *  way to name `count` in a predicate (it lives on no base table); compose several with `and`/`or`. */
type HavingProxy<Row> = { [K in keyof Row & string]: (arg: Arg<Row[K]>) => Cond<Row> };

/** The `countAs` relationship aliases of a query — the {@link Rels} keys whose value is the scalar
 *  `number` a count aggregate surfaces. These are the only aliases {@link QueryBase.having}'s
 *  parent-by-child-aggregate overload accepts (a plain `sub` alias carries a row object, not a
 *  number, so it is rejected). */
type AggregateAlias<Rels> = { [K in keyof Rels]: Rels[K] extends number ? K & string : never }[keyof Rels];

/** What a query materializes: the post-aggregation row once reshaped by {@link QueryBase.count}
 *  (`Agg`), else the projected base row plus its relationship values. */
type ResultRow<C extends AnyCols, Rels, Sel extends string, Agg> = [Agg] extends [false]
  ? Projected<C, Sel> & Rels
  : Agg;

type FragmentEdge<C extends AnyCols, Rels, Sel extends string, LocalRels> = (
  q: Query<C, Rels, false, Sel, LocalRels>,
) => Query<C, Rels, boolean, Sel, LocalRels>;

interface QueryBase<C extends AnyCols, Rels, One extends boolean, Sel extends string, LocalRels, Agg = false> {
  /** Present when this local query came from `defineQuery`; used as the remote identity. */
  readonly name?: string;
  /** Present when this local query came from `defineQuery`; sent with `name` upstream. */
  readonly args?: unknown;
  /** Present when this local query came from a realtime-labeled `defineQuery`
   *  (RINDLE-REALTIME-QUERY-ENABLEMENT §2.1). Declaration metadata only — it never joins the
   *  wire identity and never changes the AST. */
  readonly realtime?: RealtimeQueryLabel;
  /** Condition form — consumes `or()`/`and()`/`exists()`/field conditions (AND-ed across calls).
   *  Filters over the FULL column set (`RowOf<C>`), independent of what's `select`ed/masked. */
  where(cond: Cond<RowOf<C>>): Query<C, Rels, One, Sel, LocalRels>;
  orderBy<K extends keyof C>(col: K, dir: Dir): Query<C, Rels, One, Sel, LocalRels>;
  /** Project a subset of columns (PROJECTION-SUPPORT-DESIGN.md §6, masking §6/§10.6). Chainable
   *  (each call unions into `Sel`); omit to select all. Drives what the server syncs, what the
   *  view reports, AND — once any column is named — narrows the result row TYPE to exactly the
   *  selection (masking). `where`/`orderBy`/`start`/`sub` still see the full column set. */
  select<K extends keyof C & string>(...cols: K[]): Query<C, Rels, One, Sel | K, LocalRels>;
  limit(n: number): Query<C, Rels, One, Sel, LocalRels>;
  /** Cursor paging: start at (or, with `exclusive`, after) the partial `cursor` row over the
   *  sort columns. Lowers to a `Skip` in the engine. */
  start(cursor: Partial<RowOf<C>>, opts?: { exclusive?: boolean }): Query<C, Rels, One, Sel, LocalRels>;
  /** Return a single row: `materialize()` yields a {@link SingularArrayView} (`data: R | null`)
   *  and the engine caps the query to `limit = 1`. */
  one(): Query<C, Rels, true, Sel, LocalRels>;
  /** **Merge** a {@link Fragment} into THIS node — Relay's "spread on the same type" (§5). The
   *  fragment must be over the same table (enforced in the result type: a fragment over a different
   *  table yields `never`, plus a runtime throw). Unions the projection (`Sel | FSel`), folds in the
   *  fragment's nested relationships (`Rels & FRels`), and merges any same-`alias` edge recursively
   *  — canonically, so `include` order doesn't matter (§10.5). Two fragments that spread the same
   *  alias with a conflicting row-set (correlation/where/orderBy/…) **throw** (§10.2); an included
   *  fragment may **not** add a root `where`/`orderBy`/`limit` (§10.3). This is also the single-shot
   *  way to ROOT a fragment onto a base query (`queries.issue.where.id(x).include(IssueCard)`) — the
   *  loader's composed root — equivalent to applying the fragment, but canonicalized.
   *
   *  The fragment's columns are a *separate* inferred parameter `FC` (not the class `C`) so that
   *  `include` keeps `C` out of any contravariant position — preserving `Query<Concrete>`'s
   *  assignability to `Query<AnyCols>` (needed by `Store<ColsMap>`/`QueryRoot`). Same-table is then
   *  checked as the mutual-assignability of `C` and `FC` in the return type. */
  include<FC extends AnyCols, FRels = {}, FSel extends string = never, FLocalRels = FRels>(
    fragment: Fragment<FC, FRels, FSel, FLocalRels>,
  ): [C] extends [FC] ? ([FC] extends [C] ? Query<C, Rels & FRels, One, Sel | FSel, LocalRels & FLocalRels> : never) : never;
  /** Nest a child fragment as an opaque local-read ref. The coverage query still composes the
   *  child's full AST at runtime; the React-facing fragment data exposes only refs at this
   *  boundary so the child component owns its local subscription. */
  sub<
    A extends string,
    CC extends AnyCols,
    CRels = {},
    CSel extends string = never,
    CLocalRels = CRels,
    F extends Fragment<CC, CRels, CSel, CLocalRels> = Fragment<CC, CRels, CSel, CLocalRels>,
  >(
    alias: A,
    child: TableLike<CC>,
    corr: { parent: Array<keyof C & string>; child: Array<keyof CC & string> },
    build: F,
    edge: FragmentEdge<CC, CRels, CSel, CLocalRels>,
  ): Query<
    C,
    Rels & { [P in A]: Array<Projected<CC, CSel> & CRels> },
    One,
    Sel,
    LocalRels & { [P in A]: Array<FragmentRef<F>> }
  >;
  sub<
    A extends string,
    CC extends AnyCols,
    CRels = {},
    CSel extends string = never,
    CLocalRels = CRels,
    F extends Fragment<CC, CRels, CSel, CLocalRels> = Fragment<CC, CRels, CSel, CLocalRels>,
  >(
    alias: A,
    child: TableLike<CC>,
    corr: { parent: Array<keyof C & string>; child: Array<keyof CC & string> },
    build: F,
  ): Query<
    C,
    Rels & { [P in A]: Array<Projected<CC, CSel> & CRels> },
    One,
    Sel,
    LocalRels & { [P in A]: Array<FragmentRef<F>> }
  >;
  /** Relationship-value form of the fragment-ref overload above. */
  sub<
    A extends string,
    CC extends AnyCols,
    CRels = {},
    CSel extends string = never,
    CLocalRels = CRels,
    F extends Fragment<CC, CRels, CSel, CLocalRels> = Fragment<CC, CRels, CSel, CLocalRels>,
  >(
    alias: A,
    relationship: Relationship<C, CC>,
    build: F,
    edge: FragmentEdge<CC, CRels, CSel, CLocalRels>,
  ): Query<
    C,
    Rels & { [P in A]: Array<Projected<CC, CSel> & CRels> },
    One,
    Sel,
    LocalRels & { [P in A]: Array<FragmentRef<F>> }
  >;
  /** Relationship-value form of the fragment-ref overload above. */
  sub<
    A extends string,
    CC extends AnyCols,
    CRels = {},
    CSel extends string = never,
    CLocalRels = CRels,
    F extends Fragment<CC, CRels, CSel, CLocalRels> = Fragment<CC, CRels, CSel, CLocalRels>,
  >(
    alias: A,
    relationship: Relationship<C, CC>,
    build: F,
  ): Query<
    C,
    Rels & { [P in A]: Array<Projected<CC, CSel> & CRels> },
    One,
    Sel,
    LocalRels & { [P in A]: Array<FragmentRef<F>> }
  >;
  /** Nest a child by EXPLICIT correlation (no schema relationship). `alias` is the result key.
   *  The child's own projection (`CSel`) masks the nested row, so a fragment spread here
   *  contributes exactly the columns it declared. */
  sub<A extends string, CC extends AnyCols, CRels = {}, CSel extends string = never>(
    alias: A,
    child: TableLike<CC>,
    corr: { parent: Array<keyof C & string>; child: Array<keyof CC & string> },
    build?: (q: Query<CC>) => Query<CC, CRels, boolean, CSel>,
  ): Query<
    C,
    Rels & { [P in A]: Array<Projected<CC, CSel> & CRels> },
    One,
    Sel,
    LocalRels & { [P in A]: Array<Projected<CC, CSel> & CRels> }
  >;
  /** Nest a child by a named {@link Relationship} (`rel(parent, child, {...})`) — the correlation comes
   *  from the relationship, so no `{ parent, child }` keys are restated. The relationship must belong to
   *  THIS table (its parent columns are checked against `C`). `alias` is still the result key. */
  sub<A extends string, CC extends AnyCols, CRels = {}, CSel extends string = never>(
    alias: A,
    relationship: Relationship<C, CC>,
    build?: (q: Query<CC>) => Query<CC, CRels, boolean, CSel>,
  ): Query<
    C,
    Rels & { [P in A]: Array<Projected<CC, CSel> & CRels> },
    One,
    Sel,
    LocalRels & { [P in A]: Array<Projected<CC, CSel> & CRels> }
  >;
  /** Add a **relationship aggregate** — `issue.countAs("commentCount", comment, …)`
   *  (`REDUCE-DESIGN.md` §9). Like {@link sub} (explicit correlation, optional child
   *  `build` for a filtered `count(child WHERE …)`), but the relationship surfaces a single
   *  scalar `count(*)` of the correlated child rows named `alias` — so the result key is a
   *  `number`, not an array; an empty (childless) parent reads `0`. */
  countAs<A extends string, CC extends AnyCols>(
    alias: A,
    child: TableLike<CC>,
    corr: { parent: Array<keyof C & string>; child: Array<keyof CC & string> },
    build?: (q: Query<CC>) => Query<CC, unknown>,
  ): Query<C, Rels & { [P in A]: number }, One, Sel, LocalRels & { [P in A]: number }>;
  /** `countAs` by a named {@link Relationship} — like the explicit form, but the correlation comes from
   *  `rel(...)` instead of being restated. */
  countAs<A extends string, CC extends AnyCols>(
    alias: A,
    relationship: Relationship<C, CC>,
    build?: (q: Query<CC>) => Query<CC, unknown>,
  ): Query<C, Rels & { [P in A]: number }, One, Sel, LocalRels & { [P in A]: number }>;
  /** Reshape this query into a top-level `count(*)` aggregate (`REDUCE-DESIGN.md` §8) — the SQL
   *  `SELECT count(*) FROM table [GROUP BY …] [HAVING …]`. Without {@link groupBy} it is a GLOBAL
   *  count (one `{ count }` row, value `0` even on empty input); with it, one `{ …group, count }`
   *  row per distinct group. The result row becomes the aggregate's OUTPUT — the group-by columns
   *  plus a numeric `count` — so chain {@link having} to filter it. Distinct from {@link countAs}
   *  (a child-relationship scalar attached to the parent row); this reshapes the query ITSELF. The
   *  engine rejects pairing a root aggregate with `select`/`sub`/`countAs`/`orderBy`/`limit`/`one`. */
  count(): Query<C, Rels, One, Sel, LocalRels, AggAcc<Agg> & { count: number }>;
  /** Add a top-level `GROUP BY` column (chain for a compound key); only meaningful with
   *  {@link count}. Each grouped column joins the aggregate result row, keyed + sorted by the
   *  group key. */
  groupBy<K extends keyof C & string>(
    col: K,
  ): Query<C, Rels, One, Sel, LocalRels, AggAcc<Agg> & Pick<RowOf<C>, K>>;
  /** `HAVING (…)` — filter the **post-aggregation** rows of a {@link count} query (`REDUCE-DESIGN.md`
   *  §4: a filter directly above the reduce). `build` receives a field binder over the aggregate's
   *  OUTPUT columns — the {@link groupBy} columns and the synthetic `count` — and returns a
   *  {@link Cond}; compose several with `and`/`or`. E.g.
   *  `.groupBy("status").count().having((h) => h.count(gt(3)))`. Distinct from {@link where}, which
   *  filters base rows BELOW the reduce. */
  having(build: (h: HavingProxy<AggAcc<Agg>>) => Cond<AggAcc<Agg>>): Query<C, Rels, One, Sel, LocalRels, Agg>;
  /** `HAVING count(child) <op> n` — filter THIS parent by a child relationship aggregate's count
   *  (`PARENT-AGGREGATE-FILTER-DESIGN.md`). `alias` must name a {@link countAs} relationship already
   *  on this query; this drops parents whose child count fails `<op> val`, maintained incrementally
   *  (a child add/remove crossing the threshold adds/removes the parent). The display `countAs` is
   *  untouched — a survivor still shows its real count. Distinct from the {@link having} overload
   *  above, which filters a top-level {@link count}'s own output rows.
   *
   *  **v1: high-pass predicates only** — predicates *false* at count 0 (`>`, `>=`/`=`/`!=` for
   *  `n ≥ 1`). A childless parent forms no group, so the engine rejects (at build) a predicate *true*
   *  at count 0 (`<=`, `< n` for `n ≥ 1`, `= 0`, `>= 0`); those need row-widening (deferred). */
  having<A extends AggregateAlias<Rels>>(
    alias: A,
    op: SimpleOp,
    val: number,
  ): Query<C, Rels, One, Sel, LocalRels, Agg>;
  /** The compiled Zero-wire AST (what a backend's `query` consumes). */
  ast(): Ast;
  /** Materialize into a live, typed view. Wired by the Store/backend. A top-level `.one()`
   *  yields a {@link SingularArrayView}; otherwise an {@link ArrayView}. The row is the aggregate
   *  output once {@link count} reshaped the query, else masked to the projection ({@link Projected})
   *  — full {@link RowOf} until a column is `select`ed. */
  materialize(): MaterializedView<ResultRow<C, Rels, Sel, Agg>, One>;
}

export type Query<
  C extends AnyCols,
  Rels = {},
  One extends boolean = false,
  Sel extends string = never,
  LocalRels = Rels,
  Agg = false,
> =
  & Omit<QueryBase<C, Rels, One, Sel, LocalRels, Agg>, "where">
  & { where: QueryBase<C, Rels, One, Sel, LocalRels, Agg>["where"] & WhereProxy<C, Rels, One, Sel, LocalRels> }
  & WhereSugar<C, Rels, One, Sel, LocalRels>;

export type AnyQuery = Query<any, any, any, any, any, any>;

export type QueryLocalData<Q extends AnyQuery> =
  Q extends Query<infer C, any, infer One, infer Sel, infer LocalRels, infer Agg>
    ? [Agg] extends [false]
      ? One extends true
        ? (Projected<C, Sel> & LocalRels) | null
        : readonly (Projected<C, Sel> & LocalRels)[]
      : readonly Agg[]
    : never;
const STAMP_NAMED_QUERY: unique symbol = Symbol("rindle.stampNamedQuery");

interface QueryInternals {
  [STAMP_NAMED_QUERY](name: string, args: unknown, realtime?: RealtimeQueryLabel): unknown;
}

function stampNamedQuery<Q extends AnyQuery>(
  query: Q,
  name: string,
  args: unknown,
  realtime?: RealtimeQueryLabel,
): Q {
  const stamp = (query as unknown as QueryInternals)[STAMP_NAMED_QUERY];
  if (typeof stamp !== "function") {
    throw new Error("defineQuery's build must return a Query built by newQueryBuilder/queries");
  }
  return stamp(name, args, realtime) as Q;
}

/** Turn raw, UNTRUSTED wire args into the canonical, typed args a query is built from. Its return
 *  type IS the query's args type — written once, it flows to both the client call signature and the
 *  `build` step, so there's no second place to restate the shape. */
export type QueryValidator<Args> = (rawArgs: unknown) => Args;
/**
 * Build a `Query` (an `Ast` constructor) from already-validated args, plus an optional CONTEXT — the
 * authenticated principal the query is scoped to. `Ctx` is **off-wire**: the client passes its own
 * session ctx at the callsite, the server injects the AUTHORITATIVE ctx ({@link NamedQuery.resolve}
 * via `registerQueries`), and the wire still carries only `name` + args. Both tiers run the same
 * `build`, so whenever their ctx agrees the AST is byte-identical — and a client can never spoof it,
 * since the server re-derives ctx from its trusted session and ignores anything the client claimed.
 *
 * The ctx is a *tuple* so a query opts into it by how it types `build`'s second parameter — and that
 * shape becomes the call signature verbatim:
 *   - `(args) => Q` — no ctx (`q(args)`).
 *   - `(args, ctx: C) => Q` — ctx REQUIRED (`q(args, ctx)`); for always-authenticated queries.
 *   - `(args, ctx?: C) => Q` — ctx OPTIONAL (`q(args)` or `q(args, ctx)`); sound only when "no ctx"
 *     is a real, SYMMETRIC state — i.e. the build yields the SAME broad AST whether ctx is absent or
 *     present-with-no-user (anonymous / SSR), since the server always forwards a ctx (possibly
 *     anonymous). Otherwise type ctx required so the type catches an omitted ctx.
 */
export type QueryBuilder<Args, Q extends AnyQuery, Ctx extends readonly unknown[] = []> = (
  args: Args,
  ...ctx: Ctx
) => Q;

/**
 * The realtime LABEL a named query may declare (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §2.1):
 * which api-server room PROFILE the query wants to be served from, and how the query's args map
 * to that profile's key args. This is the DECLARATION only — the serve decision (covering proof,
 * lease routing) is the server's, and the final room key is minted server-side by the profile's
 * own `key` under authoritative ctx, so a label can never place a query in a room the server
 * didn't derive itself. Both tiers import the SAME `defineQuery` value, so they always agree
 * *which profile* a query belongs to.
 */
export interface RealtimeQueryLabel<Args = any> {
  /** The room profile name — must match a `realtime.rooms` key on the api-server (validated
   *  loudly at `createRindleApiServer` construction). May not contain `/` (the wire room-key
   *  delimiter). */
  readonly room: string;
  /** Map the query's VALIDATED args to the profile's key args (what the server feeds the
   *  profile's `key(args)`). Identity when omitted. Must be pure — both tiers may run it. */
  readonly args?: (queryArgs: Args) => unknown;
}

/** Options for {@link defineQuery}. */
export interface DefineQueryOptions<Args = any> {
  /** Declare this query realtime-eligible — see {@link RealtimeQueryLabel}. */
  realtime?: RealtimeQueryLabel<Args>;
}

/** Loud, definition-time validation of a realtime label — a malformed label is a config bug the
 *  author should hit at module load, not a query that silently never room-serves. */
function validateRealtimeLabel(
  name: string,
  label: RealtimeQueryLabel<any> | undefined,
): RealtimeQueryLabel<any> | undefined {
  if (label === undefined) return undefined;
  if (typeof label.room !== "string" || label.room.length === 0) {
    throw new Error(`defineQuery("${name}"): realtime.room must be a non-empty room-profile name.`);
  }
  if (label.room.includes("/")) {
    throw new Error(
      `defineQuery("${name}"): realtime.room "${label.room}" may not contain "/" — it delimits the ` +
        `wire room key ("<profile>/<key>").`,
    );
  }
  if (label.args !== undefined && typeof label.args !== "function") {
    throw new Error(
      `defineQuery("${name}"): realtime.args must be a function mapping the query's args to the ` +
        `profile's key args (or omitted for identity).`,
    );
  }
  return label;
}

/**
 * A single, co-located NAMED query (see {@link defineQuery}). It is:
 *   - **callable on the client** — `q(args, ctx?)` validates + builds + stamps the result with its
 *     remote subscription identity (`name` + args — NOT ctx), so a component that imports it always
 *     SYNCS. There is no unstamped builder to import by accident.
 *   - **registerable on the server** — it carries its `queryName` and a `resolve` that re-runs the
 *     SAME validator on untrusted wire args and builds the AUTHORITATIVE `Query` from the server's
 *     own ctx ({@link registerQueries `registerQueries`} in `@rindle/api-server`).
 *
 * `Ctx` is the ctx parameter list mirrored from `build` — `[]`, `[ctx: C]`, or `[ctx?: C]`. It is
 * never part of the wire identity.
 */
export interface NamedQuery<Args, Ctx extends readonly unknown[], Q extends AnyQuery> {
  (args: Args, ...ctx: Ctx): Q;
  /** The wire identity. The daemon leases by this name; client and server MUST agree on it — which
   *  they do, because both sides import the SAME `defineQuery` value. */
  readonly queryName: string;
  /** The §2.1 realtime label, when this query was defined with one ({@link DefineQueryOptions}).
   *  Absent on unlabeled queries. Carried onto the stamped built Query too, and preserved through
   *  `registerQueries` on the server. */
  readonly realtime?: RealtimeQueryLabel<Args>;
  /** Server-side: validate raw wire args, then build the authoritative `Query` from the server's
   *  authoritative ctx (forwarded by `registerQueries`). */
  resolve(rawArgs: unknown, ...ctx: Ctx): Q;
}

/**
 * Define ONE co-located, named query. Keep it next to the component that reads it (a `*.queries.ts`
 * file), so a query lives where its fragments and its component live.
 *
 * The returned value is callable on the client (it stamps its result with `name` + args, so the
 * subscription SYNCS) and registerable on the server ({@link registerQueries}). The optional
 * `validate` step turns untrusted wire args into the canonical args type — and that type flows to
 * both the call signature and `build`, so the shape is written exactly once. `validate` runs on
 * BOTH tiers (it builds the byte-identical authoritative AST on the server, and guards + guarantees
 * the same AST on the client). If the server must DIVERGE from the client, define a second
 * `defineQuery` with the same `name` for the server and register that one instead.
 *
 * `build` may take a second CONTEXT parameter (see {@link QueryBuilder}). Context is off-wire: the
 * client passes its session ctx at the callsite, the server injects its authoritative ctx — so a
 * per-user query stays symmetric without ever trusting (or transmitting) a client-supplied identity.
 *
 * ```ts
 * // recentComments({ limit }) — validated, no ctx, byte-identical on both tiers
 * export const recentCommentsQuery = defineQuery(
 *   "recentComments",
 *   (raw): { limit: number } => ({ limit: validateFeedLimit(raw) }),
 *   ({ limit }) => q.comment.orderBy("createdAt", "desc").limit(limit).include(FeedItemFragment),
 * );
 *
 * // myIssues({ limit }, ctx) — ctx-scoped; the wire still carries only { limit }
 * export const myIssuesQuery = defineQuery(
 *   "myIssues",
 *   (raw): { limit: number } => ({ limit: validateLimit(raw) }),
 *   ({ limit }, ctx: { user: string }) => q.issue.where.ownerId(ctx.user).limit(limit),
 * );
 * // client: myIssuesQuery({ limit: 20 }, { user: currentUser() })
 * ```
 */
export function defineQuery<Q extends AnyQuery>(
  name: string,
  build: () => Q,
  options?: DefineQueryOptions<void>,
): NamedQuery<void, [], Q>;
export function defineQuery<Args, Ctx extends readonly unknown[], Q extends AnyQuery>(
  name: string,
  build: (args: Args, ...ctx: Ctx) => Q,
  options?: DefineQueryOptions<Args>,
): NamedQuery<Args, Ctx, Q>;
export function defineQuery<Args, Ctx extends readonly unknown[], Q extends AnyQuery>(
  name: string,
  validate: QueryValidator<Args>,
  build: (args: Args, ...ctx: Ctx) => Q,
  options?: DefineQueryOptions<Args>,
): NamedQuery<Args, Ctx, Q>;
export function defineQuery(
  name: string,
  validateOrBuild: (...a: any[]) => any,
  maybeBuildOrOptions?: ((...a: any[]) => any) | DefineQueryOptions<any>,
  maybeOptions?: DefineQueryOptions<any>,
): NamedQuery<any, any, AnyQuery> {
  const hasValidator = typeof maybeBuildOrOptions === "function";
  const validate = (hasValidator ? validateOrBuild : (raw: unknown) => raw) as (raw: unknown) => unknown;
  const build = (hasValidator ? maybeBuildOrOptions : validateOrBuild) as (args: unknown, ...ctx: unknown[]) => AnyQuery;
  const options = hasValidator ? maybeOptions : (maybeBuildOrOptions as DefineQueryOptions<any> | undefined);
  // §2.1 realtime label — pure declaration metadata, validated loudly at definition time.
  const realtime = validateRealtimeLabel(name, options?.realtime);
  const resolve = (rawArgs: unknown, ...ctx: unknown[]): AnyQuery => build(validate(rawArgs), ...ctx);
  // The wire identity is (name, args) ONLY — ctx is never stamped, so it never crosses the wire.
  // The realtime label rides the stamp as metadata beside the identity, never inside it.
  const call = (args: unknown, ...ctx: unknown[]): AnyQuery =>
    stampNamedQuery(build(validate(args), ...ctx), name, args ?? null, realtime);
  return Object.assign(
    call,
    realtime === undefined ? { queryName: name, resolve } : { queryName: name, resolve, realtime },
  ) as NamedQuery<any, any, AnyQuery>;
}

// ----------------------------- fragments (FRAGMENT-COMPOSITION-DESIGN.md, Phase 0) -----------------------------

const FRAGMENT_BRAND: unique symbol = Symbol("rindle.fragment");
const LOCAL_FRAGMENT_REF_BRAND: unique symbol = Symbol("rindle.localFragmentRef");
const FRAGMENT_REL_BRAND: unique symbol = Symbol("rindle.fragmentRelationship");

/**
 * A reusable, typed *selection over a table* — Relay's "fragment", promoted to a first-class
 * value. **Two verbs** compose a fragment, on one axis — does its data belong to THIS row or to a
 * related one:
 *
 *   - SAME node — `q.include(Frag)`: merge its selection into this node. This is also how you
 *     **root** a fragment onto a base query — `queries.t.where.id(x).include(Frag)`.
 *   - CHILD node — `q.sub(alias, child, corr, Frag)`: nest it under a relationship (`sub`'s 4th arg).
 *
 * A `Fragment` is therefore also a `build` transform (`(q: Query<C>) => Query<C, Rels>`); that
 * call signature is the mechanism by which `sub` accepts a fragment as its build. Prefer
 * `include` to root a fragment (canonical + merged) over calling it directly. It additionally
 * carries its `table` (for {@link FragmentRef}/`useFragment` typing and masking). Composing
 * fragments assembles ONE {@link Ast} → one materialization → one `/query` — the whole point
 * (no request waterfall; design §1).
 */
export interface Fragment<C extends AnyCols, Rels = {}, Sel extends string = never, LocalRels = Rels> {
  (q: Query<C>): Query<C, Rels, false, Sel, LocalRels>;
  readonly table: TableLike<C>;
  readonly [FRAGMENT_BRAND]: true;
}

/**
 * The data returned by `useFragment(fragment, ref)`: the fragment's own selected columns plus
 * immediate relationship values. Relationships whose builder is another fragment surface as
 * opaque {@link FragmentRef}s, so child-owned payload is read only by the child fragment reader.
 */
export type FragmentData<F> = F extends Fragment<infer C, unknown, infer Sel, infer LocalRels>
  ? Projected<C, Sel> & LocalRels
  : never;

export interface FragmentCoverage<Q extends AnyQuery = AnyQuery> {
  readonly key: string;
  readonly query: Q;
}

export interface LocalFragmentRef<F extends Fragment<any, any, any, any> = Fragment<any, any, any, any>> {
  readonly [LOCAL_FRAGMENT_REF_BRAND]: true;
  readonly source: "local";
  readonly table: string;
  readonly pk: Readonly<Record<string, LitValue>>;
  readonly coverage: FragmentCoverage;
  readonly __fragment?: F;
}

/** The opaque token a parent passes to a component that reads {@link FragmentData} for `F`. */
export type FragmentRef<F> = F extends Fragment<any, any, any, any> ? LocalFragmentRef<F> : never;

/**
 * Define a co-located, composable {@link Fragment}: a named selection over `table`. The returned
 * value is callable (the `build` transform), so it threads through the existing `sub`/`Rels`
 * spine — no GraphQL, no codegen, no schema change (design §3).
 *
 * ```ts
 * const UserAvatar = defineFragment(schema.user, (f) => f.select("id", "name", "avatarUrl"));
 * const CommentRow = defineFragment(schema.comment, (f) =>
 *   f.select("id", "body", "authorId")
 *    .sub("author", schema.user, { parent: ["authorId"], child: ["id"] }, UserAvatar));
 * ```
 */
export function defineFragment<C extends AnyCols, Rels = {}, Sel extends string = never, LocalRels = Rels>(
  table: TableLike<C>,
  build: (q: Query<C>) => Query<C, Rels, boolean, Sel, LocalRels>,
): Fragment<C, Rels, Sel, LocalRels> {
  const frag = (q: Query<C>): Query<C, Rels, false, Sel, LocalRels> => build(q) as Query<C, Rels, false, Sel, LocalRels>;
  return Object.assign(frag, { table, [FRAGMENT_BRAND]: true as const }) as unknown as Fragment<C, Rels, Sel, LocalRels>;
}

/** Runtime guard: is `v` a {@link Fragment} (a `defineFragment` value, not a plain build fn)? */
export function isFragment(v: unknown): v is Fragment<AnyCols, unknown, never, unknown> {
  return typeof v === "function" && (v as Partial<Fragment<AnyCols, unknown, never, unknown>>)[FRAGMENT_BRAND] === true;
}

function isLocalFragmentRef(v: unknown): v is LocalFragmentRef {
  return typeof v === "object" && v !== null && (v as Partial<LocalFragmentRef>)[LOCAL_FRAGMENT_REF_BRAND] === true;
}

type FragmentRelationship = CorrelatedSubquery & { [FRAGMENT_REL_BRAND]?: true };

function markFragmentRelationship<T extends CorrelatedSubquery>(rel: T): T {
  Object.defineProperty(rel, FRAGMENT_REL_BRAND, { value: true });
  return rel;
}

export function isFragmentRelationship(rel: CorrelatedSubquery): boolean {
  return (rel as FragmentRelationship)[FRAGMENT_REL_BRAND] === true;
}

export function fragmentKey(ref: FragmentRef<any>): string {
  if (!isLocalFragmentRef(ref)) throw new Error("fragmentKey(): expected an opaque fragment ref.");
  return stableKey({ table: ref.table, pk: ref.pk });
}

export function createLocalFragmentRef<F extends Fragment<any, any, any>>(
  fragment: F,
  pk: Record<string, LitValue>,
  coverage: FragmentCoverage,
): LocalFragmentRef<F> {
  return createLocalFragmentRefForTable(fragment.table[SCHEMA].name, pk, coverage);
}

export function createLocalFragmentRefForTable<F extends Fragment<any, any, any>>(
  table: string,
  pk: Record<string, LitValue>,
  coverage: FragmentCoverage,
): LocalFragmentRef<F> {
  return {
    [LOCAL_FRAGMENT_REF_BRAND]: true,
    source: "local",
    table,
    pk: { ...pk },
    coverage,
  };
}

export function createRootFragmentRef<F extends Fragment<any, any, any>, Q extends AnyQuery>(
  fragment: F,
  query: Q,
  coverageKey = stableKey({
    ast: query.ast(),
    remote: typeof query.name === "string" ? { name: query.name, args: query.args } : null,
  }),
): LocalFragmentRef<F> {
  const ast = query.ast();
  const table = fragment.table[SCHEMA];
  if (ast.table !== table.name) {
    throw new Error(
      `useRoot(): the coverage query is over "${ast.table}" but the fragment is over "${table.name}".`,
    );
  }
  const pk = primaryKeyFromWhere(ast, table.primaryKey);
  return createLocalFragmentRef(fragment, pk, { key: coverageKey, query });
}

export function fragmentAst<F extends Fragment<any, any, any>>(fragment: F): Ast {
  return childAst(fragment.table as AnyTable, fragment as unknown as (q: unknown) => unknown);
}

export function localFragmentReadAst<F extends Fragment<any, any, any>>(
  fragment: F,
  ref: LocalFragmentRef<F>,
  primaryKeyFor: (table: string) => readonly string[],
): Ast {
  const table = fragment.table[SCHEMA].name;
  if (ref.table !== table) {
    throw new Error(`useFragment(): the ref is for "${ref.table}" but the fragment is over "${table}".`);
  }
  const ast = localizeFragmentAst(fragmentAst(fragment), primaryKeyFor);
  ast.where = andConditions(pkConditions(ref.pk), ast.where);
  ast.one = true;
  return ast;
}

export function localRootFragmentRefsAst<F extends Fragment<any, any, any>>(
  fragment: F,
  query: AnyQuery,
  primaryKeyFor: (table: string) => readonly string[],
): Ast {
  const ast = query.ast();
  const table = fragment.table[SCHEMA].name;
  if (ast.table !== table) {
    throw new Error(
      `useRoot(): the coverage query is over "${ast.table}" but the fragment is over "${table}".`,
    );
  }
  const out: Ast = { ...ast, select: uniqSort([...primaryKeyFor(table)]) };
  delete out.related;
  return out;
}

export function localQueryReadAst(
  query: AnyQuery,
  primaryKeyFor: (table: string) => readonly string[],
): Ast {
  return localizeFragmentAst(query.ast(), primaryKeyFor);
}

export function queryFromAst(ast: Ast): AnyQuery {
  return {
    ast: () => ast,
    materialize: () => {
      throw new Error("queryFromAst(): materialize() requires a Store; pass this query through Store/React.");
    },
  } as unknown as AnyQuery;
}

function primaryKeyFromWhere(ast: Ast, primaryKey: readonly string[]): Record<string, LitValue> {
  const found = new Map<string, LitValue>();
  collectLiteralEquals(ast.where, found);
  const pk: Record<string, LitValue> = {};
  for (const col of primaryKey) {
    if (!found.has(col)) {
      throw new Error(
        `useRoot(): the coverage query must constrain primary key column "${col}" with a literal equality.`,
      );
    }
    pk[col] = found.get(col)!;
  }
  return pk;
}

function collectLiteralEquals(cond: Condition | undefined, out: Map<string, LitValue>): void {
  if (!cond) return;
  if (cond.type === "and") {
    for (const c of cond.conditions) collectLiteralEquals(c, out);
    return;
  }
  if (cond.type !== "simple" || cond.op !== "=") return;
  const leftCol = cond.left.type === "column" ? cond.left.name : undefined;
  const rightCol = cond.right.type === "column" ? cond.right.name : undefined;
  if (leftCol !== undefined && cond.right.type === "literal") out.set(leftCol, cond.right.value);
  else if (rightCol !== undefined && cond.left.type === "literal") out.set(rightCol, cond.left.value);
}

function pkConditions(pk: Readonly<Record<string, LitValue>>): Condition[] {
  return Object.keys(pk)
    .sort()
    .map((name) => fieldCondition(name, pk[name]));
}

function andConditions(conditions: Condition[], tail: Condition | undefined): Condition | undefined {
  const all = tail ? [...conditions, tail] : conditions;
  if (all.length === 0) return undefined;
  if (all.length === 1) return all[0];
  return { type: "and", conditions: all };
}

function localizeFragmentAst(ast: Ast, primaryKeyFor: (table: string) => readonly string[]): Ast {
  const out: Ast = { ...ast };
  if (ast.select !== undefined) out.select = uniqSort([...ast.select, ...primaryKeyFor(ast.table)]);
  if (ast.related !== undefined) {
    out.related = ast.related.map((rel) => {
      if (rel.subquery.aggregate !== undefined) return rel;
      if (!isFragmentRelationship(rel)) {
        return { ...rel, subquery: localizeFragmentAst(rel.subquery, primaryKeyFor) };
      }
      const childPk = primaryKeyFor(rel.subquery.table);
      const subquery: Ast = {
        ...rel.subquery,
        select: uniqSort([...childPk]),
      };
      delete subquery.related;
      return markFragmentRelationship({ ...rel, subquery });
    });
  }
  return out;
}

// ----------------------------- the runtime builder -----------------------------

interface State {
  table: string;
  alias?: string;
  wheres: Condition[];
  orderBy: OrderPart[];
  related: CorrelatedSubquery[];
  start?: Bound;
  limit?: number;
  one: boolean;
  select?: string[];
  // Top-level aggregate (REDUCE-DESIGN.md §8): `aggregate` reshapes the query into a `count(*)`;
  // `groupBy` partitions it; `having` filters the post-aggregation rows. All three are read by the
  // engine only together (see the guard in `compile`).
  aggregate?: "count";
  groupBy: string[];
  having?: Condition;
}

interface NamedQueryState {
  name: string;
  args: unknown;
  /** The §2.1 realtime label (metadata only — never part of the wire identity). */
  realtime?: RealtimeQueryLabel;
}

function emptyState(table: string): State {
  return { table, wheres: [], orderBy: [], related: [], one: false, groupBy: [] };
}

function compile(s: State): Ast {
  const ast: Ast = { table: s.table };
  if (s.alias !== undefined) ast.alias = s.alias;
  if (s.wheres.length === 1) ast.where = s.wheres[0];
  else if (s.wheres.length > 1) ast.where = { type: "and", conditions: s.wheres };
  if (s.related.length > 0) ast.related = s.related;
  if (s.start !== undefined) ast.start = s.start;
  if (s.orderBy.length > 0) ast.orderBy = s.orderBy;
  if (s.limit !== undefined) ast.limit = s.limit;
  if (s.one) ast.one = true;
  if (s.select && s.select.length > 0) ast.select = s.select;
  if (s.aggregate !== undefined) ast.aggregate = s.aggregate;
  if (s.groupBy.length > 0) ast.groupBy = s.groupBy;
  if (s.having !== undefined) ast.having = s.having;
  // The engine takes the aggregate lowering ONLY when `aggregate` is set (REDUCE-DESIGN.md §8 /
  // builder `build_pipeline`); `groupBy`/`having` on the row spine would be silently ignored. Fail
  // loudly so a forgotten `.count()` is a clear error, not a query that quietly returns all rows.
  if ((ast.groupBy !== undefined || ast.having !== undefined) && ast.aggregate === undefined) {
    throw new Error(
      "groupBy()/having() require count(): a top-level GROUP BY / HAVING is only honored on an " +
        "aggregate query (REDUCE-DESIGN.md §8) — add .count().",
    );
  }
  return ast;
}

/** Every base table an AST draws rows from — the root plus every related / `EXISTS` subquery (a
 *  conservative deep scan for `table` fields, mirroring the optimistic backend's `collectTables`). */
function astTables(ast: Ast): Set<string> {
  const out = new Set<string>();
  const walk = (v: unknown): void => {
    if (Array.isArray(v)) {
      for (const x of v) walk(x);
    } else if (v && typeof v === "object") {
      const o = v as Record<string, unknown>;
      if (typeof o.table === "string") out.add(o.table);
      for (const k of Object.keys(o)) walk(o[k]);
    }
  };
  walk(ast);
  return out;
}

/** Throw if `ast` references a local-only table ANYWHERE — the complete form of the {@link queries}
 *  root guard (Q1 / E3 client half). The root proxy `get` only sees the table accessed off the
 *  builder root; a local table reached through a relationship / `sub` / `countAs` / `exists` CHILD is
 *  passed as a value and never touches the proxy, so it is caught here instead: at `.ast()` for a
 *  server-scope builder, and again as an SSR backstop in `ServerStore.preload`. A server/named query
 *  may never name a local table (`201-LOCAL-ONLY-TABLES-DESIGN.md` §5). */
export function assertNoLocalTables<S extends ColsMap>(ast: Ast, schema: Schema<S>): void {
  for (const t of astTables(ast)) {
    if (isLocalTable(schema, t)) {
      throw new Error(
        `local-only table "${t}" may not be used in a server/named query — it was reached through a ` +
          `relationship/subquery; build it from store.query (the local builder) instead ` +
          `(201-LOCAL-ONLY-TABLES-DESIGN.md §5 / Q1 / E3).`,
      );
    }
  }
}

/** Build a child AST for `sub`/`exists`: a fresh child query, the optional `build`, compiled. */
function childAst(child: AnyTable, build: ((q: unknown) => unknown) | undefined): Ast {
  const cm = child[SCHEMA];
  let cq: unknown = makeQuery(cm, emptyState(cm.name));
  if (build) cq = build(cq);
  return (cq as { ast(): Ast }).ast();
}

interface ResolvedCorrelated {
  child: AnyTable;
  corr: { parent: string[]; child: string[] };
  build: ((q: unknown) => unknown) | undefined;
  fragment: boolean;
}

function composeCorrelatedBuild(
  build: ((q: unknown) => unknown) | undefined,
  edge: ((q: unknown) => unknown) | undefined,
): ((q: unknown) => unknown) | undefined {
  if (edge === undefined) return build;
  if (!isFragment(build)) {
    throw new Error("sub(): an edge callback is only supported when the child build is a Fragment.");
  }
  return (q: unknown) => edge(build(q as Query<AnyCols>));
}

/** Resolve the two `sub`/`countAs`/`exists` call shapes to a common `{ child, corr, build }`:
 *  either a named {@link Relationship} (`(rel, build?)`) or an explicit `(child, corr, build?)`. */
function resolveCorrelated(a: AnyTable | AnyRelationship, b: unknown, c: unknown, d?: unknown): ResolvedCorrelated {
  if (isRelationship(a)) {
    return {
      child: a.child as AnyTable,
      corr: { parent: [...a.correlation.parent], child: [...a.correlation.child] },
      build: composeCorrelatedBuild(
        b as ((q: unknown) => unknown) | undefined,
        c as ((q: unknown) => unknown) | undefined,
      ),
      fragment: isFragment(b),
    };
  }
  return {
    child: a,
    corr: b as { parent: string[]; child: string[] },
    build: composeCorrelatedBuild(
      c as ((q: unknown) => unknown) | undefined,
      d as ((q: unknown) => unknown) | undefined,
    ),
    fragment: isFragment(c),
  };
}

// ----------------------------- the merge pass (`include`, FRAGMENT-COMPOSITION-DESIGN §5) -----------
//
// `include(fragment)` folds a fragment's selection into the CURRENT node (same table). The merge is
// canonical (sorted) so include order is irrelevant — `q.include(A).include(B)` and
// `q.include(B).include(A)` compile to byte-identical ASTs → one `viewKey` → one materialization
// (§10.5). Two fragments that spread the same relationship `alias` are merged recursively; if they
// disagree on what rows that edge selects (correlation / table / where / orderBy / limit / paging /
// aggregate), that is an unresolvable conflict and we throw (§10.2).

function uniqSort(cols: string[]): string[] {
  return [...new Set(cols)].sort();
}

/** Merge two same-node *fragment* selections: an empty (absent) `select` means "all columns", so
 *  merging "all" with anything stays "all" (§5). Both sides here are fragment-contributed. */
function mergeNestedSelect(a: string[] | undefined, b: string[] | undefined): string[] | undefined {
  if (a === undefined || a.length === 0 || b === undefined || b.length === 0) return undefined;
  return uniqSort([...a, ...b]);
}

/** Merge a fragment's `select` into the ROOTING query's `select`. A pure **union** — this mirrors
 *  the type level (`Sel | FSel`, where a fragment that selects nothing contributes `never`) and keeps
 *  `q.include(Frag)` equivalent to applying `Frag(q)`. An absent `select` (on either side) means
 *  "contributes no columns", NOT "all columns" — so a relationship-only fragment (e.g. an edge that
 *  just `sub`s, no root columns) doesn't blow the projection open. The result is "all columns" (a
 *  dropped `select`) only when nothing anywhere selected, i.e. the union is empty. (This differs from
 *  {@link mergeNestedSelect}: a *nested* same-alias child is an INTERSECTION of row types at the
 *  type level — `Proj<A> & Proj<B>` — so there a no-select side, meaning "all child columns", wins.) */
function mergeRootSelect(baseSel: string[] | undefined, fragSel: string[] | undefined): string[] | undefined {
  const union = uniqSort([...(baseSel ?? []), ...(fragSel ?? [])]);
  return union.length === 0 ? undefined : union;
}

function aliasOf(csq: CorrelatedSubquery): string {
  return csq.subquery.alias ?? "";
}

/** A canonical key over everything that defines *which* rows a related edge yields — the fields
 *  two same-alias spreads must AGREE on (select + related are merged, so they're excluded). */
function edgeShapeKey(csq: CorrelatedSubquery): string {
  const sq = csq.subquery;
  return stableKey({
    correlation: csq.correlation,
    system: csq.system,
    table: sq.table,
    where: sq.where,
    orderBy: sq.orderBy,
    limit: sq.limit,
    start: sq.start,
    one: sq.one,
    aggregate: sq.aggregate,
    aggregatePrecomputed: sq.aggregatePrecomputed,
  });
}

/** Merge the `select` + nested `related` of two same-alias subqueries (callers verified the edge
 *  shape agrees). Returns a canonical (sorted) AST, omitting empty `select`/`related` like compile. */
function mergeSubqueryAst(base: Ast, frag: Ast): Ast {
  const out: Ast = { ...base };
  const select = mergeNestedSelect(base.select, frag.select);
  if (select === undefined) delete out.select;
  else out.select = select;
  const related = mergeRelatedLists(base.related ?? [], frag.related ?? []);
  if (related.length === 0) delete out.related;
  else out.related = related;
  return out;
}

/** Merge two related lists by `alias`: same alias ⇒ recurse (after an edge-shape agreement check);
 *  distinct aliases ⇒ kept side by side. Output is ordered by alias (canonical, §10.5). */
function mergeRelatedLists(base: CorrelatedSubquery[], add: CorrelatedSubquery[]): CorrelatedSubquery[] {
  const byAlias = new Map<string, CorrelatedSubquery>();
  for (const csq of base) byAlias.set(aliasOf(csq), csq);
  for (const csq of add) {
    const alias = aliasOf(csq);
    const existing = byAlias.get(alias);
    if (existing === undefined) {
      byAlias.set(alias, csq);
      continue;
    }
    if (edgeShapeKey(existing) !== edgeShapeKey(csq)) {
      throw new Error(
        `include(): conflicting definitions for relationship "${alias}" — two fragments spread the ` +
          `same alias with different correlation/table/where/orderBy/limit. Give them distinct aliases ` +
          `or align them (FRAGMENT-COMPOSITION-DESIGN §10.2).`,
      );
    }
    const merged = { ...existing, subquery: mergeSubqueryAst(existing.subquery, csq.subquery) };
    byAlias.set(alias, isFragmentRelationship(existing) || isFragmentRelationship(csq) ? markFragmentRelationship(merged) : merged);
  }
  return [...byAlias.values()].sort((a, b) => {
    const x = aliasOf(a);
    const y = aliasOf(b);
    return x < y ? -1 : x > y ? 1 : 0;
  });
}

/** Fold an `include`d fragment's compiled AST into the current builder {@link State}. An included
 *  fragment contributes only `select` + nested relationships; it may not constrain the node's row
 *  set / window — adding a root `where` (§10.3) or `orderBy`/`limit`/`start`/`one` throws. */
function mergeIncludedFragment(s: State, frag: Ast): State {
  if (frag.where !== undefined) {
    throw new Error(
      "include(): a fragment may not add a root `where` — only the rooting query filters " +
        "(FRAGMENT-COMPOSITION-DESIGN §10.3).",
    );
  }
  if (frag.orderBy !== undefined || frag.limit !== undefined || frag.start !== undefined || frag.one) {
    throw new Error(
      "include(): a fragment may not set a root `orderBy`/`limit`/`start`/`one` — it contributes only " +
        "`select` and nested relationships (those define the node's window, which is the rooting query's job).",
    );
  }
  const select = mergeRootSelect(s.select, frag.select);
  const next: State = { ...s, related: mergeRelatedLists(s.related, frag.related ?? []) };
  if (select === undefined) delete next.select;
  else next.select = select;
  return next;
}

// Internal: the runtime is untyped (Proxy magic); the public `Query<C,Rels>` type is the contract.
function makeQuery(
  meta: TableMeta,
  s: State,
  onMat?: (query: AnyQuery) => unknown,
  named?: NamedQueryState,
  // Set on a SERVER-scope root (`newQueryBuilder` / `includeLocal: false`); validates the finalized
  // AST has no local-only table (caught even when reached via a child). Threaded to descendants of
  // the same root so chained `.where`/`.sub`/`.countAs`/stamp keep enforcing it (Q1 / E3).
  guardAst?: (ast: Ast) => void,
): unknown {
  const next = (patch: Partial<State>): unknown => makeQuery(meta, { ...s, ...patch }, onMat, named, guardAst);
  const applyField = (field: string, arg: unknown) =>
    next({ wheres: [...s.wheres, fieldCondition(field, arg)] });
  let proxy: unknown;

  const base: Record<string, unknown> = {
    name: named?.name,
    args: named?.args,
    realtime: named?.realtime,
    where: (cond: Condition) => next({ wheres: [...s.wheres, cond] }),
    orderBy: (col: string, dir: Dir) => next({ orderBy: [...s.orderBy, [col, dir]] }),
    select: (...cols: string[]) => next({ select: [...(s.select ?? []), ...cols] }),
    limit: (n: number) => next({ limit: n }),
    start: (cursor: Record<string, LitValue>, opts?: { exclusive?: boolean }) =>
      next({ start: { row: { ...cursor }, exclusive: opts?.exclusive ?? false } }),
    one: () => next({ one: true }),
    include: (fragment: Fragment<AnyCols, unknown, string>) => {
      const fragTable = fragment.table;
      if (fragTable[SCHEMA].name !== s.table) {
        throw new Error(
          `include(): the fragment is over "${fragTable[SCHEMA].name}" but this query is over "${s.table}". ` +
            `include() merges a fragment into the SAME table — use sub() to nest a different table.`,
        );
      }
      const fragAst = childAst(fragTable as AnyTable, fragment as unknown as (q: unknown) => unknown);
      return makeQuery(meta, mergeIncludedFragment(s, fragAst), onMat, named, guardAst);
    },
    sub: (alias: string, childOrRel: AnyTable | AnyRelationship, b?: unknown, c?: unknown, d?: unknown) => {
      const { child, corr, build, fragment } = resolveCorrelated(childOrRel, b, c, d);
      const sub = childAst(child, build);
      sub.alias = alias;
      const csq: CorrelatedSubquery = {
        correlation: { parentField: corr.parent, childField: corr.child },
        subquery: sub,
      };
      return next({ related: [...s.related, fragment ? markFragmentRelationship(csq) : csq] });
    },
    countAs: (alias: string, childOrRel: AnyTable | AnyRelationship, b?: unknown, c?: unknown) => {
      // Same correlated-child mechanism as `sub`, but mark the child a `count` aggregate:
      // the builder lowers it to a scalar-projected singular relationship (REDUCE-DESIGN §9).
      const { child, corr, build } = resolveCorrelated(childOrRel, b, c);
      const sub = childAst(child, build);
      sub.alias = alias;
      sub.aggregate = "count";
      const csq: CorrelatedSubquery = {
        correlation: { parentField: corr.parent, childField: corr.child },
        subquery: sub,
      };
      return next({ related: [...s.related, csq] });
    },
    // Top-level aggregate (REDUCE-DESIGN §8): `count` reshapes this query into a `count(*)`;
    // `groupBy` partitions it; `having` filters the post-aggregation rows. `having`'s callback gets
    // a field binder over the aggregate's output columns (group cols + the synthetic `count`), so a
    // predicate can name `count` — which lives on no base table — exactly like `where.<field>(…)`.
    count: () => next({ aggregate: "count" }),
    groupBy: (col: string) => next({ groupBy: [...s.groupBy, col] }),
    having: (a: unknown, op?: SimpleOp, val?: number) => {
      // Overload 1 — top-level aggregate HAVING: `having((h) => h.count(gt(3)))`. The callback
      // binds the aggregate's output columns (group cols + the synthetic `count`).
      if (typeof a === "function") {
        const build = a as (h: Record<string, (arg: unknown) => Condition>) => Condition;
        const h = new Proxy({} as Record<string, (arg: unknown) => Condition>, {
          get: (_t, prop) => (typeof prop === "string" ? (arg: unknown) => fieldCondition(prop, arg) : undefined),
        });
        return next({ having: build(h) });
      }
      // Overload 2 — filter THIS parent by a child aggregate's count:
      // `.having("commentCount", ">", 10)` (PARENT-AGGREGATE-FILTER-DESIGN.md). Bind the existing
      // `countAs(alias, …)` relationship and push a hidden EXISTS whose subquery clones the same
      // correlation + child + `count` aggregate, plus a `HAVING count <op> val`. The engine lowers
      // it to an EXISTS over a HAVING-filtered reduce; the display `countAs` is untouched.
      const alias = a as string;
      const display = s.related.find(
        (r) => r.subquery.alias === alias && r.subquery.aggregate === "count",
      );
      if (!display) {
        throw new Error(
          `having("${alias}", …): this query has no countAs("${alias}", …) relationship to filter ` +
            `on — attach the child count aggregate first.`,
        );
      }
      const gate: CorrelatedSubquery = {
        correlation: display.correlation,
        subquery: {
          ...display.subquery,
          alias: `__having_${alias}`,
          having: {
            type: "simple",
            op: op as SimpleOp,
            left: { type: "column", name: "count" },
            right: { type: "literal", value: val as LitValue },
          },
        },
      };
      return next({ wheres: [...s.wheres, { type: "correlatedSubquery", op: "EXISTS", related: gate }] });
    },
    ast: () => {
      const a = compile(s);
      guardAst?.(a);
      return a;
    },
    materialize: () => {
      if (!onMat) throw new Error("materialize() requires a Store — use store.query.<table>");
      return onMat(proxy as AnyQuery);
    },
  };

  // `where` is callable AND a field proxy.
  const whereProxy = new Proxy(base.where as object, {
    get(_t, prop) {
      if (typeof prop === "string") return (arg: unknown) => applyField(prop, arg);
      return undefined;
    },
  });

  proxy = new Proxy(base, {
    get(target, prop) {
      if (prop === "where") return whereProxy;
      if (prop === STAMP_NAMED_QUERY)
        return (name: string, args: unknown, realtime?: RealtimeQueryLabel) =>
          makeQuery(meta, s, onMat, { name, args, realtime }, guardAst);
      if (typeof prop === "string" && prop.length > 5 && prop.startsWith("where")) {
        const field = prop[5].toLowerCase() + prop.slice(6);
        return (arg: unknown) => applyField(field, arg);
      }
      return target[prop as string];
    },
  });
  return proxy;
}

// ----------------------------- EXISTS / NOT EXISTS -----------------------------

/** Options for `exists` / `notExists`. */
export interface ExistsOpts {
  /**
   * Fold this `EXISTS` as a build-time **scalar** subquery: when the child binds a
   * statically-unique key, the engine reads it once, inlines the correlation value as a
   * literal, and deletes the join. **Snapshot semantics** — the inlined value does not
   * react to later child changes. Defaults to `false` (a live `EXISTS` join).
   */
  scalar?: boolean;
}

function existsImpl(
  child: AnyTable,
  corr: { parent: string[]; child: string[] },
  build: ((q: unknown) => unknown) | undefined,
  op: ExistsOp,
  // `"permissions"` ⇒ a server-only, non-syncing gate (`exists_noSync`): the normalized
  // serializer prunes its witnesses so the permission table is never synced to the client.
  system?: "permissions",
  opts?: ExistsOpts,
): Condition {
  const sub = childAst(child, build);
  sub.alias = child[SCHEMA].name;
  const related: CorrelatedSubquery = {
    correlation: { parentField: corr.parent, childField: corr.child },
    subquery: sub,
  };
  if (system) related.system = system;
  return {
    type: "correlatedSubquery",
    op,
    related,
    ...(opts?.scalar ? { scalar: true } : {}),
  };
}

/** `EXISTS` by a named {@link Relationship} — correlation from the rel; the parent row type is the
 *  relationship's parent, so the condition lands on the matching `.where()`. */
export function exists<PC extends AnyCols, CC extends AnyCols>(
  relationship: Relationship<PC, CC>,
  build?: (q: Query<CC>) => Query<CC, unknown>,
  opts?: ExistsOpts,
): Cond<RowOf<PC>>;
/** `EXISTS (<correlated subquery>)` — a condition (use inside `where`/`or`/`and`). */
export function exists<CC extends AnyCols, R = unknown>(
  child: TableLike<CC>,
  corr: { parent: Array<keyof R & string>; child: Array<keyof CC & string> },
  build?: (q: Query<CC>) => Query<CC, unknown>,
  opts?: ExistsOpts,
): Cond<R>;
export function exists(a: AnyTable | AnyRelationship, b?: unknown, c?: unknown, d?: unknown): Cond<unknown> {
  const { child, corr, build } = resolveCorrelated(a, b, c);
  const opts = (isRelationship(a) ? c : d) as ExistsOpts | undefined;
  return existsImpl(child, corr, build, "EXISTS", undefined, opts) as Cond<unknown>;
}

/** `NOT EXISTS` by a named {@link Relationship}. */
export function notExists<PC extends AnyCols, CC extends AnyCols>(
  relationship: Relationship<PC, CC>,
  build?: (q: Query<CC>) => Query<CC, unknown>,
  opts?: ExistsOpts,
): Cond<RowOf<PC>>;
/** `NOT EXISTS (<correlated subquery>)`. */
export function notExists<CC extends AnyCols, R = unknown>(
  child: TableLike<CC>,
  corr: { parent: Array<keyof R & string>; child: Array<keyof CC & string> },
  build?: (q: Query<CC>) => Query<CC, unknown>,
  opts?: ExistsOpts,
): Cond<R>;
export function notExists(a: AnyTable | AnyRelationship, b?: unknown, c?: unknown, d?: unknown): Cond<unknown> {
  const { child, corr, build } = resolveCorrelated(a, b, c);
  const opts = (isRelationship(a) ? c : d) as ExistsOpts | undefined;
  return existsImpl(child, corr, build, "NOT EXISTS", undefined, opts) as Cond<unknown>;
}

/**
 * `EXISTS (<correlated subquery>)` as a **server-only, non-syncing** gate (`exists_noSync`).
 * Identical to {@link exists} for filtering parent visibility, but stamps the subquery
 * `system: "permissions"`, so the engine's normalized serializer prunes its witnesses from the
 * footprint — the permission table's rows are never synced to the client and the client never
 * re-evaluates the gate. Use this when building the **server's** query (the user's API server);
 * the client holds its own un-gated query.
 */
export function existsNoSync<PC extends AnyCols, CC extends AnyCols>(
  relationship: Relationship<PC, CC>,
  build?: (q: Query<CC>) => Query<CC, unknown>,
): Cond<RowOf<PC>>;
export function existsNoSync<CC extends AnyCols, R = unknown>(
  child: TableLike<CC>,
  corr: { parent: Array<keyof R & string>; child: Array<keyof CC & string> },
  build?: (q: Query<CC>) => Query<CC, unknown>,
): Cond<R>;
export function existsNoSync(a: AnyTable | AnyRelationship, b?: unknown, c?: unknown): Cond<unknown> {
  const { child, corr, build } = resolveCorrelated(a, b, c);
  return existsImpl(child, corr, build, "EXISTS", "permissions") as Cond<unknown>;
}

/** `NOT EXISTS (<correlated subquery>)` as a **server-only, non-syncing** gate — the `NOT EXISTS` form of {@link existsNoSync} (a deny-style permission rule). */
export function notExistsNoSync<PC extends AnyCols, CC extends AnyCols>(
  relationship: Relationship<PC, CC>,
  build?: (q: Query<CC>) => Query<CC, unknown>,
): Cond<RowOf<PC>>;
export function notExistsNoSync<CC extends AnyCols, R = unknown>(
  child: TableLike<CC>,
  corr: { parent: Array<keyof R & string>; child: Array<keyof CC & string> },
  build?: (q: Query<CC>) => Query<CC, unknown>,
): Cond<R>;
export function notExistsNoSync(a: AnyTable | AnyRelationship, b?: unknown, c?: unknown): Cond<unknown> {
  const { child, corr, build } = resolveCorrelated(a, b, c);
  return existsImpl(child, corr, build, "NOT EXISTS", "permissions") as Cond<unknown>;
}

// ----------------------------- the query root (store.query.<table>) -----------------------------

export type QueryRoot<S extends ColsMap> = { [N in keyof S]: Query<S[N]> };

/** Scoping for {@link queries} (`201-LOCAL-ONLY-TABLES-DESIGN.md` §5). */
export interface QueriesOptions {
  /** Include {@link TableMeta.local local-only} tables in the builder's root scope. The **local**
   *  builder (`store.query`) sets this; the **server** builder ({@link newQueryBuilder}) does NOT,
   *  so naming a local table in a remote/named query is a build error (Q1 / E3 client half). */
  includeLocal?: boolean;
}

/** A typed query entry over a schema: `queries(schema).issue.where.closed(false)…`. */
export function queries<S extends ColsMap>(
  schema: Schema<S>,
  onMaterialize?: (query: AnyQuery) => unknown,
  opts?: QueriesOptions,
): QueryRoot<S> {
  const includeLocal = opts?.includeLocal ?? false;
  return new Proxy({} as Record<string, unknown>, {
    get(_t, prop) {
      if (typeof prop !== "string") return undefined;
      // Framework/JS introspection probes are never table names — return undefined instead of
      // throwing so the query root (and a `Store` carrying it) can be safely enumerated by code
      // that inspects objects. React 19's dev-mode prop diffing reads `$$typeof` on every prop
      // value; `then` is the thenable check; `toJSON` is JSON serialization. A real typo'd table
      // (`store.query.isue`) is a plain identifier and still throws below.
      if (prop[0] === "$" || prop === "then" || prop === "toJSON") return undefined;
      const meta = schema.tables[prop];
      if (!meta) throw new Error(`unknown table: ${prop}`);
      // Server scope: a local-only table is absent (Q1 / E3 client half) — a remote/named query
      // may never reference one. The local builder (`store.query`) opts in via `includeLocal`.
      if (meta.local && !includeLocal) {
        throw new Error(
          `local-only table "${prop}" may not be used in a server/named query — build it from ` +
            `store.query (the local builder) and materialize it ad-hoc (201-LOCAL-ONLY-TABLES-DESIGN.md §5 / Q1).`,
        );
      }
      // A SERVER-scope builder (`!includeLocal`) also guards CHILD references: the root proxy never
      // sees a table passed by value to `sub`/`countAs`/`exists`, so the finalized AST is re-checked
      // at `.ast()` (Q1 / E3 client half — the complete form of the root check above).
      const guardAst = includeLocal ? undefined : (ast: Ast) => assertNoLocalTables(ast, schema);
      return makeQuery(meta, emptyState(meta.name), onMaterialize, undefined, guardAst);
    },
  }) as QueryRoot<S>;
}

/** A schema-bound query-builder factory for portable client/server query definitions. SERVER
 *  scope: local-only tables are excluded (a named/remote query may never reference one). */
export function newQueryBuilder<S extends ColsMap>(schema: Schema<S>): QueryRoot<S> {
  return queries(schema);
}
