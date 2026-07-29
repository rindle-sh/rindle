<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/supported-queries-ts.md (https://rindle.sh/docs/supported-queries-ts)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# Supported query shapes (TypeScript)

> The honest matrix of what builds, pushes, and materializes today — written in the @rindle/client TypeScript builder. The explicit list of what does not work yet.

This page is the contract: which query shapes Rindle can lower into an
incrementally-maintained pipeline, and what each shape does at the three stages
of the engine. It is the [Rust reference](https://rindle.sh/docs/supported-queries) told in
TypeScript — the same engine, the same matrix, the `@rindle/client` builder.

> **Writing Rust?** Every shape here is in the
> [Rust query builder reference](https://rindle.sh/docs/supported-queries) too — same verdicts,
> the `rindle::table` builder.

- **fetch** — hydrate the pipeline and materialize the initial result.
- **push** — apply incremental source changes and emit the downstream change stream.
- **view** — the change reaches the materialized result and updates it in place.

Legend: ✅ supported · ⚠️ partial (see note) · ❌ unsupported. Every ✅ row is
backed by a passing test in the engine repo.

## How you express a query

A query is written in **Deltic** — Rindle's query language — using the typed
fluent builder. `store.query.<table>` is the entry point; you chain methods and
finish with `materialize()`, which opens a **live view**. There is no SQL string
and no separate CLI; the query *is* a TypeScript value, fully typed against your
schema.

```ts
import { gt } from "@rindle/client";

// Build a query: open issues, ordered, capped. Each method returns a new typed
// builder; `materialize()` opens the live view.
const view = store.query.issue
  .where.open(true)              // field proxy: `open = true`
  .where.priority(gt(3))         // explicit operator
  .orderBy("createdAt", "desc")
  .limit(50)
  .materialize();

// `subscribe` fires once immediately with the current rows (the `Hydrated`
// snapshot, every row an Add), then again after every committed write that
// affects the query — each time with the new materialized array.
const unsubscribe = view.subscribe((rows) => {
  // rows: { id; title; priority; open }[]
  render(rows);
});
```

The contract is **view-after-write == fresh-query**: you fold nothing yourself;
the view is always exactly what a from-scratch query would return. (Need the raw
delta vocabulary instead of a folded array? That's the
[Rust delta-stream seam](https://rindle.sh/docs/example-rust) — the `@rindle/client` view does the
folding for you.)

`where` is both callable and a field proxy, with camelCase sugar — three spellings
of the same thing:

```ts
import { or, eq, gt } from "@rindle/client";

store.query.issue.where(or(issue.priority(gt(8)), issue.open(eq(true)))); // condition form
store.query.issue.where.open(true);                                       // field proxy
store.query.issue.whereOpen(true);                                        // camelCase sugar
```

The escape hatch is `.ast()`, which returns the wire `Ast` the builder compiles
to (the same value `.materialize()` consumes). On the synced client, a query that
opens a **server** subscription must be **named** — define it with `defineQuery`
(co-located with its component) and call the value (`issuesPageQuery(args)`); a bare
`store.query.…` builder query resolves locally only. See
[the browser client](https://rindle.sh/docs/client).

## Correlated relationships and EXISTS

Relationships are correlated subqueries. You give the correlation as
`{ parent: [...], child: [...] }` — the parent columns on the left, the child
columns they match on the right.

```ts
import { exists, notExists } from "@rindle/client";

// issues, each carrying its comments (a materialized relationship)
const withComments = store.query.issue
  .sub("comments", comment, { parent: ["id"], child: ["issueId"] })
  .materialize();
// rows: ( …Issue & { comments: Comment[] } )[]

// only issues that have at least one comment (an EXISTS filter)
const commented = store.query.issue
  .where(exists(comment, { parent: ["id"], child: ["issueId"] }))
  .materialize();

// the negation
const uncommented = store.query.issue
  .where(notExists(comment, { parent: ["id"], child: ["issueId"] }))
  .materialize();
```

`sub` takes an optional final builder to shape the child (`(c) => c.orderBy("id", "asc")`),
and `exists` / `notExists` take an optional builder to filter the gate
(`(c) => c.where.spam(false)`).

If you'd rather not restate the same correlation at every call site, declare each
join once with `defineRelationships` and pass the named relationship to `sub` /
`countAs` / `exists` instead of the `{ parent, child }` object — it lowers to the
exact same correlated subquery:

```ts
import { defineRelationships, rel } from "@rindle/client";

const rels = defineRelationships({
  issueComments: rel(issue, comment, { id: "issueId" }), // issue.id → comment.issueId
});

store.query.issue.sub("comments", rels.issueComments).materialize();
store.query.issue.where(exists(rels.issueComments)).materialize();
```

## Aggregates: a live `count`

`countAs` attaches a **correlated child count** to each parent row as a scalar —
maintained incrementally, so adding or removing a child increments or decrements
the count without re-scanning. An empty child reads `0`. The alias resolves to a
`number`, not an array:

```ts
const view = store.query.issue
  .countAs("commentCount", comment, { parent: ["id"], child: ["issueId"] })
  .materialize();
// rows: ( …Issue & { commentCount: number } )[]
```

`count` is the only aggregate today; `sum` / `avg` / `min` / `max` are designed
but not yet built, and the aggregate is a **relationship** count keyed on the
correlation — a top-level `count(table)` over a whole table is not exposed.

## Scalar subqueries: fold a unique lookup at build time

When an `exists` child binds a **statically-unique key** (a primary key or a
unique index, fully pinned to constants), pass `{ scalar: true }`. The resolver
reads that one row **once at build time**, inlines its correlation value as a
literal, and deletes the join entirely. The parent pipeline never subscribes to
the child table.

```ts
import { exists } from "@rindle/client";

// only the project that owns issue #7 — resolved once, then a plain literal filter
store.query.project.where(
  exists(issue, { parent: ["id"], child: ["projectId"] }, (i) => i.where.id(7), { scalar: true }),
);
```

The trade-off is **snapshot semantics**: the inlined value is frozen for the
pipeline's lifetime, so changes to the child after build do not propagate. Leave
`scalar` off (the default) for an ordinary live `exists` join.

## Aggregates

`countAs` attaches a live count to each parent row. A query can also *be* the
aggregate: `count()` reshapes the result to count rows instead of materializing
them, `groupBy` keys it, and `having` filters the post-aggregation rows — all
maintained incrementally, like any other shape:

```ts
import { gt } from "@rindle/client";

// one { count } row, maintained as rows enter and leave the filter
store.query.issue.where.open(true).count().materialize();

// one { status, count } row per distinct status, HAVING count > 3.
// The `having` proxy addresses the aggregate's OUTPUT columns — the groupBy
// columns and the synthetic `count` (which lives on no base table).
store.query.issue
  .groupBy("status")
  .count()
  .having((h) => h.count(gt(3)))
  .materialize();

// filter the PARENT by a child count — issues with more than 10 comments.
// This `having(alias, op, n)` overload takes a `countAs` alias already on the query.
store.query.issue
  .countAs("commentCount", comment, { parent: ["id"], child: ["issueId"] })
  .having("commentCount", ">", 10)
  .materialize();
```

`having` filters *above* the aggregation; `where` filters base rows *below* it.
The parent-by-child-count overload accepts **high-pass** predicates only in v1
(see the matrix and rejections below), and the dropped parent's visible
`commentCount` is untouched — a survivor still shows its real count.

## Supported shapes

| Query shape | fetch | push | view | Notes |
|---|:---:|:---:|:---:|---|
| Simple `where` (`=`,`!=`,`<`,`>`,`<=`,`>=`) | ✅ | ✅ | ✅ | `.where.field(v)` / `eq` `ne` `lt` `gt` `le` `ge` |
| `is` / `isNot` (null-aware, three-valued) | ✅ | ✅ | ✅ | matches SQLite's three-valued logic |
| `like` / `ilike` / `notIlike`, incl. `\%`/`\_`/`\\` escapes | ✅ | ✅ | ✅ | memory matcher agrees with SQLite |
| `and` / `or` of leaf conditions | ✅ | ✅ | ✅ | `and(...)` / `or(...)` |
| `inList` / `notInList` over a literal list | ✅ | ✅ | ✅ | `.where.field(inList([...]))` |
| Sibling relationships (multiple `sub` on a row) | ✅ | ✅ | ✅ | |
| Nested relationships (`sub` with its own `sub`) | ✅ | ✅ | ✅ | nest inside the child builder |
| `start` paging bound | ✅ | ✅ | ✅ | `.start(cursor, { exclusive })` |
| `limit` (ordered take / exists cap) | ✅ | ✅ | ✅ | `.limit(n)` |
| `exists` (correlated EXISTS) | ✅ | ✅ | ✅ | `where(exists(child, corr))`; the engine picks the cheaper drive side (parent- or child-driven) internally |
| `notExists` (NOT EXISTS) | ✅ | ✅ | ✅ | `where(notExists(child, corr))` |
| Top-level `or` fan of EXISTS conditions | ✅ | ✅¹ | ✅ | |
| Nested `or`/`and` mix of EXISTS conditions | ✅ | ✅¹ | ✅ | including AND-within-AND |
| Multi-EXISTS under top-level `and`/`or` | ✅ | ✅ | ✅ | slots uniquified to distinct query-local ids |
| Deepest-nested child push | ✅ | ✅ | ✅ | surfaces as a re-projected child subtree |
| Self-join (reentrant fetch-during-push) | ✅ | ✅ | ✅ | |
| Many-to-many through a junction table | ✅ | ✅ | ✅ | nest `sub` through the junction; junction rows materialize uncollapsed (no hidden-edge magic) |
| Top-level `.one()` (singular root) | ✅ | ✅ | ✅ | caps the query to limit 1; the view's `.data` is `row \| null` |
| Relationship-level `.one()` (a singular `sub`) | ⚠️ | ⚠️ | ⚠️ | view layer implemented + unit-tested, not yet reachable via a query (builds plural today) |
| Aggregate: `countAs` of a correlated child | ✅ | ✅ | ✅ | a scalar count per parent row, incrementally maintained. `sum` / `avg` / `min` / `max` are designed but not built yet |
| Top-level `count()` (global aggregate) | ✅ | ✅ | ✅ | reshapes the result to one `{ count }` row instead of materializing rows |
| `groupBy` + `count()` (grouped aggregate) | ✅ | ✅ | ✅ | one `{ …group, count }` row per distinct value-tuple, keyed and sorted by the group columns |
| `having((h) => …)` (filter post-aggregation rows) | ✅ | ✅ | ✅ | the proxy addresses the `groupBy` columns and the synthetic `count` |
| `having(alias, op, n)` (filter a parent by a child count) | ⚠️ | ⚠️ | ⚠️ | gates a parent by a `countAs` alias, maintained incrementally; **v1: high-pass predicates only** — see rejections below |
| Scalar subquery (`exists` with `{ scalar: true }`) | ✅ | —² | ✅ | a build-time **snapshot**: a unique-key match is folded to a literal and the join is removed |
| Projection / column pruning (`select`) | ✅ | ✅ | ✅³ | `.select("id", "title")` drives **what syncs** — over the wire, into the client engine, and out to the view; a query never resolves a row from a column it did not select |
| Static / bound parameters | ❌ | ❌ | ❌ | a deprecated upstream (ZQL) form, not represented — permission subqueries instead carry the AST's `system: "permissions"` provenance tag and are pruned from sync |

¹ **`exists` under a union fan, on push — a deliberate divergence from upstream.**
For one internal lowering of an `exists` under a top-level `or` fan, Zero's JS
engine (which Rindle ports) emits a push result that *violates* the IVM
contract; Rindle upholds *view-after-push == fresh-query*, pinned by a dedicated
consistency test (`union_fan_consistency`). So the ✅ is real, but on this one
shape Rindle intentionally does **not** match upstream ZQL output.

² **A scalar subquery does not push.** That is the point: it is resolved **once,
at build time**, inlined to a literal, and the join is deleted — so the parent
pipeline never subscribes to the child table and later changes to it do not
propagate. Opt in per-condition (`{ scalar: true }`); leave it off for a live join.

³ **Projection's two remaining follow-ups.** The selection already shapes what
syncs end to end. Still outstanding (pure optimizations, not correctness): the
SQLite **leaf read-narrowing** (the server still `SELECT`s the full declared
column list from disk) and narrowing the **local view's reported column set**.
Neither changes results.

## Relationship slots are query-local

When two or more EXISTS conditions sit under a top-level `and` / `or`, the engine
uniquifies their internal slots to distinct query-local ids — you don't name them
(only `sub` and `countAs` take an explicit alias). The slot layout is **derived
from the query AST**, not from any engine-level schema — which is why a
`defineRelationships` value is pure convenience over the same inline correlation,
and a production-shaped schema needs no synthesized gate slots. The slot order is
materialized relationships (`sub` / `countAs`) first, then the
`exists` gates in `where`-tree pre-order — the one tree shared by the dataflow
joins, the gates, and the view materialization, so their relationship ids agree
by construction.

## Build-time rejections

These are genuine limitations. Two of them you hit before the query ever builds —
because the schema is **typed**, an unknown table or column is a **compile-time**
TypeScript error, not a runtime surprise. The rest surface as a thrown
`BuildError` when you `materialize()` (or, on the synced client, when the server
resolves the lease):

- **A root aggregate combined with row-shaping** — pairing `count()` with
  `select` / `sub` / `countAs` / `orderBy` / `limit` / `one` is rejected: a
  `count()` query's result *is* the aggregate output (`groupBy` columns +
  `count`), not rows.
- **A low-pass parent-by-child-count `having`** — a predicate *true* at count 0
  (`"<="`, `"<"` for `n ≥ 1`, `= 0`, `>= 0`) → `BuildError::Unsupported`. A
  childless parent forms no group, so those need row-widening (deferred); the
  high-pass set (`">"`; `">="` / `"="` / `"!="` for `n ≥ 1`) builds and maintains.
- **An `exists` subquery carrying a paging bound (`start`) or a nested
  relationship (`sub`)** → `BuildError::Unsupported`.
- **A bare top-level `exists` whose implied slot collides with a `sub` of the same
  name** → `BuildError::Unsupported` (one relationship per slot). Two `exists`
  under a top-level `and` / `or` are uniquified to distinct slots and never
  collide.

## A note on value types

Unlike the Rust delta stream — where cells arrive as an `OwnedValue` enum you
match on — the TypeScript view hands you **plain typed JavaScript values**,
shaped by your schema: a `number()` column reads back as `number`, `string()` as
`string`, `boolean()` as `boolean`, and a missing/null cell as `null`. A
`json<T>()` column is stored as a string and **parsed to an object once, on read**,
typed as `T`:

```ts
import { table, string, number, boolean, json } from "@rindle/client";

const issue = table("issue")
  .columns({
    id: number(),
    title: string(),
    priority: number(),
    open: boolean(),
    tags: json<string[]>(),  // stored as a string, read back as string[]
  })
  .primaryKey("id");

// view rows are fully typed — no enum matching, no manual coercion:
// { id: number; title: string; priority: number; open: boolean; tags: string[] }
```

The comparator that orders and dedupes rows is keyed on the column type, so the
sort you get in TypeScript is byte-for-byte the sort the Rust engine produces.

Where your data lives in SQLite, this schema isn't hand-written — it's **generated from your
SQL** ([schema & migrations](https://rindle.sh/docs/schema)). A column declared `BOOLEAN`/`JSON` generates
`boolean()`/`json()`; the only refinements you add by hand are a `json<T>()` element type or a
string/number literal union. The schema is a typed facade over the builder — the SQL is the
source of truth.

## Next steps

- [Reactive queries in the browser](https://rindle.sh/docs/wasm-client) — build and materialize a
  query end to end on the in-process wasm engine.
- [The browser client](https://rindle.sh/docs/client) — the same builder in a synced app:
  `defineQuery`, optimistic writes, live views.
- [Compose the UI with fragments](https://rindle.sh/docs/fragments) — split a query across the
  component tree with the same `select` / `sub` / `countAs` surface.
- [The change model](https://rindle.sh/docs/change-model) — the delta vocabulary the view folds for you.
- [Synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) — these shapes in a real React app.
