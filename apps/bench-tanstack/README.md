# @rindle/bench-tanstack — Rindle vs TanStack DB

A head-to-head benchmark of two **client-side, incremental-view-maintenance stores**:

- **[`@rindle/wasm`](../../packages/wasm)** — Rindle's engine compiled to WebAssembly, running queries
  in-process in the tab. The query runs in wasm; the result snapshot crosses into JS and a
  `FlatArrayView` folds the incremental change stream.
- **[TanStack DB](https://tanstack.com/db)** — a pure-TypeScript reactive store whose live
  queries are maintained by differential dataflow (`d2ts`).

Both keep a query's result **live**: you materialize a query once and the store maintains it
as the data changes, computing the incremental difference per write instead of re-running the
query. This benchmark measures the two things that matters for that contract over a large-ish
dataset and a ladder of simple → complex queries:

1. **Hydrate from scratch** — build the query and materialize its full result over an
   already-loaded dataset (the cold first paint).
2. **Incremental update** — with the view materialized, apply one write and let the store
   maintain the result (the steady-state hot path).

## Running it

```sh
pnpm install            # at the repo root (single pnpm workspace)
pnpm run build:wasm                          # once — builds packages/wasm/pkg (needs Rust + wasm-bindgen)

SCALE=large pnpm --filter @rindle/bench-tanstack bench
```

The bench resolves `@rindle/wasm` from its TypeScript source via the `@rindle/source` export
condition (Node strips the types), so no `dist/` build is needed — only the wasm artifact.

Knobs (env):

| Var | Default | Meaning |
|---|---|---|
| `SCALE` | `medium` | `small` · `medium` · `large` · `xl`, or `"users,issues,comments"` |
| `ROUNDS` | `4` | hydrate rounds (the reported time is the **min**) |
| `PAIRS` | `25` | incremental add+remove pairs per round |
| `IROUNDS` | `3` | incremental rounds (min per-pair is reported) |

| Scale | users | issues | comments | rows |
|---|--:|--:|--:|--:|
| small | 100 | 1,000 | 5,000 | 6,100 |
| medium | 300 | 5,000 | 25,000 | 30,300 |
| large | 1,000 | 10,000 | 50,000 | 61,000 |
| xl | 2,000 | 20,000 | 100,000 | 122,000 |

Each run prints a table and writes [`RESULTS.md`](RESULTS.md). **Numbers are machine-specific
— re-run locally; do not quote the committed `RESULTS.md` as authoritative.**

## The dataset

An issue-tracker shape (the same shape the in-repo `bench_graph_sources` benches use), scaled
up: `user`, `issue (creatorID → user)`, `comment (issueID → issue, creatorID → user)`. Integer
keys throughout, so neither engine pays a string-comparison tax the other avoids — the variable
under test is the store, not the key shape. ~1/3 of issues are closed; `created` descends with
id (a well-defined "newest"); comments fan out evenly across issues. The dataset is generated
once and handed to both engines byte-for-byte.

## The two ladders

Each query is expressed as the **same logical query** on both engines (not necessarily the same
output bytes — e.g. a Rindle `.sub()` nests an array of children, the matching TanStack idiom is
a `toArray()` include). The driver checks that the top-level row count and a secondary invariant
(total children / summed aggregate) agree across engines before trusting a row (✅ in the table).

### Realistic UI views — bounded result (the representative workload)

**A client app does not pull a table into the host language; it queries enough to fill a view.**
Every query here is bounded to a viewport (`VIEW_LIMIT = 50` top-level rows) over the *same large
store*, so only the viewport crosses the wasm boundary — the shape real client queries actually
have. This is the section that matters.

| Query | Rindle | TanStack DB |
|---|---|---|
| list: newest 50 open | `.where.open(true).orderBy("created","desc").limit(50)` | `.where(…).orderBy(…,'desc').limit(50)` |
| list + author | `…limit(50).sub("creator", user, …)` | `…limit(50).select({ creator: toArray(from(users)…) })` |
| list + comment count | `…limit(50).countAs("commentCount", comment, …)` | limit-50 subquery → `leftJoin(comments).groupBy.count` |
| list + 3 recent comments | `…limit(50).sub("comments", …, c => c.orderBy("id","desc").limit(3))` | `…limit(50).select({ comments: toArray(…orderBy.limit(3)) })` |
| issue detail + comments | `.where.id(X).limit(1).sub("comments", …)` | `.where(eq(id, X)).select({ comments: toArray(…) })` |
| list: page 2 | `…orderBy(…).start({created: cursor}, {exclusive}).limit(50)` | `…orderBy(…).limit(50).offset(50)` |

### Full materialization — whole result into JS (engine characterization)

These pull the entire result set across the boundary. Useful to characterize the engine, but
**not** how a client app queries — kept as a contrast, not the headline.

| Query | Rindle | TanStack DB |
|---|---|---|
| scan all issues | `query.issue` | `from({issue})` |
| filter open | `.where.open(true)` | `.where(eq(issue.open, true))` |
| filter + order + limit 50 | `.where.open(true).orderBy("created","desc").limit(50)` | `.where(…).orderBy(…,'desc').limit(50)` |
| issue → comments[] | `.sub("comments", comment, …)` | `.select({ comments: toArray(from(comments)…) })` |
| issue → creator | `.sub("creator", user, …)` | `.select({ creator: toArray(from(users)…) })` |
| issue → comments → creator | nested `.sub(...).sub(...)` | nested `toArray` includes |
| issue → commentCount | `.countAs("commentCount", comment, …)` | `leftJoin(comments).groupBy(issue.id).select(count)` |

The incremental op for each query is a **reversible add+remove pair** that actually flows
through that query — a new newest-open issue for the list windows (it lands at the top of the
viewport), a new comment on a *visible* issue for the count / preview / detail views, a new top
issue for the scans, a new comment on a shown issue for the relationship/aggregate views — so
steady state holds across thousands of iterations and the dataset never grows.

## Fairness notes — read these before trusting a number

- **Both engines are indexed for the workload.** Rindle's engine maintains the indexes its
  correlations and sorts need; TanStack otherwise logs *"Join requires an index … falling back
  to loading all data"*, so the bench gives it the matching `BasicIndex` (equality probes) and
  `BTreeIndex` (the ordered top-K). Comparing indexed-to-indexed is the fair comparison.
- **Seeding is not timed, and uses each engine's bulk path.** Rindle loads the snapshot in one
  write transaction; TanStack loads via `localOnlyCollectionOptions({ initialData })` (the
  authoritative `begin/write*/commit` sync path — O(n)). TanStack's per-row `.insert()` is
  ~45× slower and is used **only** in the incremental probe, where a single mutation is exactly
  what we mean to measure.
- **Hydrate measures build + materialize + a full read** of the result (every cell and child is
  touched, inside the timed region, so neither engine can defer projection past the clock and
  dead-code elimination can't drop it). Each round builds and disposes a **fresh** view, so we
  never measure a warm cache. The reported time is the **min** over rounds (the most
  noise-robust estimator — it strips GC pauses and scheduler jitter).
- **Incremental measures the maintenance**, not re-projection: with the view live, one write
  propagates through the dataflow. Propagation is **synchronous** on both engines (verified), so
  the add+remove pair is timed directly. Note Rindle's `FlatArrayView` rebuilds its projected
  `.data` array on *every* write (its subscriber contract), so the Rindle incremental number is,
  if anything, **conservative** — it does strictly more per write than reading TanStack's O(1)
  `.size`.
- **The wasm boundary is real and is included.** Rindle's hydrate snapshot crosses
  `serde-wasm-bindgen` from wasm into JS row by row; TanStack is pure JS. Expect that cost to
  show up on **simple bulk hydration** (a large unfiltered scan), where there is little engine
  work to amortize it against — and to be repaid on the **incremental** path and on
  **complex-query** hydration, where the engine work dominates the boundary.

## Reading the result

- **On the realistic (bounded) workload, Rindle wins both paths across the board** — hydrate
  *and* incremental. Because the result is viewport-sized, only ~50 rows ever cross the wasm
  boundary, so the per-row serialization cost is negligible and the engine's efficiency shows
  through on hydration too; the incremental margins are large and grow with query complexity.
  This is the section that reflects how a client app actually queries.
- **On full materialization the hydrate path splits** — simple large scans favor the pure-JS
  store (Rindle pays `serde-wasm-bindgen` per row to ship the whole table into JS), while
  relationship, top-K, and aggregate hydration still favor the engine. But pulling 10k rows
  into the host language is not a real client query shape; the split is an artifact of asking
  for more data than a UI would. Incremental stays a decisive Rindle win regardless.

The takeaway: query the store the way an app does — bounded to a view — and the wasm store is
faster on both the first paint and every update.
