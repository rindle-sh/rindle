# Rindle Issue Tracker

The real-world demo of Rindle's one topology — three tiers, real wires:

```text
┌────────────┐  HTTP /api/rindle/*   ┌────────────────┐  writes  ┌──────────────────┐
│ browser    │──────────────────────►│ API authority  │─────────►│ HCTree master     │
│ wasm IVM   │                       │ serverless-     │  reads   │ rindle-replicator│
│ optimistic │◄──── follower ws ─────│ shaped         │─────────►│       │ journal    │
└────────────┘   leases + deltas      └────────────────┘          └───────┼────────────┘
                                                                          ▼
                                                                  ┌───────────────┐
                                                                  │ rindled       │
                                                                  │ read follower │
                                                                  └───────────────┘
```

The browser app is a **TanStack Start** SPA: file-based routes in `src/routes` (`/` list · `/board` ·
`/activity`), with the open issue + the create pane carried in URL search params (`?issue=`/`?new=`),
so every view + selection is a shareable deep link. It is deliberately **client-rendered** — the
in-browser IVM engine (wasm) *is* the data layer, so Start prerenders a static shell and the engine
boots in the browser (`src/RindleApp.tsx`); the SSR/prerender pass never touches wasm.

- the **browser** runs the optimistic engine: mutations apply instantly, ship through the
  client queue as confirmed in-order batches, and rebase on confirmations;
- the **API server** is the app authority — it resolves named queries to ASTs (the daemon
  mints opaque leases), runs the authoritative mutators into approved SQL, and enforces
  policy (try a title with the word "spam" → rejection toast + snap-back; try deleting
  someone else's issue → accepted-but-no-op, the row snaps back);
- the **data tier** is a `rindle-replicator` HCTree write-master plus a read-only `rindled`
  follower. The master accepts concurrent writes and commits them into one journal order; the
  follower holds the live IVM pipelines and streams normalized updates to every subscriber.

## Fragments: co-located selections that compose into one query

The data is normalized across four tables — `user`, `issue`, `tag` (one row per tag applied
to an issue), and `comment` (the earliest doubles as the issue's description) — defined **SQL-first**
in `migrations/0001_init.sql`, generated into `shared/schema.gen.ts`, and re-exported by
`shared/app-def.ts` (which layers on the relationships, faceted filter, and mutators); they're joined
back together at query time with correlated subqueries (`sub`).

Each UI component declares the columns + relationships **it** renders as a `defineFragment` — a
reusable, typed selection over one table (Relay's fragment, as a first-class value). A parent
spreads its children's fragments into its own selection, so composing them assembles **one** query
AST → one daemon materialization → one `/query` — no per-component request waterfall. The root
component calls `useRoot(namedQuery, args, RootFragment)` to retain the composed coverage query and
receive opaque root refs for row components, or `useRoot(namedQuery, args)` when the route owns the
root data directly; both return `[data, details]`. Descendants call `useFragment(Fragment, ref)` to
read narrow local data for the fragment they own:

```ts
// leaves — co-located with the components that render them
export const UserBadge   = defineFragment(user, (u) => u.select("id", "name"));
export const TagChip     = defineFragment(tag,  (t) => t.select("id", "name"));
export const CommentCard = defineFragment(comment, (c) =>
  c.select("id", "authorId", "body", "createdAt")
   .sub("author", user, { parent: ["authorId"], child: ["id"] }, UserBadge));

// a list row / board card: owner + tags + an accurate scalar comment COUNT (not the rows)
export const IssueCard = defineFragment(issue, (i) =>
  i.select("id", "title", "status", "priority", "ownerId", "updatedAt")
   .sub("owner", user, …, UserBadge)
   .sub("tags",  tag,  …, (t) => TagChip(t).orderBy("name", "asc"))
   .countAs("commentCount", comment, { parent: ["id"], child: ["issueId"] }));
```

The payoff is **reuse across roots**: `UserBadge` is an issue's owner *and* a comment's author;
`CommentCard` is a row in the detail thread *and* a row in the activity feed; `IssueCard` is a list
row *and* a board card. Write the data contract once, render it in N places — each view stays a
single round-trip. Four views, four roots, the same fragments:

| View | Root query | Fragments |
| --- | --- | --- |
| **List** | `issuesPage({ limit, filter })` — a growing window | `IssueCard` |
| **Board** | `issuesPage` × 5, each + a `status:` facet | `IssueCard` (reused) |
| **Detail** | `issueDetail(id)` — one issue, the whole thread | `IssueDetailCard` → `CommentCard` |
| **Activity** | `recentComments({ limit })` — newest comments, all issues | `FeedItem` → `CommentCard` (reused) |

Each fragment `select`s exactly what it displays — column-level **projection** that flows through
the optimistic client: the daemon syncs only those columns (plus the order/correlation keys it needs
internally, preserved automatically), and the client scatters them into its shared store, leaving
un-selected columns `Absent`. So `IssueDetailCard` (no `createdAt`/`updatedAt`) genuinely syncs a
narrower issue row than `IssueCard`. The TS fragment data type is masked to the fragment's selected
columns, and child fragment relationships surface as refs rather than child-owned payload.

Row types are derived from the fragments (`type IssueCardRef = FragmentRef<typeof IssueCard>`), and
the mutators are per-row across the touched tables (`createIssue` writes the issue + its description
comment + its tag rows; `addTag`/`removeTag`, `addComment`/`editComment` …) — the granularity
normalized data wants, and the only shape the client's optimistic `MutationTx` can express (it
writes by key, it can't scan a table to replace a whole set).

The schema (`migrations/0001_init.sql`) creates each table **with the indices that keep the `sub`
joins fast in both directions** — the forward fetch (an issue's comments/tags) and the reverse
lookup (a changed child re-finding its parents): `comment(issueId, createdAt)`, `tag(issueId, name)`
and `tag(name)`, `issue(ownerId)`, plus `issue(createdAt DESC, id)` for the window seek. It is applied
through the master's `/migrate` — automatically by `rindle up --migrate --gen` in dev (`pnpm dev`),
which also regenerates `shared/schema.gen.ts` from the introspected result — or with
`pnpm migrate:local` for a manual local run. Remote/CI runs use `pnpm migrate` with an explicit
`RINDLE_REPLICATOR_URL`, which is passed directly to the CLI. It is never created on boot: the
daemon introspects whatever the file holds, so a column change is a forward migration rather than
the old `createSql`-every-boot blast + data-wiping reset.

The issue table is **big** — the master is seeded with 5,000 issues at boot (one
idempotency-keyed bulk SqlTxn through the write ingress, so the file-backed Rust data tier
never re-seeds). There is deliberately no "all issues" query: the client holds paginated
**live windows** (`issuesPage(cursor)`, `limit(50)` + cursor `start`), accumulated as you
scroll (infinite scroll, with a "Load more" fallback). New issues enter the top window
incrementally — IVM keeps every window live,
so a window stays a window (the 50th row falls out when a new one enters, and backfills
after a delete).

## Run it

```bash
# once, from the repo root — install the workspace dependencies:
#   pnpm install

cd apps/example-issue-tracker
pnpm dev          # runs `rindle up`, applies migrations + regenerates the schema + seeds,
                  # then boots the TanStack Start app (Ctrl-C tears it down)
pnpm dev:swarm    # same as dev, plus a swarm of bots hammering the tracker + the footprint bar
```

`pnpm dev` first builds the workspace's `rindle`, `rindled`, and `rindle-replicator` binaries plus
the browser WASM engine, then runs `concurrently` around the Rindle CLI and web tier. The preflight
keeps the generated runtimes in lockstep with their Rust and TypeScript sources. The data-tier side runs `pnpm rindle up --migrate --gen
shared/schema.gen.ts --watch` against `rindle.ncl`: it supervises the write-master/follower pair plus stable fleet edge,
applies `migrations/` at the master, and regenerates `shared/schema.gen.ts` from the follower. The web
side runs the idempotent seed before `vite dev`, so a fresh DB comes up populated in one command.
`pnpm migrate:local`, remote/CI `pnpm migrate`, and `pnpm seed` remain available for manual use.

Open the printed URL in **two browser windows** and watch edits sync live. Each window's
"user" decides ownership (deletes only work on your own issues). Toggle **List / Board /
Activity** in the toolbar — three views over the same data, each a different root query reusing
the same co-located fragments.

## The load test: a swarm of bots + the live footprint bar

`pnpm dev:swarm` adds a **fourth tier** — a bot swarm (`server/swarm.ts`) that hammers the live
tracker so you can watch it move in realtime, plus the **footprint bar** that shows what the one
box absorbing the storm is actually doing. It's the issue-tracker twin of the wiki demo's ingester:
there one source replays Wikimedia; here many virtual bots drive the **same authoritative path a
browser hits** — `createIssueApi(...).pushMutation`, so every write runs the real mutators **and
policy** (the `spam` rejection, the owner-gated delete), the master admits concurrent transactions,
and the follower's IVM does genuine work.

```text
  swarm (Node authority)        HCTree master       rindled follower       viewers
 ┌───────────────────────┐     ┌──────────────┐    ┌──────────────────┐   ┌─────────┐
 │ N concurrent bots     │────►│ concurrent   │───►│ live windows kept│──►│ browser │ × N
 │ + /metrics            │     │ write ingress│    │ one shared query │──►│ ws bot  │ × M
 └───────────┬───────────┘     └──────────────┘    └──────────────────┘   └─────────┘
             └──────── footprint bar polls /metrics ────────────────────────────┘
```

A human who opens the tracker is subscribed to the live top-of-window and watches the bots' issues
stream in, statuses flip, comments land, and rows backfill on delete — no refetch, nothing to
invalidate. The swarm is **colocated with `rindled`**, so its `/metrics` reads the **whole-VM** CPU
from `/proc` and the resident memory of both itself and the daemon — the honest "tiny footprint
under a write storm" the bar shows (CPU% · RAM / limit · ≈ $/mo), alongside the load side
(bots · mutations/sec). The bar (`src/components/MetricsBar.tsx`) renders **only** when the swarm's
`/metrics` answers, so a plain `pnpm dev` shows no bar. The `machine-stats.ts` / `pricing.ts` helpers
are shared verbatim with `apps/wiki-demo`.

**The bots are viewers too.** Writers aren't the whole load — `SWARM_SUBSCRIBERS` of the bots also
attach as **live readers**: each leases the same window in-process and opens a raw ws to the daemon's
public port (no wasm engine, so thousands cost almost nothing here), then drains the fan-out. They
all watch the **same** window, so they **share one materialization** — the dedup story made literal:
the bar's **watching** count (real ws connections on the daemon, subscriber bots + human tabs) scales
into the thousands while **live queries** stays flat. The viewers count is daemon truth: `rindled`'s
`/stats` reports its live public ws `connections` (and `subscriptions`).

Run the swarm standalone against an already-running daemon with `pnpm swarm`. It is **tunable**:

| env | default | meaning |
| --- | --- | --- |
| `SWARM_BOTS` | `2000` | headline virtual writers (each its own mutation stream + own issues) |
| `SWARM_RATE` | `150` | aggregate mutations / second (a token bucket paces it) |
| `SWARM_SUBSCRIBERS` | `500` | lightweight ws viewers — lease + watch the live window (the dedup demo) |
| `SWARM_USERS` | `64` | author pool — kept small so bots don't bloat the (whole-table) user picker |
| `SWARM_MAX_ISSUES` | `4000` | live bot-corpus cap; creates yield to deletes past it (bounded) |
| `SWARM_CONCURRENCY` | `64` | max in-flight mutations (a slow daemon sheds load, not piles up) |
| `SWARM_SPAM_RATE` | `0.004` | share of creates that trip the policy, to exercise rejection under load |
| `SWARM_METRICS_PORT` | `7711` | the `/metrics` + `/version` HTTP port the footprint bar polls |

The frontend polls `VITE_METRICS_URL` when it is set. `pnpm dev:swarm` sets it to
`http://127.0.0.1:7711/metrics`; production builds leave it unset unless a public swarm metrics tier
is deployed.

Headless proof of the swarm wiring (bots write the authoritative path, viewer bots lease + watch
over ws, the live window stays a window under the storm, `/metrics` + the daemon's viewer count
report): `pnpm smoke:swarm`.

## Hosting

The example is local-first. A managed data plane is created through Headwaters and exposes one
`app-<id>.rindle.cloud` edge URL; a separately deployed API/SSR tier can use that URL for both
`DAEMON_ORIGIN` and `REPLICATOR_ORIGIN`, plus `VITE_FLEET_WS` for browser subscriptions.

## Where to look

| Concern | File |
| --- | --- |
| schema (generated SQL-first) + fragments + named queries (list/board/detail/feed) + PREDICTED mutators | `shared/schema.gen.ts`, `shared/app-def.ts` |
| the co-located fragment leaves (`useFragment` readers) | `src/components/{UserBadge,TagChip,CommentCard}.tsx` |
| the list / board / activity views (each a root query reusing the fragments) | `src/components/{IssueList,KanbanBoard,ActivityFeed}.tsx` |
| AUTHORITATIVE mutators + policy + auth + cursor validation (API tier) | `server/app-api.ts`, `src/routes/api.rindle.*.tsx` |
| schema migrations (applied on demand, `wrangler`-style) + the `pnpm migrate` command | `migrations/`, `package.json` scripts |
| idempotent bulk seeding through the control plane | `server/seed.ts` |
| the one-call client wire-up (client-only `bootClient`) | `src/rindle-client.ts` (`createRindleClient`) |
| TanStack Start shell + routes (deep-linked views/selection) | `src/routes/{__root,index,board,activity}.tsx` |
| the persistent frame + page accumulation over COW views | `src/AppChrome.tsx` (the shared live window) |
| one topology + dev orchestration | `rindle.ncl`, `package.json` scripts |
| the bot swarm load test + `/metrics` footprint tier | `server/swarm.ts` |
| whole-VM CPU/RAM sampler + monthly-cost estimate (shared with wiki-demo) | `server/machine-stats.ts`, `server/pricing.ts` |
| the live footprint bar (CPU · RAM · $/mo · bots · mutations/sec) | `src/components/MetricsBar.tsx` |

Headless proof of the whole wiring (CI-able): `pnpm smoke`.
