<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/client.md (https://rindle.sh/docs/client)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# The browser client

> createRindleClient gives the browser its own IVM engine over a local database — reads resolve instantly, writes apply optimistically and rebase, and rejections snap back on their own.

The browser tier runs its **own** IVM engine (`@rindle/wasm`) over its **own**
local database, fed by a stream of normalized row deltas from the
[daemon](https://rindle.sh/docs/daemon). It doesn't render a result someone else computed — it
*computes its own*, against rows it holds locally.

That one move solves three things app developers otherwise wire up by hand, badly:

- **Live queries** — a materialized view stays correct as data changes, with no
  polling and no refetch.
- **The cache** — there is no separate cache to invalidate. The local database
  *is* the cache, and the engine keeps every view derived from it exact.
- **Optimistic updates** — a named mutator applies to the local tables
  *synchronously*; the local engine re-materializes every affected view before the
  call returns. The server is the authority; the engine rebases your pending
  writes as confirmations stream in, and a rejected write snaps back on its own —
  you never write rollback code.

## One call wires the whole tier

`createRindleClient` boots the wasm engine, resolves query leases through your
[API server](https://rindle.sh/docs/api-server), opens the returned WebSocket endpoint with its affinity ticket, and
runs the mutation queue. It returns the `Store`, the optimistic backend, and the `mutate` entry
point.

```ts
import { createRindleClient } from "@rindle/optimistic";
import { initWasm } from "@rindle/wasm";
import { mutators, schema } from "../shared/app-def.ts";

// Optional: point the engine at a custom-served wasm asset before the client's own
// (idempotent) init runs. Omit this and createRindleClient boots the default wasm.
// (`?url` is Vite's asset-URL import; the package exports the asset at ./pkg/*.)
import wasmUrl from "@rindle/wasm/pkg/rindle_bg.wasm?url";
await initWasm(wasmUrl);

export const app = await createRindleClient({
  schema,
  mutators,
  user: () => currentUser(),                       // the acting principal a mutator sees as ctx.user
  api: {
    url: "",                                       // same-origin; or "https://api.example.com"
    headers: () => ({ "x-user": currentUser() }),  // a real app sends a session/JWT
  },
  onRejected: (envelope, reason) => showToast(`${envelope.name} rejected: ${reason}`),
});
```

The options in full:

- **`schema`** — the browser schema. Usually this is the schema [generated from your SQL](https://rindle.sh/docs/schema) with
  `rindle schema gen`; if you have local-only UI tables, pass `extendSchema(generatedSchema, { tables: [...] })`.
- **`mutators`** — your **isomorphic** mutators (below). The client drives each body
  synchronously as the optimistic prediction; the [API server](https://rindle.sh/docs/api-server)
  drives the *same* body as the authority.
- **`user`** — `() => string`, the local identity injected as a mutator's `ctx.user`
  (re-read on every invoke, so it's stable across a rebase re-invoke). Match it to
  whatever `api.headers` sends up; the server injects its **own** authenticated
  principal for the authoritative run.
- **`api`** — where named queries resolve to leases and mutations are pushed:
  `url`, optional `routes`, and `headers` (an object, or a function re-evaluated
  per request — put your session/JWT here).
- **`daemon`** — optional transport override. Normal apps omit it: the first query lease carries the
  public `wsEndpoint` + placement ticket and opens the socket lazily. Pass `{ wsUrl }` for a fixed
  endpoint or `{ transport }` for an in-process/test transport.
- **`onRejected(envelope, reason)`** — fired when the API server refuses a
  mutation; the optimistic prediction has already snapped back by the time you see
  it. Use it to surface a message.
- **`clientID`** — a stable identity (defaults to a localStorage-persisted UUID, so
  a reload keeps its mutation sequence).

It returns `{ store, backend, mutate, flushFolds, clientID, close }`.

> **Server-rendering?** Never construct the wasm engine during SSR/prerender —
> lazily `import("@rindle/optimistic")` + `import("@rindle/wasm")` on the client
> and memoize the boot promise. See [Server rendering](https://rindle.sh/docs/ssr);
> [`create-rindle`](https://rindle.sh/docs/create-rindle) ships the deferred-boot pattern in
> `src/rindle-client.ts`.

## Isomorphic mutators

A write is a call to a **named mutator** — written **once** and run on **both**
tiers. A mutator is a **generator** that `yield`s logical write ops
(`yield tx.insert(...)`) instead of touching a database directly; the client
drives the body **synchronously** against the local tables — the optimistic
prediction, **re-invoked on every rebase**, so it must not read clocks or
randomness (pass those in as args) — and the [API server](https://rindle.sh/docs/api-server)
drives the *same* body inside an authoritative transaction, rendering each op to
SQL. The acting user is never an argument — each tier injects it as `ctx.user`
(the client's `user` option here; the server's authenticated principal there), so
it can't be spoofed over the wire.

```ts
import { defineMutators } from "@rindle/client";
import type { MutationGen } from "@rindle/client";
import type { ClientRegistry } from "@rindle/optimistic";
import { z } from "zod";
import { schema } from "./schema.gen.ts";

const { shared } = defineMutators(schema); // ops typecheck against the schema

export const mutators = {
  // update names only the pk + the columns that change; a missing row is a no-op
  setStatus: shared(
    z.object({ id: z.string(), status: z.string(), updatedAt: z.number() }),
    function* (tx, a): MutationGen {
      yield tx.update("issue", { id: a.id, status: a.status, updatedAt: a.updatedAt });
    },
  ),
} satisfies ClientRegistry;
```

The full contract — the op vocabulary (`insert` / `update` / `upsert` /
`insertIgnore` / `delete`), reads inside a body (`yield tx.row` /
`tx.query` / `tx.all`), the `ctx.user` principal, and the determinism rules — is
[Isomorphic mutators](https://rindle.sh/docs/mutators). Everything on the rest of this page (rebase,
folding, rejection) applies to any mutator you write there.

## Reads: live views

### With React

Wrap the tree in `<Rindle store={app.store}>` and read a query with `useQuery`. The
hook returns the live `.data` and re-renders only when the result changes — views
are reference-stable, so memoized components stay put.

```tsx
import { Rindle, useQuery } from "@rindle/react";
import { issuesPageQuery } from "./components/IssueListItem.queries.ts";
import { app } from "./rindle-client.ts";

createRoot(root).render(
  <Rindle store={app.store}>
    <App />
  </Rindle>,
);

function IssueList() {
  const rows = useQuery(issuesPageQuery({ limit: 50 })); // readonly Issue[], live
  return <ul>{rows.map((r) => <li key={r.id}>{r.title}</li>)}</ul>;
}
```

When a whole component *tree* needs data, don't give each component its own
`useQuery` — that's a request waterfall. Declare each component's slice as a
**fragment**, compose them into one named root query, open that root with
`useRoot`, and let descendants read opaque refs with `useFragment`. See
[Compose the UI with fragments](https://rindle.sh/docs/fragments).

#### Typeahead: opt out of the warm window

When the last component reading a query unmounts, the view and its server
subscription are kept warm for **2 seconds** rather than dropped immediately. That
grace window is what stops navigation from flashing empty: change a filter or a
limit and the replacement re-materializes from the still-warm local base while its
server lease streams the first answer.

It's the wrong default for a query you know you'll never return to. Typeahead is
the canonical case — `sea`, `sear`, `searc`, `search` are four *different* queries,
so a 2s window leaves one dead view and one open subscription per character typed.
Pass `releaseDelayMs: 0` to tear down on unmount:

```tsx
function SearchResults({ term }: { term: string }) {
  const rows = useQuery(searchIssues({ term }), { releaseDelayMs: 0 });
  return <ul>{rows.map((r) => <li key={r.id}>{r.title}</li>)}</ul>;
}
```

The option is available on `useQuery`, `useQueryStatus`, `useSyncQuery`,
`useFragment`, and `<Frag releaseDelayMs={0}>`. `useRoot` takes the tree default —
its argument list is variadic (`query, args, ...ctx, fragment`), so a trailing
options object would be ambiguous.

To move the default for a whole tree, set it on the provider — treat it as a
constant, since changing it rebuilds the caches and tears down every live view:

```tsx
<Rindle store={app.store} releaseDelayMs={0}>
```

For a query several components share with different delays, the window is a
**deadline, not a duration**: every unmount stamps `now + that component's delay`,
and the query stays warm until the latest deadline any of its readers asked for.
Two things follow, both of them what you'd want:

- **The clock starts when a reader leaves, never when it arrives.** A component
  that stays mounted for an hour is never timed out, and when it does unmount it
  gets its full window from that moment.
- **A deadline expires on its own**, so a later reader inherits at most what is
  *left* of an older window, never a fresh copy of it. Unmount a 2s reader, remount
  a `releaseDelayMs: 0` one 1.9s later and drop it: teardown lands at the original
  2s mark, not 1.9s past it.

So `releaseDelayMs: 0` means "I'm asking for no window of my own" rather than "tear
down now regardless" — a still-unexpired deadline from a component sharing the same
query still applies. For the typeahead case that distinction never arises, because
every keystroke is a distinct query with no shared history.

### Without React

`app.store.materialize(query)` returns an `ArrayView` whose `.data` is the current
result; `subscribe` fires now and after every change.

```ts
const view = app.store.materialize(issuesPageQuery({ limit: 50 }));
view.subscribe((rows) => render(rows));   // fires immediately, then on every change
```

Wire that into whatever reactivity your framework has — the store core is
framework-agnostic:

- **Vue** — `subscribe` into a `ref`/`shallowRef`, unsubscribe in `onUnmounted`.
- **Svelte** — wrap in a `readable(store)` (the callback returns the unsubscriber).
- **Solid** — `createSignal` + `onCleanup(unsub)`.
- **Vanilla** — call your DOM render function from the subscription.

`app.mutate.<name>(args)` is the same everywhere — call it from any event handler.

> Remote subscriptions must be **named** — define each one with `defineQuery` and
> call the value (`issuesPageQuery(args)`); it stamps the result with its wire
> identity, so it always **syncs**. An ad-hoc `app.store.query.issue.where…` builder
> query is resolved **locally only**, off rows already synced; it never opens a
> server subscription. (That local resolution is a feature — see below.) Because a
> query is defined once and *callable*, there is no unstamped builder to import by
> accident: every named query lives next to its component as a `defineQuery`.


## Local-only tables: drafts, selections, prefs

Not every row belongs to the server. Selection state, draft text, expanded/collapsed rows,
and view preferences often need the same reactive query machinery as synced rows, but they
should be **client-authoritative** and private. Declare those tables in the shared client
schema with `{ local: true }`:

```ts
import { createSchema, number, string, table } from "@rindle/client";

export const issue = table("issue")
  .columns({ id: number(), title: string() })
  .primaryKey("id");

export const draft = table("draft", { local: true })
  .columns({ id: string(), issueId: number(), body: string() })
  .primaryKey("id");

export const schema = createSchema({ tables: [issue, draft] });
```

A local-only table never crosses the wire: it is omitted from the normalized schema the
client advertises to the server, it is not tracked by the daemon, and an optimistic rebase
never rewinds it. That makes it the right place for UI-only state that should survive
server confirmations/rejections unchanged.

Use `store.writeLocal` for those direct client writes:

```ts
await app.store.writeLocal((tx) =>
  tx.add("draft", { id: "compose", issueId: 42, body: "half-written reply" }),
);
```

Two guardrails keep private and synced state separate:

- `writeLocal` accepts **only** local-only tables; writing a synced table there is rejected.
- Mutators cannot read or write local-only tables. A mutator is replayed on the
  server and during rebase from `(name, args)` alone, so it must not depend on private
  browser rows.

`app.store.query` can still materialize local-only tables and compose them with synced rows
locally. Named server queries cannot: `newQueryBuilder(schema)` excludes local-only tables
so your remote contract never accidentally depends on private UI state.

When your synced schema is generated, keep it generated. Put local tables in a small
hand-authored module and combine them with `extendSchema`:

```ts
// shared/schema.local.ts
import { extendSchema, string, table } from "@rindle/client";
import { schema as generatedSchema } from "./schema.gen.ts";

export const composerDraft = table("composerDraft", { local: true })
  .columns({ id: string(), body: string() })
  .primaryKey("id");

export const clientSchema = extendSchema(generatedSchema, { tables: [composerDraft] });
```

Hand `clientSchema` to `createRindleClient` in the browser. Your API server and named-query
registry should continue to use the generated synced schema; `extendSchema` refuses ordinary
non-local tables, so server-owned tables still come only from `rindle schema gen`.

## Writes: optimistic, rebased

`app.mutate.<name>(args)` drives a mutator's body against the local tables
**synchronously** — every affected view updates before the call returns — and
returns the mutation id. No spinner, no round-trip, nothing to `await`:

```ts
app.mutate.createIssue({ id, title: "ship it", status: "todo", /* … */ createdAt: Date.now() });
// the issue list already shows it; the API server confirms moments later.
```

Under the hood the mutation's **name and arguments** (never its effects) are queued
and pushed to the API server, which drives the **same body** in an authoritative
transaction. The confirmed deltas stream back and the client **rebases**: it rewinds
to the authoritative state, re-invokes every still-pending mutator on top, and the
engine derives exactly the view delta that resolves the difference.

Re-invocation is what makes **read-dependent** writes correct. A mutator that reads
before it writes recomputes against the authoritative data on every rebase — so it
replays the *intent*, not a stale effect:

```ts
bumpPriority: shared(z.object({ id: z.string(), delta: z.number() }), function* (tx, a): MutationGen {
  const cur = (yield tx.row("issue", { id: a.id })) as { priority: number } | undefined; // base + this txn's writes
  if (cur) yield tx.update("issue", { id: a.id, priority: cur.priority + a.delta });
}),
// predicts against a local value; if the server lands a concurrent change first,
// the rebase re-runs the +delta against the authoritative value and settles correctly.
```

## Folded writes: high-frequency drags

Some writes fire *tens of times a second* — dragging a slider, scrubbing a timeline,
a field that persists on every keystroke. The local view has to track every
intermediate instantly, but the server only ever needs the **last** value. Sent as
plain `mutate.<name>(args)` calls, sixty drag events become sixty pending entries and
sixty server writes — and every rebase re-invokes all sixty.

`app.mutate.<name>.folded(opts, args)` collapses a run of same-key calls into **one**
pending entry whose args are overwritten in place: the local view still updates on
**every** call, but the server write is debounced and ships only the last value.

```ts
function onSliderDrag(id: string, value: number) {
  // setScore(id, value) is an ordinary mutator; `.folded` just changes how it's queued.
  app.mutate.setScore.folded({ key: id, debounceMs: 120, maxWaitMs: 1000 }, { id, value });
  // the local view tracks every frame; one server write lands once the drag settles.
}
```

- **`key`** *(required)* — the identity the run folds on. The fold key is `(mutator
  name, key)`, so two sliders bound to two rows fold **independently**, while the same
  slider dragged twice folds into one entry. Use the row's primary key.
- **`debounceMs`** *(default 120)* — the server write fires this long after the
  **last** call for `key`.
- **`maxWaitMs`** — a hard cap so a never-idle drag still persists periodically.
  Without it, only an idle gap of `debounceMs` ever flushes.
- **`deferAcrossWrites`** *(default `false`)* — by default, an overlapping ordinary
  write flushes the fold first, so a read-dependent write never sees a value the
  server hasn't been told about yet. Set `true` only for an isolated leaf cell, to
  keep deferring for maximum economy.

`.folded()` returns a small handle rather than a mutation id (none is assigned until
the write flushes): `{ flush(), mid }`, where `mid` is a `Promise<number>` that
resolves with the wire id once the window flushes — `await` it when you need the ack.

```ts
const h = app.mutate.setScore.folded({ key: id }, { id, value });
h.flush();                  // force the debounced write out right now
const wireId = await h.mid; // resolves once it flushes
```

`app.flushFolds()` drains every outstanding fold immediately, and the client also
flushes automatically on `pagehide` / `beforeunload`, so a fold in flight isn't lost
on navigation. (Two costs are inherent to debouncing the write: an unclean crash
mid-drag loses the un-shipped tail, and other clients see the new value a beat later.
Every *non-folded* write still flushes as soon as possible.)

> **Folded mutators must be absorbing** — replaying only the last args must yield the
> same state as replaying all of them (`setScore(8)` after `setScore(5)` is just `8`).
> An `increment()`-style mutator is **not** absorbing and must not be folded; the
> folded path refuses a mutator that reads state with `yield tx.row` / `tx.query` (the
> classic non-absorbing shape) by throwing.

## Rejection snaps back on its own

If the authoritative mutator throws, nothing commits; the rejection rides back on
the stream and the client rebases **without** the refused mutation. The phantom
rows vanish from every affected view — there is no rollback code, because the
authoritative state never saw the write. `onRejected` fires so you can show a
message.

## Loading and pending are separate signals

A query carries a `ResultType` that tracks **the server channel only** — has this
query been answered yet?

```ts
app.backend.resultType(queryId);              // "unknown" (loading) → "complete"
app.backend.onResultType((qid, rt) => …);     // or subscribe to transitions
```

`"unknown"` means *not hydrated* — the server hasn't produced a first result yet —
and `"complete"` means it has. A pending optimistic write **does not** move it: the
prediction is your best current answer, so the query stays `"complete"` through the
write, and a rejection is an *event* (`onRejected`), not a downgrade. (`"error"` is
reserved for a future server-side, query-level error and is currently unproduced.)
With React, `useQueryStatus(query)` returns this same value.

"Is an unconfirmed write pending here?" is a **separate** axis — drive a "saving…"
affordance off it and it clears itself once the server catches up:

```ts
app.backend.pending(queryId);                 // true while a pending write touches its tables
app.backend.onPending((qid, pending) => …);   // reactive: flips on invoke, back on confirm
app.backend.pendingTables();                  // the coarse, table-level set
```

Most UIs ignore both and just render the optimistic result — which is the whole point
of optimism.

## Local query resolution

A second query over rows you've already synced resolves **locally**, immediately —
the local engine answers it from the base tables it already holds, with no new
server round-trip:

```ts
// issues already pulled in by issuesPage; a detail view materializes off them:
const detail = app.store.query.issue.where.id(eq(selectedId)).one().materialize();
detail.subscribe((row) => renderDetail(row)); // Issue | null, resolved locally
```

This is what makes navigation and drill-downs feel instant: they hit the local
engine, and the server stream only ever *adds* freshness on top.


## Devtools

For development, attach the Rindle devtools core after `createRindleClient` and
mount the React panel once near the root. The floating Rindle launcher opens the
mutation timeline, query inspector, and raw delta stream.

See [Devtools](https://rindle.sh/docs/devtools) for the package wiring, SSR-safe mounting pattern,
and production tree-shaking rules. [`create-rindle`](https://rindle.sh/docs/create-rindle) apps
include this wiring by default.

## Next steps

- [Isomorphic mutators](https://rindle.sh/docs/mutators) — the full write contract: the op
  vocabulary, reads inside a body, the acting principal, the determinism rules.
- [Compose the UI with fragments](https://rindle.sh/docs/fragments) — let a component tree declare its
  data as composable fragments that assemble into one waterfall-free query.
- [The API server](https://rindle.sh/docs/api-server) — the tier that drives these same mutators with
  authority and resolves your named queries.
- [Server rendering](https://rindle.sh/docs/ssr) — preload named queries, hydrate the seed store,
  then hand off to the live wasm client.
- [`@rindle/cli`](https://rindle.sh/docs/rindle-cli) — run the local daemon and schema workflow this
  client subscribes to.
- [Devtools](https://rindle.sh/docs/devtools) — inspect optimistic mutations, live queries, and raw
  deltas in development.
- [Synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) — this client tier wired to the
  API server and data tier.
- [The change model](https://rindle.sh/docs/change-model) — the normalized deltas the engine folds.
- [Reactive queries in the browser](https://rindle.sh/docs/wasm-client) — the `@rindle/wasm` engine
  the client runs on, used standalone (no server).
