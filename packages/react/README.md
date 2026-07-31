# @rindle/react

React bindings for Rindle stores.

Full docs — reads, fragments, and the waterfall-free composition pattern:
**[rindle.sh/docs/client](https://rindle.sh/docs/client)** and
**[rindle.sh/docs/fragments](https://rindle.sh/docs/fragments)** · markdown mirrors:
[`client.md`](https://rindle.sh/docs/client.md) / [`fragments.md`](https://rindle.sh/docs/fragments.md)
· for agents: [llms.txt](https://rindle.sh/llms.txt)

```tsx
import { Rindle, fragmentKey, useFragment, useRoot } from "@rindle/react";
import { IssueCardFragment, issuesPageQuery, type IssueCardRef } from "./IssueListItem.queries.ts";

createRoot(root).render(
  <Rindle store={app.store}>
    <Issues />
  </Rindle>,
);

function Issues() {
  const [issues, { status }] = useRoot(issuesPageQuery, { limit: 50 }, IssueCardFragment);
  if (status !== "complete" && issues.length === 0) return <p>Loading...</p>;
  return issues.map((issue) => <IssueRow key={fragmentKey(issue)} issue={issue} />);
}

function IssueRow({ issue }: { issue: IssueCardRef }) {
  const data = useFragment(IssueCardFragment, issue);
  if (!data) return null;
  return <div>{data.title}</div>;
}
```

`useRoot(namedQuery, args?, ...ctx?, RootFragment?)` retains the full named root query for sync
coverage and returns local React-facing data. Passing a root fragment returns opaque refs for the
root rows; child components read those refs with `useFragment(fragment, ref)`. `useQuery(query)` is
still available for direct live query reads that do not need fragment-local boundaries.

For local-first backends, the React-facing data view is shared by compiled local AST. Each
mounted hook still retains its own `name`/`args` lease through the Store, so backend-level
subscription refcounting remains responsible for server dedupe and teardown. Backends that
cannot split local views from remote subscriptions keep the older materialized-view fallback.

Agent narration is an optional integration provided by `@rindle/narrator-react`; it is not a
runtime dependency of these base React bindings.

## Other exports

- `Rindle` / `RindleProvider` — the context provider (`RindleProvider` is an alias of `Rindle`).
- `useQuery(query)` — live query data; subscribes on mount, releases on unmount.
- `useQueryStatus(query)` — the query's `ResultType`, which tracks the **server channel only**:
  `unknown` while it loads (not yet server-authoritative), `complete` once the server has
  answered. A pending optimistic mutation no longer moves it — "is a write pending here?" is a
  separate axis on the backend (`backend.pending(qid)` / `onPending`), and `error` is reserved
  and currently unproduced. Shares `useQuery`'s leased view, so reading both is one subscription.
- `useSyncQuery(query)` — retains a named remote query for normalized/local-first sync coverage
  without subscribing React to the broad result tree; returns only `ResultType`.
- `useRoot(namedQuery, args?, ...ctx?)` — retains the full root coverage query and returns
  `[data, details]`, where `details.status` distinguishes loading from completed-not-found.
  Without a fragment argument, `data` is the root query's local React-facing data. Fragment child
  relationships are surfaced as `FragmentRef`s, so children can keep owning their own reads.
  Passing a fragment as the final argument switches `data` to root-ref mode:
  `useRoot(namedQuery, args, RootFragment)`.
- `useFragment(fragment, ref)` — reads an opaque local fragment ref. It opens a narrow local-only
  query for that fragment and returns `null` until the local row is available. Child relationships
  composed as `sub(alias, rel, ChildFragment)` are refs; inline child builders remain inline data.
- `fragmentKey(ref)` — stable key for opaque fragment refs, useful for React list keys.
- `useRindleStore()` — the `Store` from context, for imperative writes/reads inside components.
- `SyncQueryCache` — the per-provider cache that dedupes sync-only coverage retains.
- `QueryCache` — the per-provider cache that dedupes a query's materialized view across hooks.
- `queryCacheKey(query)` — the stable string key (`@rindle/client`'s `stableKey(ast)`) under which
  the cache stores that view. Same AST → same key → one shared entry, and it matches the SSR seed
  key so hydration finds the dehydrated view. Keys the view (bare AST), not the lease.

## Streaming an LM response

`useStreamedText({ streamId, durable, live })` renders a language-model response that is arriving on
two planes: the checkpointed prefix through your ordinary query, and the not-yet-checkpointed tail
over SSE. It returns the merged text.

```tsx
const data = useFragment(MessageFragment, message);
const streaming = data.status === "streaming" || data.status === "pending";
const text = useStreamedText({
  streamId: data.id,
  durable: assembleDurableText(data, data.chunks),   // body ++ the un-compacted chunk rows
  live: streaming,
});
```

`durable` is a string, not the row, because the column names belong to your schema — the hook never
guesses where `body` lives. It handles the parts that are easy to get wrong: not reconnecting when the
durable text advances, seeding the tail with the offset it joined at, closing its own `EventSource` on
a terminal frame (`EventSource` reconnects on *any* close), keying the tail to its stream so
switching messages can't show the previous one's text, and splicing a resumed replay at its own
offset so an `EventSource` reconnect (which can rewind behind the tail) never duplicates text. Losing the live leg is not an error — without
`EventSource`, or on the wrong server instance, the text still advances through the query at
checkpoint granularity.

`StreamTransport` is injectable for WebSocket, a fetch stream, or a test. Server side:
**[`@rindle/api-server`](https://rindle.sh/docs/api-server)** (`openStream` / `subscribeStream`).
