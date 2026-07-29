# @rindle/wiki-demo — the tiny machine

A live demo of the one thing rindle is built for: **keep expensive, deeply-nested queries
permanently prepared, and let many readers share the one materialization each.** A real `rindled`
daemon keeps **three** nested boards **pinned** (maintained on every write, warm with zero viewers)
— most-active pages, just-edited pages, and most-active editors — all fed by the *same* edit
stream under different correlations; a thin Node tier ingests Wikimedia's real edit firehose and
fronts the data tier; and every browser attaches to the *same* server-side results. Readers load
instantly; there is nothing to invalidate; the cost tracks the *change*, not the table — and N
readers cost **one** pipeline each, not N.

```text
  Wikimedia             Node tier           HCTree master         read follower       readers
 ┌──────────┐  SSE  ┌──────────────┐  SQL  ┌──────────────┐ log ┌──────────────┐ ws ┌─────────┐
 │ recent-  │──────►│ ingester +   │──────►│ rindle-      │────►│ rindled      │───►│ browser │ × N
 │ change   │       │ query API    │       │ replicator   │     │ pinned views │    └─────────┘
 └──────────┘       └──────────────┘       └──────────────┘     └──────────────┘
       ▲ offset            └──────── query leases ──────────────────────┘
       └─ resume ──────────┘
```

Three real tiers (the same one topology as `apps/example-issue-tracker`; browsers are read-only):

1. **The Rindle data tier** — `rindle-replicator` owns the authoritative HCTree database and one
   journal order; a read-only `rindled` follower tails it over loopback, dedupes identical queries
   onto **one** pipeline, and serves a private read/control plane plus a public subscription ws. The
   top-N is **pinned** (`policy: pinned`) so it is maintained whether or not anyone is watching — a
   new visitor attaches to an already-warm result, and every visitor *shares* it.
2. **The Node tier** (`src/tier.ts`) — the app authority: `@rindle/api-server` resolves the named
   `active`/`latest`/`editors` queries to follower leases and **pins** them, and the
   **ingester** turns each Wikimedia change into a `page` upsert + an `editor` upsert + an `edit`
   insert through the master's write ingress (`executeSqlTxn`). It also prunes edits outside a
   rolling window (decrementing/removing pages **and** editors) so the dataset stays bounded.
   Browsers are **read-only**: there is no mutate route; the only writer is the ingester.
3. **Browsers** — real `createRindleClient` readers (`product-page`, route `/wiki`): the in-browser
   optimistic IVM engine leases the named queries through the API server, then subscribes over the
   daemon's ws. The queries (`src/schema.ts`) are two "live edit board" shapes over one `edit`
   stream: **pages** (top pages by recent edit count, newest edits nested, correlated by
   `page.id ← edit.page_id`) and **editors** (top editors by edit count, newest edits nested,
   correlated by `editor.name ← edit.user`, with a `where edits > 1` Filter). Both ride only the
   ✅ shapes in `docs/SUPPORTED_SHAPES.md` (ordered-limit + a nested correlated relationship, plus
   a Filter under the Take).

### Seed + tail, with no data tears

The source is a genuine **CDC stream**: Wikimedia's `recentchange` is a forward log, and each SSE
event carries a resumable Kafka **offset**. The board only ever shows edits the stream actually
delivered (it accumulates forward), so it is **complete by construction** — no partial rows, no
missing parents.

On a **cold start** (fresh volume, no stored offset) the ingester *seeds* the window by opening the
stream `WIKI_BACKFILL_MIN` minutes in the past — the EventStreams `?since=` cursor, defaulting to
`WIKI_WINDOW_MIN` — and replays that backlog faster-than-realtime so the boards are full at once
instead of filling over wall-clock time, then transitions seamlessly to the live tail. On every
write the ingester persists the latest applied offset to the data directory; on a **restart**
`rindled` re-warms the pins from the on-disk data and the ingester resumes from exactly where it
left off — the persisted offset wins over backfill — gap-free. (Backfill depth is capped by the
stream's retention, ~7d; set `WIKI_BACKFILL_MIN=0` to start live from "now" instead.)

### Surviving a follower restart

`rindled` keeps **no durable** lease/materialization state, so it stamps a `bootId` on its ready
line, every control-plane response (a header), and every `nhello`. On a restart:

- the **API server** notices the new `bootId` (it rides the ingester's constant writes, via the
  daemon-client `onBootId` hook) and **re-asserts the pins** — no polling;
- **browsers** reconnect the ws (capped backoff), re-lease, re-subscribe, and force a clean
  re-hydrate (the boot-id resets the client's `cv` watermark) — open tabs heal with no refresh.

## Run it

```sh
pnpm install            # at the repo root (single pnpm workspace)

cd apps/wiki-demo
pnpm dev            # builds/boots the write-master/follower pair + API/ingester; REAL feed
pnpm dev:synthetic  # a deterministic, offline synthetic edit stream (no egress needed)
pnpm smoke          # offline end-to-end test: starts the pair, drives it with a real client
```

`pnpm dev` builds the local `rindle-replicator` and `rindled` binaries, then `src/main.ts` starts the
colocated pair directly. The fixed demo schema is declared on the master as base `tables`; its
genesis DDL reaches the bare follower through the same journal (one boot, no `/migrate` step). It
then starts the Node tier. A container build uses the same path with `RINDLE_REPLICATOR_BIN`,
`RINDLED_BIN`, and `WIKI_DATA_DIR` set for the image; see [DEPLOY.md](DEPLOY.md).

- **API + metrics:** `http://127.0.0.1:7700` (`WIKI_API_PORT`) — the lease route
  (`/api/rindle/query`) the page calls, plus `/metrics` (`{ pages, editors, edits, editsInWindow,
  materializations, leases, vcpus, cpuPercent, memUsedBytes, memLimitBytes, viewers,
  source, daemonWsUrl, offset }`). `editsInWindow` is the live working set the boards run over (the
  scale ticker); `materializations` holds the three pinned board pipelines (shared by every tab —
  that's the dedup) plus a tiny per-client bookkeeping query.
- **follower:** read/control plane `http://127.0.0.1:7600`, public subscription ws
  `ws://127.0.0.1:7601`.
- **master:** write ingress `http://127.0.0.1:7611`; its fan-out `:7610` stays internal to the pair.
- **auth:** `WIKI_DAEMON_TOKEN` (follower) and `WIKI_WRITE_TOKEN` (master); omit both for open
  loopback development.

The frontend (`product-page`, route `/wiki`) reads `VITE_WIKI_API` (default `http://127.0.0.1:7700`)
and `VITE_WIKI_WS` (default `ws://127.0.0.1:7601`).

### The real Wikimedia source

The default source tails the public **EventStreams** SSE API (no auth, no key):
`https://stream.wikimedia.org/v2/stream/recentchange`, filtered here to human edits of English
Wikipedia articles (`WIKI_DOMAIN`, default `en.wikipedia.org`, namespace 0, type edit/new,
non-bot). It needs outbound access to `stream.wikimedia.org` from the **ingester** (the Node tier).
In a sandbox with an egress allowlist, add that host; otherwise the `--source synthetic` stream runs
anywhere.

**Env:** `WIKI_API_PORT` (7700) · `RINDLED_BIN` · `RINDLE_REPLICATOR_BIN` · `WIKI_DATA_DIR` · `WIKI_NWORKERS` (3) ·
`WIKI_WINDOW_MIN` (180, the rolling prune window) · `WIKI_BACKFILL_MIN` (cold-start seed depth;
defaults to the window, `0` disables) · `WIKI_DOMAIN` (en.wikipedia.org) · `WIKI_SOURCE`
(wikimedia | synthetic) · `WIKI_DAEMON_TOKEN` · `WIKI_WRITE_TOKEN`.

## Deploy (self-host)

The colocated write-master/follower pair + the stateless API/ingester tier package into one
container (`Dockerfile`) you run on a single box — a worked example of **self-hosting** the rindle
server tier. The live demo runs it on one always-on Fly machine + a volume, fronted by a Cloudflare
Worker that reverse-proxies `rindle.sh/demo/wiki/*` to it (`WIKI_ORIGIN`). This *is* the production
shape — the same one topology as `apps/example-issue-tracker`, here serving a read-heavy fan-out
with the boards pinned and shared across every reader. Full steps (Fly + Cloudflare, plus running
the image on any other host) in [DEPLOY.md](DEPLOY.md).

(The managed, hosted data plane is a separate path — provisioned through Headwaters on its own
infrastructure — and needs none of the above.)
