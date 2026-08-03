<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/troubleshooting.md (https://rindle.sh/docs/troubleshooting)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# Troubleshooting

> The rules that keep a Rindle app correct, and the ways an app goes subtly wrong when one is broken — sync that never starts, schema drift, optimistic flicker, silent writes, and auth smells, each with its fix.

Most Rindle bugs are **contract violations, not framework bugs**. The contract is
small; this page states it once, then works through the failure modes you'll
actually see, most common first.

## The rules that keep an app correct

These are not style preferences. Break one and the app is wrong in a way tests may
not immediately catch.

1. **SQL is the source of truth; the TS schema is generated.** Evolve the schema by
   **adding** an ordered migration (pure DDL for schema, pure DML for data), and never
   hand-edit `schema.gen.ts` — see [Schema & migrations](https://rindle.sh/docs/schema).
2. **A mutator is one isomorphic body, run on both tiers.** Write it once with
   `shared(args, gen)`; add a hand-written server entry only for authority the
   client must *not* predict — see [Isomorphic mutators](https://rindle.sh/docs/mutators).
3. **Mutators must be deterministic and replayable.** No `Date.now()`, no
   `Math.random()`, no I/O — ids and timestamps arrive as args, and the acting user
   is `ctx.user`, never a client-supplied arg.
4. **Only `(name, args)` crosses the wire.** Client-built ASTs and client-computed
   effects never become authority; the server parses untrusted args through the
   mutator's `.args` schema and each query's validator.
5. **Remote subscriptions must be named.** Only a `defineQuery` value opens a
   server subscription; a bare builder query resolves locally.
6. **Subscribe to windows, not whole tables.** Order + `limit`, and ratchet the
   limit up for "load more".
7. **The database token is server-only.** Only trusted code receives
   `RINDLE_URL` + `RINDLE_DATABASE_TOKEN`; the browser receives an authorized lease,
   public WebSocket endpoint, and placement ticket.
8. **Keep `*.queries.ts` modules framework-free.** The browser, the API authority,
   and any SSR loader all import them; no component imports.

Everything below is one of these rules, broken.

## Nothing syncs / a query never leaves "loading"

- **The query isn't named.** Only a `defineQuery` value opens a **server**
  subscription. A bare `store.query.<table>.where…` builder resolves **locally
  only** — it renders off already-synced rows and never pulls new data. Wrap it in
  `defineQuery`, call `myQuery(args)`, and register it on the server.
- **The query isn't registered on the server.** Add it to the
  `registerQueries<User>([...])` list in your API server. An unregistered name
  can't resolve to an AST.
- **The lease has no usable `wsEndpoint`.** Remove old browser-side `daemon.wsUrl`
  configuration. Configure the API server with `rindle: { url, token }`; it derives the
  public socket from the unified ingress (or from an explicit server-side `rindle.wsUrl`)
  and returns it on the lease.
- **`api.url` doesn't reach your API server.** Check the dev-server proxy (e.g.
  Vite's `server.proxy["/api"]`) points at the API server's port.
- **The daemon isn't running / migrations weren't applied.** `rindle status` and
  `rindle migrate status`.

## `schema.gen.ts` errors / types don't match the DB

- **Someone hand-edited `schema.gen.ts`.** It's generated and overwritten. Change
  the **SQL migration** instead, then re-run `rindle schema gen` (or let
  `rindle dev --gen …` regenerate on change).
- **Forgot to regenerate after a migration.** Run
  `rindle schema gen --out shared/schema.gen.ts`. (The daemon rejects a stale
  schema fingerprint on subscribe, so this fails loudly, not silently.)
- **A column is the wrong kind** (e.g. a boolean reads as `number`). SQLite kept
  the declared name — declare it `BOOLEAN`/`JSON`, not bare `INTEGER`/`TEXT`.
- **A migration was rejected.** `RENAME` and column **type changes** are not
  supported — expand instead (add the new column/table, move writes, then `DROP`
  the old one). `blob` is also refused. Drops themselves are supported and print a
  `[destructive]` notice. (`BIGINT`/`INT8` are accepted — they declare the exact
  int64 plane; live queries touching such a column are refused until the browser
  bigint lane ships, so `select` the other columns.) See
  [Schema & migrations](https://rindle.sh/docs/schema).
- **A migration mixes DDL and DML.** Split it into ordered files: add the schema in
  one file, then seed or backfill it with `INSERT` / `UPDATE` / `DELETE` in the next.
  Reads, PRAGMAs, and transaction-control statements are not migration steps.
- **A data migration is too large.** One file may capture at most 8,191 user-row
  changes and 64 MiB. Split the backfill into explicit key ranges; each file is its
  own atomic, checksum-bound migration.

## Optimistic writes flicker, double-apply, or drift after rebase

- **A mutator is non-deterministic.** It re-runs on every rebase — remove
  `Date.now()`, `Math.random()`, and any I/O. Generate ids/timestamps at the
  **callsite** and pass them as args ([the determinism
  rules](https://rindle.sh/docs/mutators#the-determinism-rules)).
- **A server override drifted from the shared body.** The two tiers run the SAME
  isomorphic generator, so the base case can't disagree — but a hand-written
  server entry (a policy guard, a raw `tx.exec` cascade) can. Keep the override's
  effect a superset of the shared body: drive the shared body via
  `runSharedMutation` and add only the server-only authority, so the prediction
  still matches the commit.
- **A read-dependent mutator was folded.** A mutator that reads (`yield tx.row` /
  `tx.query`) or isn't absorbing (e.g. `increment`) must **not** use
  `.folded(...)`. The folded path throws for readers; route non-absorbing mutators
  through plain `mutate`.

## A write is silently ignored (no error, nothing changes)

- **Accepted-but-no-op is by design.** If a server op matches no row (e.g. a raw
  `DELETE … WHERE id = ? AND ownerId = ?` for a non-owner), the write is accepted
  and the optimistic change rebases away. If you meant to *reject*, `throw` in the
  mutator body (or a server guard) instead — a hard reject fires `onRejected`. The
  two shapes are contrasted in [the API
  server](https://rindle.sh/docs/api-server#driving-the-shared-mutators).
- **Args failed server validation.** The shared mutator's `.args` schema parses
  the untrusted wire args before the body runs; if `parse(raw)` throws, it's a
  hard reject — surface it via `onRejected`.

## A query throws `BuildError` when it materializes

You hit an unsupported shape. Check [Supported query
shapes](https://rindle.sh/docs/supported-queries-ts) — common ones: root `count()` mixed with
`select`/`sub`/`orderBy`; a low-pass parent-by-child-count `having`; an `exists`
carrying `start` or a nested `sub`; `sum`/`avg`/`min`/`max`.

## Auth / security smells

- **`RINDLE_DATABASE_TOKEN` reached the browser.** It is a database-wide credential.
  Keep it in the API server's secret store; the browser should know only your API URL
  and the short-lived lease data that API returns.
- **Trusting a client-supplied owner/author.** Identity is **off-wire** — the
  actor is `ctx.user` (the server injects its authenticated principal), never an
  `owner`/`author` arg. The shared body already reads `ctx.user`, so the server's
  `sharedCtx` is the single place identity enters.
- **Not validating args on the server.** The client's prediction is a guess.
  Every shared mutator carries its `.args` schema; `sharedApiMutators` parses the
  untrusted wire args through it before the body runs. A hand-written override
  must parse too.

## SSR / wasm boot errors

- **wasm constructed during server render.** Never construct the optimistic
  client during SSR/prerender. Defer: lazily `import("@rindle/optimistic")` +
  `import("@rindle/wasm")` on the client and memoize the boot promise — see
  [Server rendering](https://rindle.sh/docs/ssr); [`create-rindle`](https://rindle.sh/docs/create-rindle) apps ship
  this pattern in `src/rindle-client.ts`.

## A restart loses live queries

Expected — `rindled` keeps no durable materialization state. Wire `onBootId` on
the `HttpRindleDaemonClient` and re-assert pins (`api.assertPins()`) when it fires
— see [pinned queries](https://rindle.sh/docs/api-server#pinned-queries-the-one-shot-read).

## SQL points at the wrong service

Use the application ingress, not an internal master port or legacy replicator URL:

```sh
RINDLE_URL=https://app-… RINDLE_DATABASE_TOKEN=… rindle sql "select 1"
```

Application code uses the same values with `createSqlClient({ url, authToken })`. A
`404` for `/v1/sql` usually means `RINDLE_URL` points at a follower/control endpoint
instead of the unified edge.

## `ECONNREFUSED` on `:7600` / `:7611` / `:7650` after upgrading

Local ports are no longer fixed. Each project gets its own 100-wide block, chosen from
the path of the directory holding `rindle.ncl`, so several Rindle projects — or several
git worktrees of one project — can run at the same time. `:7600` and friends are simply
not your fleet's ports any more. See
[Running several projects at once](https://rindle.sh/docs/rindle-cli#running-several-projects-at-once).

Take the URLs from the environment `rindle dev` injects, or from `rindle.json`'s
`bindings` — never a literal:

```ts
// not: process.env.RINDLE_DAEMON_URL ?? "http://127.0.0.1:7600"
const daemonUrl = process.env.RINDLE_DAEMON_URL;
if (!daemonUrl) throw new Error("RINDLE_DAEMON_URL is required");
```

A hardcoded fallback is worse than a crash here: if another Rindle project happens to
hold that port, the read **succeeds** against the wrong database. The CLI and the
daemons fence themselves against that with a project fingerprint, but a browser or an
app-tier HTTP client sends no identity and cannot be fenced.

To keep the old numbers instead, pin the block in `rindle.ncl`:

```text
{ portBase = 7600 }
```

`rindle render` prints the resolved URLs for whichever block you end up on.

## Performance: subscribing to too much

Subscribe to **windows**, not whole tables — `orderBy` + `limit`, and ratchet the
limit up for "load more". IVM keeps the window (and any `countAs`) exact as rows
enter and leave. Add SQL indices for the directions your joins and windows
traverse — see [Performance](https://rindle.sh/docs/performance).

## Next steps

- [Isomorphic mutators](https://rindle.sh/docs/mutators) — the write contract most of these rules
  protect.
- [Supported query shapes](https://rindle.sh/docs/supported-queries-ts) — what the builder can and
  can't express.
- [The API server](https://rindle.sh/docs/api-server) — validation, authority, and the two
  rejection shapes.
- [Run the daemon](https://rindle.sh/docs/daemon) — ports, planes, and restart recovery.
