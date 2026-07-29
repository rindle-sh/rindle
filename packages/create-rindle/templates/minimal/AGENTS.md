# AGENTS.md — working on __PROJECT_NAME__

Guidance for coding agents (and new humans). This is a **Rindle** app — an
incremental-view-maintenance (IVM) engine keeps every registered query's result
exact on each write instead of re-running it. Three tiers: a TanStack Start SPA
running the wasm engine in-process, an API authority, and the data tier — the one
topology (design 214): a `rindle-replicator` write-master + a `rindled`
read-follower (`followers = 1` = the colocated pair). The correctness contract
everywhere is **view-after-write == fresh-query**.

Rindle docs are served as raw markdown for LLMs: index at
<https://rindle.sh/llms.txt>, the whole app track in one file at
<https://rindle.sh/llms-app.txt>.

## Commands

- `pnpm dev` — the one lifecycle command: `rindle dev` evaluates `rindle.ncl`
  once, supervises the `rindle-replicator` write-master + `rindled` follower +
  fleet edge (prebuilt binaries from `@rindle/cli`, no Rust toolchain), waits for
  them, applies `migrations/*.sql`, regenerates `shared/schema.gen.ts`, then runs
  Vite on :3000 with `RINDLE_URL` + `RINDLE_DATABASE_TOKEN`. It watches migration
  and follower-schema changes and tears down the whole process tree together.
- `pnpm typecheck` — regenerates the route tree, then `tsc --noEmit`.
- `pnpm migrate` — one-shot `rindle migrate apply` against the unified ingress derived
  from `rindle.ncl` (the follower's `/migrate` is write-fenced). The dev loop already
  applies on boot + on every `migrations/` change.
- `pnpm rindle:deploy` / `pnpm rindle:migrate:remote` — deploy the data tier to
  Rindle Cloud (reads `rindle.ncl`, the same file `rindle up` runs locally; run
  `rindle login` once first) and push `migrations/*.sql` to the deployed master.

## Rules that keep the app correct

Break one of these and the app goes subtly wrong. Treat violations as bugs in
review:

1. **SQL is the source of truth.** Change the schema by **adding** a
   `migrations/*.sql` file (SQL DDL: `CREATE TABLE` / `ADD COLUMN` /
   `CREATE INDEX`, plus the destructive `DROP TABLE` / `DROP COLUMN` /
   `DROP INDEX` — no `RENAME`; every table a single primary key). **Never
   hand-edit `shared/schema.gen.ts`** — it is generated from the live daemon
   and overwritten on the next migration.
2. **Mutators are one isomorphic body, deterministic and replayable**
   (`shared/app-def.ts`): a generator that `yield`s logical ops
   (`yield tx.insert(...)`), paired with its zod arg schema via
   `shared(args, gen)`. No `Date.now()`, no `Math.random()`, no I/O — the client
   re-invokes the body on every rebase. Generate ids and timestamps at the
   callsite and pass them in as args; the acting user is `ctx.user` (injected
   per tier), never an arg.
3. **The server drives the same body** — `sharedApiMutators` in
   `server/app-api.ts` auto-wraps the whole registry (parse the untrusted wire
   args through each mutator's `.args`, inject the authenticated principal,
   render every op to SQL). Add an explicit entry ONLY for server-only
   authority the client must not predict (a policy guard, a raw `tx.exec`
   relational gate). Only `(name, args)` ever crosses the wire; `throw` to
   hard-reject (the optimistic write snaps back on its own; write no rollback
   code).
4. **Remote subscriptions must be named.** Define queries with `defineQuery` in
   `src/components/*.queries.ts` and register them in `server/app-api.ts`. An
   ad-hoc `store.query.…` builder resolves **locally only** — it never opens a
   server subscription.
5. **Database tokens are server-only.** `rindle dev` injects the one application connection as
   `RINDLE_URL` + `RINDLE_DATABASE_TOKEN`; the bearer must never reach the browser. The browser
   learns only the public WebSocket endpoint + placement ticket from its query-lease response.
   Never copy topology ports into package scripts or add a browser config endpoint.
6. **Subscribe to windows, not whole tables** — order + `limit`, and ratchet
   `limit` up for "load more". The engine keeps the window (and any `countAs`)
   exact as rows enter and leave.
7. **Keep `*.queries.ts` modules React-free** — no `.tsx` imports. The browser,
   the API authority, and the SSR loader all import these same modules.
8. **Declare route reads through `rindle.loader(...)`** (`src/rindle-tanstack.ts`).
   It owns server preloading, client navigation readiness/cancellation, and
   blocking stale reloads. Keep `rindle.Provider` at the root; do not recreate
   the old `useMatches()` merge or a separate `<RindleSSR>` wrapper.

## File map

| Path | What it is |
| --- | --- |
| `migrations/*.sql` | the real schema — the only place DDL lives |
| `shared/schema.gen.ts` | **generated** table schema — do not edit |
| `shared/app-def.ts` | the shared contract: relationships, query builder, isomorphic mutators |
| `src/components/*.queries.ts` | named queries + fragments, co-located with their components |
| `src/rindle-client.ts` | the one-call browser wire-up (`createRindleClient`) |
| `src/rindle-tanstack.ts` | the shared `rindle.loader` + `rindle.Provider` integration |
| `server/app-api.ts` | the authority: `registerQueries` + `sharedApiMutators` + server-only policy |
| `src/routes/api.rindle.*.tsx` | TanStack Start server routes exposing the authority over HTTP |
| `rindle.ncl` | the one topology (the colocated pair) — `rindle up` runs it locally, `rindle deploy` provisions it |
| `src/ssr.ts` | SSR preload of the same named queries for first paint |

## Reading more

Per-page markdown mirrors live at `https://rindle.sh/docs/<slug>.md`. Most
relevant here: `synced-app-quickstart`, `client`, `api-server`, `schema`,
`fragments`, `ssr`, `supported-queries-ts`, `change-model`.
