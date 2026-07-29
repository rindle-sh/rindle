# Rindle Forum

A standalone, open-sourceable **forum built on Rindle** — the second flagship example after
`example-issue-tracker`. A forum is a better incremental-view showcase: live thread counts, reply
counts, and vote tallies are all `countAs` views the engine maintains on every write — no polling.

> Design: `../../designs/RINDLE-FORUM-DESIGN.md` (identity
> layered on later, headwaters-OIDC scoped there). Deployment follows the **one topology** (design
> `214`): a `rindle-replicator` write-master + one or more
> `rindled` read-followers — writes go to the master, reads to the follower.

## What's here

The same three-tier shape as the issue tracker:

- **Browser** — a TanStack Start SPA whose data layer *is* Rindle: the wasm IVM engine runs in-process,
  reads resolve locally and instantly, writes apply optimistically and reconcile on confirmation. The
  views (`src/components/*.queries.ts`) are Relay-style co-located fragments composed into one query
  per route.
- **API server** (`server/app-api.ts`) — the app authority. Resolves named queries to ASTs, runs the
  authoritative SQL mutators, and enforces policy: **reads are public; writes require a verified
  identity**, edits/deletes are author-gated, replies respect a thread lock, and a `"spam"` title is
  rejected (snap-back + toast). It talks to the follower for reads and the write-master for writes via
  a `SplitDaemonClient` — pure config, no code change between dev and prod.
- **Database tier** — the one-topology pair: a `rindle-replicator` **write-master** owns the SQLite
  data and captures every write into its change log; one or more `rindled` **read-followers** tail that
  log and serve live IVM + normalized deltas to subscribers. A follower has no write plane. In dev
  `rindle up` (see `rindle.ncl`) supervises the colocated pair plus stable fleet edge over loopback; `followers = 1` is the
  smallest shape.

Five normalized tables — `user`, `category`, `thread`, `post`, `vote` — joined back at query time with
correlated subqueries (`sub`) + scalar `countAs`. The schema is **SQL-first**: the DDL in
`migrations/0001_init.sql` is the source of truth, and the `@rindle/client` table schema is generated
from it into `shared/schema.gen.ts` (re-exported by `shared/app-def.ts`, which layers on the
relationships, normalization, and mutators). Migrations apply on demand through `/migrate`.

## Identity is a seam, not a database

The forum owns **no** account lifecycle. It depends only on `shared/auth.ts` — an `AuthProvider` that
resolves a request to a `ForumIdentity { subject, displayName, avatarUrl }`:

- **Open-source / dev** (`server/auth-dev.ts`): the principal rides an `x-forum-user` header; the
  TopBar "Switch user" just rewrites it. No passwords, no SaaS — the example runs standalone.
- **Production** (`server/auth-oidc.ts`): validates a JWT minted by **Rindle Cloud / headwaters**
  against its JWKS (`jose`, cached) — checking signature + `iss`/`aud`/`exp` — and maps the
  `sub`/`name`/`picture` claims to the SAME `ForumIdentity`. The api-server never changes. The forum's
  `user` row is a **projection** keyed by the OIDC `sub`, refreshed from the token on every
  authenticated write — the SaaS stays the source of truth.

`server/select-auth.ts` picks between them off `FORUM_AUTH` (`dev` default, `oidc` when set, with
`HEADWATERS_ISSUER` / `HEADWATERS_AUDIENCE`). The dev provider remains the default so the example
stays standalone.

The sign-in handoff is OAuth-implicit style — no cross-origin cookies, no CORS. "Sign in with Rindle
Cloud" (`src/cloud-auth.ts`) navigates to headwaters' `/authorize`, which (after a headwaters login)
303s back to `/auth/callback` with the JWT in the URL **fragment**. The browser stashes it and
forwards it as `Authorization: Bearer <jwt>` on every API call (`src/rindle-client.ts`); the server
verifies it offline against the JWKS. The token rides navigations + a fragment — never a credentialed
fetch — so the daemon/SaaS stay decoupled.

## Run it

```bash
pnpm install
pnpm --filter @rindle/example-forum dev        # the local pair + dev identity (standalone)
pnpm --filter @rindle/example-forum dev:oidc   # ALSO boots headwaters; real "Sign in with Rindle Cloud"
```

`pnpm dev` (`server/dev.ts`) is one command: it builds `rindle` + `rindled` + `rindle-replicator`,
then runs the database tier as `rindle up --migrate --gen --watch` over `rindle.ncl` — supervising
the one-topology fleet (the **write-master** on `:7611`, the private **read-follower** on `:7600` /
`:7601`, and the stable app-facing fleet edge on `:7650`), applying schema migrations through the
master's `/migrate` (they replicate to the follower), and regenerating `shared/schema.gen.ts` off the follower's `/schema` on boot and on
every `migrations/` change — then starts the API server (`:7700`, reads through `:7650` / writes to
`:7611`), runs the idempotent seed **against the master**, and runs Vite against the same fleet edge.
Open two windows to watch writes sync live. Switch the dev user and post as someone else; try a title containing "spam" to see
the rejection path. (Editing the schema? Add or change a `migrations/*.sql` and `--watch` re-applies it
and regenerates `shared/schema.gen.ts` automatically; `pnpm migrate` remains for manual/CI runs.)

`pnpm dev:oidc` additionally stands up **headwaters** (the SaaS identity provider) on `:8787` — it
copies `.dev.vars` from the example, applies the local D1 migrations, and runs `wrangler dev` — then
flips the forum to `FORUM_AUTH=oidc`. Click "Sign in with Rindle Cloud", create a headwaters account,
and you're posting as a real Rindle Cloud user whose JWT the forum verified against headwaters' JWKS.

> The TanStack route tree (`src/routeTree.gen.ts`) is a generated artifact — `pnpm dev` and
> `pnpm generate-routes` produce it; `pnpm typecheck` runs `tsr generate` first.

## The write/read split (one topology)

There is one deployment shape (design 214): a `rindle-replicator` **write-master** + one or more
`rindled` **read-followers**. The split lives entirely in `server/app-api.ts`'s daemon construction —
`createForumApi` hands the api-server a `SplitDaemonClient` (from `@rindle/api-server`) that routes
writes (`executeSqlTxn`/`rejectMutation`/mutation sessions) to the write-master and reads/control
(`materialize`/`query`/…) to a follower. A follower has **no write plane** — a write there 404s — so
the master is the only write target, in dev and prod alike. No mutator/query/policy code touches this
seam; it is **pure env**:

| env | role |
|---|---|
| `RINDLE_FOLLOWER_URL` (Node) / `DAEMON_ORIGIN` (Worker) | reads + control plane (the follower) |
| `RINDLE_REPLICATOR_URL` (Node) / `REPLICATOR_ORIGIN` (Worker) | writes (the write-master) |
| `RINDLE_REPLICATOR_TOKEN` / `REPLICATOR_TOKEN` | write-master bearer token (defaults to the follower's) |

Locally, `rindle.ncl` + `rindle up` render and supervise the pair plus fleet edge over loopback. For
a managed deployment, create the app in Headwaters and use its single hostname for both origin vars;
the OVH edge path-routes each request to the correct role.
