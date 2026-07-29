# create-rindle

Scaffold a new **SQL-first [Rindle](https://github.com/rindle-sh/rindle) app on
[TanStack Start](https://tanstack.com/start)** in one command.

```bash
npm create rindle@latest my-app
# or
npx create-rindle my-app
# or: pnpm create rindle my-app · yarn create rindle my-app · bun create rindle my-app
```

Then:

```bash
cd my-app
pnpm dev
```

Full docs — what the template contains and how to grow it:
**[rindle.sh/docs/create-rindle](https://rindle.sh/docs/create-rindle)** · markdown
mirror: [`create-rindle.md`](https://rindle.sh/docs/create-rindle.md) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

## What you get

A minimal but real three-tier Rindle app (a tiny forum-of-rooms with **live message counts**):

- **Browser** — a TanStack Start SPA whose data layer is Rindle's in-process wasm IVM engine: local
  instant reads, optimistic writes, fragment-rooted `useRoot` reads, and a dev-only Rindle devtools
  pane.
- **API server** — the authority: named-query resolution, authoritative SQL mutators, and policy (a
  `"spam"` rejection demo shows the optimistic snap-back + toast).
- **Daemon** (`rindled`) — owns the SQLite data + live incremental views, streamed to subscribers.

The schema is **SQL-first**: `migrations/*.sql` is the source of truth, and the `@rindle/client`
schema is generated from it into `shared/schema.gen.ts` (`rindle up --migrate --gen shared/schema.gen.ts --watch` in
the dev loop), so the TypeScript can't drift from the DDL.

The prebuilt `rindle` + `rindled` binaries come from `@rindle/cli` (per-platform, installed as a dev
dependency) — **no Rust toolchain required**.

## Fragment flow in the template

The generated routes use the current co-located fragment pattern:

- `src/components/*.queries.ts` defines each component's `defineFragment` beside the named
  `defineQuery` that roots it.
- The home route declares `roomsQuery()` through `@rindle/tanstack`'s `rindle.loader`, which seeds
  SSR and waits for the same live query on client navigation, then calls
  `useRoot(roomsQuery, RoomCardFragment)` to receive opaque room refs.
- Row components call `useFragment(RoomCardFragment, room)` to open narrow local reads without a
  new server subscription.
- The room detail route calls `useRoot(roomDetailQuery, id)` when the route itself owns the root
  fields and passes child message refs down with `fragmentKey(ref)` list keys.

## Usage

```
create-rindle [directory] [options]

Options:
  --no-install    Don't run the package-manager install after scaffolding
  --pm <name>     Package manager: npm | pnpm | yarn | bun (default: auto-detected)
  --link          Internal: wire @rindle/* to this monorepo's workspace (for apps/* in the repo)
  -h, --help      Show help
```

`--link` is for developing the template *inside* the Rindle monorepo: it rewrites the `@rindle/*`
dependencies to `workspace:*` and switches `vite.config.ts` / `tsconfig.json` to the `@rindle/source`
aliases the in-repo examples use, so a scaffold dropped into `apps/` typechecks against source. CI
uses it to typecheck the generated app on every change.


## Devtools in generated apps

The default template mounts `@rindle/react-devtools` in development and attaches
`@rindle/devtools` to the generated Rindle client. Click the floating **🌊 Rindle** launcher to
inspect the optimistic mutation timeline, live queries, and raw delta stream. Both packages are
imported behind `import.meta.env.DEV`, so they tree-shake out of production builds.

## Templates

| name | description |
|---|---|
| `minimal` (default) | rooms + messages with live counts, optimistic writes, SSR, the auth seam |

## Requirements

The generated app runs the `.ts` server files via Node's built-in type stripping — **Node ≥ 22.18**.
