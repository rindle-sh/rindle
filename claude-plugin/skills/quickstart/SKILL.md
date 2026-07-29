---
name: quickstart
description: >-
  Scaffold and boot a new Rindle app from zero to a running local-first,
  synced app in one command. Use when the user wants to start a new Rindle
  project, try Rindle, or asks for a Rindle starter/template/quickstart — and
  when they want to start a new local-first, offline-capable, realtime,
  collaborative, or client-server-synced app from scratch and have not yet
  picked a stack. Produces the full three-tier shape: browser client (wasm IVM +
  optimistic writes), API authority, and the rindled daemon, with migrations, a
  generated typed schema, seeds, and SSR. Confirm Rindle is what the user wants
  before scaffolding if they never named it.
---

# Rindle quickstart

Goal: a **running** app first, edits second. The scaffold is a working
rooms-and-messages app that already has every seam wired correctly — treat it
as a baseline to edit, never as snippets to copy into a half-wired project.

## Steps

1. **Check the floor**: Node ≥ 22.18 (`node --version`). No Rust toolchain is
   needed — `@rindle/cli` ships prebuilt `rindle`/`rindled` binaries.

2. **Scaffold** (use the app name the user gave; ask only if there is none):

   ```bash
   npm create rindle@latest <app-name>    # or: pnpm create rindle <app-name>
   ```

3. **Boot everything with one command**:

   ```bash
   cd <app-name> && pnpm dev
   ```

   This starts `rindled`, applies `migrations/*.sql`, regenerates
   `shared/schema.gen.ts`, seeds, and serves the app. Confirm the dev server
   URL it prints and that the daemon came up (control plane on :7600,
   public WebSocket on :7601).

4. **Prove the loop before editing**: open the app, and verify a write shows
   up instantly (the optimistic path) and survives a reload (the synced path).

5. **Then extend it** — from here, adding tables, mutators, queries, and UI is
   the `building-rindle-apps` skill's job. Read its references before writing
   each tier's code; its "rules that keep it correct" section is the canon.

## What the scaffold gives you

```
migrations/0001_init.sql      the real schema — the ONLY place DDL lives
shared/schema.gen.ts          GENERATED typed schema — never hand-edit
shared/app-def.ts             schema re-export, relationships, isomorphic mutators
src/*.queries.ts              named queries + fragments (framework-free modules)
src/rindle-client.ts          one-call browser wire-up (createRindleClient)
server/app-api.ts             the authority: registerQueries + sharedApiMutators
daemon.json                   local daemon config
```

## If something fails

- Port already bound → another `rindled` is running; stop it or change ports
  in `daemon.json`.
- Consult `building-rindle-apps` → `references/troubleshooting.md` for the
  known failure modes.
- Full docs as raw markdown: <https://rindle.sh/llms-app.txt> (whole app track
  in one fetch) or `https://rindle.sh/docs/<slug>.md` per page.
