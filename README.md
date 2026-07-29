# Rindle — an incremental query engine

**Query once, maintain forever.** Rindle keeps a query's result up to date as the data
changes. It is one small incremental-view-maintenance (IVM) engine — written in Rust, and
equally at home in a browser tab, a Node server, over the network, or embedded directly in a
Rust host. You register a query once and the engine maintains its result by computing the
incremental **difference** on each write instead of re-running the query.

The core contract is the same everywhere: **view-after-write == fresh-query** — the changes
you receive, applied in order, always equal what you'd get by running the query from scratch.

→ **[Docs & live playground](https://rindle.sh)** (the real engine, running in your browser tab)

> [!NOTE]
> **This repository is a generated, read-only mirror**, produced with
> [Copybara](https://github.com/google/copybara) from the upstream Rindle monorepo. Changes
> pushed directly here are overwritten on the next sync — please file issues upstream.

## What's here

| | |
| --- | --- |
| **The core IVM engine** | [`rust/src`](rust/src) — the `rindle` crate: the operator graph, the COW B+tree memory source, the wire `Ast`, the fluent query builder. std-only, no C toolchain, wasm-clean. |
| **The SQLite adapter** | [`rust/rindle-sqlite`](rust/rindle-sqlite) — the zero-copy `TableSource` leaf, `FetchRequest`→SQL lowering, the prepared-statement cache, spill-to-SQLite operator storage. |
| **The JS packages** | [`packages/`](packages) — the `@rindle/*` client and its backends, listed below. |
| **Demo apps** | [`apps/`](apps) — worked examples, including a full three-tier issue tracker. |
| **The Claude Code plugin** | [`claude-plugin/`](claude-plugin) — the `building-rindle-apps` skill, a quickstart command, a reviewer agent, and a write-time lint hook. See [Build with an AI agent](#build-with-an-ai-agent). |

Alongside the two headline crates are the four they need in order to build and to prove
themselves: [`rindle-planner`](rust/rindle-planner) (the cost-based join-flip planner — a hard
dependency of the SQLite adapter), [`rindle-d2s`](rust/rindle-d2s) (compiles a query AST into a
single SQLite `SELECT` — the **differential oracle** for what the incremental result *should*
be), [`rindle-index-advice`](rust/rindle-index-advice), and
[`rindle-testfix`](rust/rindle-testfix) (shared Chinook/mini fixtures + the IVM↔SQLite parity
harness). Correctness here is enforced differentially, not just by example tests.

[`rust/vendor/libsqlite3-sys`](rust/vendor/libsqlite3-sys) is the one patched SQLite every
SQLite-linking crate shares — a `bedrock`-branch amalgamation with wal2 + `BEGIN CONCURRENT`.
It builds with no `make` and no libclang.

### What's *not* here

The server tier that turns the engine into a synced, local-first stack — the `rindled`
read-follower daemon, the `rindle-replicator` write-master, the CDC/streaming and backup
planes, and Rindle Realtime — is not part of this repository. Two consequences to know about
while reading the code:

- **`@rindle/replica`** is the napi-rs Node backend; its native engine crate lives upstream, so
  `pnpm --filter @rindle/replica build:native` cannot run here. The TypeScript sources ship
  exactly as npm publishes them. **`@rindle/wasm`, by contrast, builds from this repository** —
  its engine is `rust/src`.
- Several packages' `.e2e.mjs` tests spawn a `rindled` binary; those are upstream-only. Unit
  tests and the whole Rust battery run here.

`rust/Cargo.toml` is synced verbatim apart from its workspace `members` list, so its
dependency, feature, and profile blocks can never drift from upstream — the trade is that a
few of its comments still refer to crates that only exist in the private monorepo.

## Packages

| Package | Role |
| --- | --- |
| `@rindle/client` | Backend-agnostic client core: typed schema, query builder, `ArrayView`, the `Backend` seam. |
| `@rindle/wasm` | The wasm backend — the in-process IVM engine. |
| `@rindle/replica` | Native (napi-rs) Node backend. |
| `@rindle/remote` | Network backend + wire protocol over a transport (`WsTransport`). |
| `@rindle/server` | Reference Node WebSocket server serving the `@rindle/remote` protocol. |
| `@rindle/normalized` | Normalized local-first glue: cross-query refcount + GC + footprint diffing. |
| `@rindle/optimistic` | Optimistic-writes glue: `createRindleClient`, named client mutators, the fork/rebase loop. |
| `@rindle/api-server` | App API-server helpers: query leases + custom mutator routing (the API tier). |
| `@rindle/daemon-client` | Low-level client for follower read/control APIs and write-master ingress. |
| `@rindle/sql-client` | SQL-surface client for the daemon's query endpoints. |
| `@rindle/react` | React bindings: context provider + `useQuery` with shared AST-keyed view retain/release. |
| `@rindle/tanstack` | TanStack Query/Router bindings. |
| `@rindle/narrator` · `@rindle/narrator-react` | Query narration — human-readable explanations of what a view is doing. |
| `@rindle/devtools` · `@rindle/react-devtools` | Inspect live views, their ASTs, and the change stream. |
| `@rindle/query-compiler` | Compiles named queries + fragments into approved ASTs. |
| `@rindle/affinity` | TypeScript twin of the signed follower-placement ticket format. |
| `@rindle/cli` · `create-rindle` | The `rindle` CLI and the project scaffolder. |

## Build with an AI agent

The docs are written to be read by coding agents as well as people, and they are served as raw
markdown — no scraping required:

| URL | What it is |
| --- | --- |
| [`rindle.sh/llms.txt`](https://rindle.sh/llms.txt) | [llmstxt.org](https://llmstxt.org) index — a link-per-line map of the whole site |
| [`rindle.sh/llms-app.txt`](https://rindle.sh/llms-app.txt) | the entire **build a synced app** track, concatenated into one fetch |
| [`rindle.sh/llms-engine.txt`](https://rindle.sh/llms-engine.txt) | the entire **use the engine** track, same shape |
| [`rindle.sh/llms-full.txt`](https://rindle.sh/llms-full.txt) | every page on the site, for ingestion / RAG |
| `rindle.sh/docs/<slug>.md` | any single doc page as clean markdown |

Claude Code users can install the plugin in [`claude-plugin/`](claude-plugin) — the
`building-rindle-apps` skill (the full canon, with eight reference files generated from those
same docs), a `/rindle:quickstart` command, a `rindle-reviewer` agent, and a write-time hook that
blocks removed APIs and non-deterministic mutator bodies *before* they land:

```
/plugin marketplace add rindle-sh/rindle
/plugin install rindle@rindle
```

## Develop

The Rust toolchain is pinned by [`rust/rust-toolchain.toml`](rust/rust-toolchain.toml). Run
cargo from `rust/` — the Cargo workspace is rooted there, with the `rindle` engine as the
workspace's own root package.

```sh
cd rust
cargo test --workspace --all-targets                      # the full battery
cargo test --workspace --features testkit --all-targets   # + the memory↔SQLite differential oracle
cargo check --target wasm32-unknown-unknown --no-default-features --features wasm
cargo clippy --workspace --all-targets -- -D warnings
```

A bare `cargo build`/`test` (no `-p`) operates on the **root `rindle` crate alone** — the
C-toolchain-free, wasm-clean core. That is deliberate: it is what the wasm target compiles.
Build the SQLite-linking crate explicitly with `-p rindle-sqlite`.

For the JavaScript side:

```sh
pnpm install
pnpm run build:wasm     # required on a fresh checkout — compiles rust/src to packages/wasm/pkg
pnpm test
```

`pnpm run build:wasm` is not optional on a fresh clone: the wasm artifact
(`packages/wasm/pkg/`) is a build product, not a checked-in file, and the JS packages run
against it. It needs [`wasm-bindgen-cli`](https://github.com/rustwasm/wasm-bindgen) and,
optionally, `wasm-opt` (binaryen ≥ 110).

Most packages test with Node's built-in runner under a `--conditions=@rindle/source` export
condition, so cross-package imports resolve to TypeScript **source** rather than built
`dist/` — you generally don't need `pnpm build` between edits.

## License

Apache-2.0 — see [LICENSE](./LICENSE) and [NOTICE](./NOTICE). Rindle is a Rust port of the ZQL
query/dataflow engine from Rocicorp's [Zero](https://github.com/rocicorp/mono), also
Apache-2.0; attribution is preserved in NOTICE.
