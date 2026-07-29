# @rindle/cli — the Rindle toolchain, prebuilt for npm

The Rindle CLI and local fleet components ship as prebuilt, per-platform binaries you can
`npm install`. This is how a JS/TS developer runs Rindle locally:

- **`rindled`** — the Rindle **read-follower** server: the `rindle-server` crate (the
  SQLite-backed `rindle-replica` live-query engine plus the public
  subscription/lease plane). It tails a `rindle-replicator` write-master and has no write
  ingress of its own.
- **`rindle`** — the CLI to inspect, run, and manage the data tier: `rindle status` / `migrate` /
  `schema`, plus **`rindle init`** (scaffold `rindle.ncl`), **`rindle up`** (fleet-only
  supervision), and **`rindle dev`** (the complete fleet/bootstrap/app lifecycle).
- **`rindle-dev-edge`** — the development-only stable HTTP/WebSocket fleet endpoint. `rindle up`
  supervises it automatically; applications never install or launch it directly.

This is the network counterpart of [`@rindle/replica`](../replica): both run the same
`rindle-replica` engine, but `@rindle/replica` is a napi addon that embeds it **in-process**,
whereas `rindled` is the daemon **executable** (a read-follower) that serves many clients over
the wire. Talk to a running daemon from JS with [`@rindle/daemon-client`](../daemon-client).

```sh
npm i -D @rindle/cli
npx rindle init      # scaffold rindle.ncl (the colocated pair) + migrations/
npx rindle dev --migrate --gen shared/schema.gen.ts -- vite dev
# evaluate rindle.ncl once; supervise + wait for the fleet; migrate + generate/watch schema;
# launch the app with RINDLE_URL + RINDLE_DATABASE_TOKEN; own signals and teardown
```

Installing pulls in exactly one prebuilt-binary package for your platform via
`optionalDependencies` (the esbuild/napi pattern) — nothing is compiled, and the other platforms'
binaries are never downloaded.

## Docs

Full docs — the command reference, flags & env vars, the local dev loop, and remote-daemon usage:
**[rindle.sh/docs/rindle-cli](https://rindle.sh/docs/rindle-cli)** · markdown mirror:
[`rindle-cli.md`](https://rindle.sh/docs/rindle-cli.md) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

## Embedding the daemon in your own supervisor

`rindled` is **not** exposed as a CLI command here — `rindle dev` is the normal app-development
entry point, while `rindle up` runs the local fleet by itself.
The binary still ships (co-located with `rindle`), so for embedding a follower in your own Node
supervisor there are programmatic helpers:

```ts
import { spawnRindled, rindledBinaryPath } from "@rindle/cli";

const child = spawnRindled(["--config", "./follower.json"]); // inherits stdio
// …or just locate the binary and manage the process yourself:
const bin = rindledBinaryPath();
```

> Running the daemon directly under an external supervisor (systemd / Docker / k8s) in production is
> a job for the cargo-dist installers or a container image, not npm.

## How the packaging works

- **`@rindle/cli`** (this package) is a tiny, dependency-free launcher. Its `bin/rindle`
  (`dist/cli.js`) resolves the matching `rindle` binary for the host and execs it, forwarding
  argv/stdio and the exit code.
- **`@rindle/cli-<key>`** — one package per target (`darwin-arm64`, `darwin-x64`, `linux-x64-gnu`,
  `linux-arm64-gnu`, `linux-x64-musl`, `linux-arm64-musl`, `win32-x64-msvc`), each carrying
  `rindle`, `rindled`, `rindle-replicator`, and `rindle-dev-edge` for that platform under `bin/`,
  gated by `os`/`cpu`/`libc`. They're listed as `optionalDependencies` of this package, so the
  installer fetches only the matching one. They are **generated at release time** (like the napi
  platform packages for `@rindle/replica`), never committed.

All four binaries ship **co-located** on purpose: even though only `rindle` is an npm bin,
`rindle up` finds every supervised component beside its own executable.

Resolution order at runtime (`src/index.ts`):

1. The binary-specific `*_BINARY_PATH` override (`RINDLE_BINARY_PATH`, `RINDLED_BINARY_PATH`,
   `RINDLE_REPLICATOR_BINARY_PATH`, or `RINDLE_DEV_EDGE_BINARY_PATH`).
2. `RINDLE_BIN_DIR` — a directory holding the native toolchain (see dev usage below).
3. the installed `@rindle/cli-<key>` optional dependency.

## Local development (in this monorepo)

No platform packages are installed in the workspace, so build the binaries and point the launcher at
the build directory — one env var lights up the whole fleet:

```sh
cargo build -p rindle-cli -p rindle-server -p rindle-replicator -p rindle-dev-edge --bins --release
RINDLE_BIN_DIR="$PWD/target/release" npx rindle dev -- vite dev # finds every component beside it
RINDLE_BIN_DIR="$PWD/target/release" npx rindle status
```

(Or set `RINDLE_BINARY_PATH` / `RINDLED_BINARY_PATH` to point at one binary each — e.g.
`RINDLED_BINARY_PATH` is what `spawnRindled()` / `rindledBinaryPath()` resolve in dev.)

## Releasing

The native binaries come from dist (cargo-dist): all four binary crates opt into dist, which uploads
their archives plus `dist-manifest.json` to a GitHub release.
`scripts/build-npm-packages.mjs` is the bridge from those artifacts to npm:

```sh
# from a published release (reads its dist-manifest.json — no archive-name guessing):
node scripts/build-npm-packages.mjs --from-release rindle-cli-v0.1.0 --update-root

# …or from locally available binaries laid out as <dir>/<rust-target>/<bin>[.exe]:
node scripts/build-npm-packages.mjs --bin-dir ./bins --version 0.1.0 --update-root
```

`--update-root` writes this package's `version` + `optionalDependencies` in lockstep. Then publish
each `npm/<key>` package, then the umbrella. (The binaries + targets live in `package.json` under
`"rindle"`, the single source of truth shared by the generator and the runtime resolver.)
