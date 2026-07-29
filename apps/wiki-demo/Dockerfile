# The wiki-demo "tiny machine" image: the ONE topology (design 214) as a COLOCATED pair (Rust
# write-master + read-follower) PLUS the Node tier (API server + Wikimedia ingester), all in ONE
# container. The Node tier is the single front door — it serves the lease/metrics HTTP and
# reverse-proxies the public ws to the local FOLLOWER — so the image exposes exactly one port; both
# engines' control planes + the master's fan-out ws stay private on localhost (`src/main.ts` spawns
# the pair over loopback via `src/daemon.ts` `startPair`; writes → master, reads → follower).
#
# Build context = repo root (see .dockerignore). Built/deployed by `fly deploy` via
# apps/wiki-demo/fly.toml. glibc is matched across stages (bookworm) so the release binaries that
# `src/main.ts` starts run on the final image.

# syntax=docker/dockerfile:1

# ── stage 1: the pair's release binaries ─────────────────────────────────────
# Full `rust:1-bookworm` (has gcc) — the vendored wal2 SQLite amalgamation compiles from C with
# prebuilt bindings (no libclang needed; see vendor/ + the root Cargo.toml [patch.crates-io]).
FROM rust:1-bookworm AS rust
WORKDIR /build
COPY . .
# The Cargo workspace lives under rust/ in the build context (target/ → /build/rust/target).
WORKDIR /build/rust
# `--features metrics` compiles in the engine atomic counters the `/metrics` endpoints render;
# `rindle-server/analyze` arms `POST /analyze`'s per-source SQLite `scanned` column. BOTH binaries:
# the follower (`rindled`) and the write-master (`rindle-replicator`).
RUN cargo build --release \
      --features rindle-replicator/metrics,rindle-server/metrics,rindle-server/analyze \
      -p rindle-replicator --bin rindle-replicator \
      -p rindle-server --bin rindled

# ── stage 2: install the JS workspace ───────────────────────────────────────
# The Node tier's runtime tree is TS-source workspace packages loaded via the @rindle/source
# condition (api-server → client + daemon-client + sql-client + query-compiler; wiki-demo wires
# them). The pnpm workspace root is now the repo root, so we copy the workspace manifests + the JS
# members and install only the wiki-demo subtree (`--filter @rindle/wiki-demo...` = the app + its
# workspace deps). The product-page / headwaters / rust/rindle-replica-node package.json files are
# copied only so the frozen lockfile validates against the full member set — their sources are
# .dockerignore-excluded / not needed here and never installed (rindle-replica-node is a napi member
# that lives in the Cargo tree).
#
# `--ignore-scripts`: api-server → @rindle/sql-client peers on drizzle-orm, whose OPTIONAL
# better-sqlite3 peer resolves against the better-sqlite3 already in the shared lockfile (headwaters
# uses it), so the filtered install pulls in that native module. The wiki-demo SERVER tier never
# loads it (the SQLite work is done by the Rust rindled/replicator binaries; no `packages/*/src`
# imports better-sqlite3), so we skip its node-gyp build rather than add Python + a C toolchain to
# this slim image for a module that's never required at runtime.
FROM node:24-bookworm-slim AS node
RUN npm install -g pnpm@11.2.2
WORKDIR /app
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json ./
COPY packages/ ./packages/
COPY apps/ ./apps/
COPY product-page/package.json ./product-page/
COPY headwaters/package.json ./headwaters/
COPY rust/rindle-replica-node/package.json ./rust/rindle-replica-node/
RUN pnpm install --frozen-lockfile --ignore-scripts --filter @rindle/wiki-demo...

# ── final image ─────────────────────────────────────────────────────────────
FROM node:24-bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=rust /build/rust/target/release/rindled /app/rindled
COPY --from=rust /build/rust/target/release/rindle-replicator /app/rindle-replicator
# The installed workspace: the hoisted root node_modules (pnpm symlink farm) + the linked TS member
# sources. Copy the whole /app tree so the relative symlinks under node_modules/.pnpm resolve intact.
COPY --from=node /app /app

# The deployed commit, stamped by `fly deploy --build-arg GIT_SHA=...` (see deploy.sh) and served
# by the Node tier at GET /version for the top-level status script. Last so it never busts the
# cargo/pnpm layers above; defaults to "unknown" for a plain `docker build`.
ARG GIT_SHA=unknown
ENV RINDLE_GIT_SHA=$GIT_SHA

# Node 24 runs the TS workspace source directly (native type stripping) via the @rindle/source
# condition. main.ts spawns the colocated PAIR (/app/rindle-replicator write-master + /app/rindled
# follower, both on loopback) with the fixed demo schema as the master's base `tables`, then starts
# the API/ingester tier (writes → master, reads → follower). The two SQLite db files + the resume
# offset live on the mounted volume (/data). nWorkers=3 matches the pinned queries.
ENV RINDLED_BIN=/app/rindled \
    RINDLE_REPLICATOR_BIN=/app/rindle-replicator \
    WIKI_DATA_DIR=/data \
    WIKI_SOURCE=wikimedia \
    WIKI_API_PORT=7700 \
    WIKI_NWORKERS=3 \
    NODE_ENV=production

EXPOSE 7700
CMD ["node", "--conditions=@rindle/source", "/app/apps/wiki-demo/src/main.ts"]
