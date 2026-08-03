# Deploying the wiki-demo

This is a **worked self-host example**: the rindle server tier (the colocated write-master +
read-follower pair, the ingester, and the API server) packaged as one container you run on a box you
own — here, a single [Fly](https://fly.io) machine. Nothing about rindle requires Fly, and nothing
here is used by the managed control plane (Headwaters), which provisions on its own infrastructure —
this is just a small, complete recipe for standing the same tier up yourself.

The live demo is two pieces:

- **Cloudflare** — `product-page` (the static site) + a Worker that reverse-proxies
  `rindle.sh/demo/wiki/*` to the Fly app. Config: `product-page/{wrangler.jsonc,worker.js,.env.production}`.
- **Fly** — one always-on machine running the colocated `rindle-replicator` write-master +
  `rindled` read-follower pair, the Wikimedia ingester, and the lease/metrics API server (this
  directory's `Dockerfile` + `fly.toml`), with a volume for both database files and the stream
  resume offset.

The Node tier is the single exposed origin (lease/metrics HTTP + a `/ws` reverse proxy to the
local follower), so the follower's control plane/ws and the master's write/fan-out planes stay
private on localhost. The Cloudflare
Worker's `WIKI_ORIGIN` (`https://rindle-wiki-demo.fly.dev`) must match the Fly app name below.

> **Wire-version note:** the browser client and this server tier must agree on
> `COMPARATOR_VERSION` (see `rust/src/wire_schema.rs`) — the client rejects a subscription on a
> mismatch (`comparator version X != Y`). So rebuild + redeploy this tier from the **same commit**
> the frontend was built from; a stale image is refused, not silently tolerated.

## One-time setup

```sh
# Fly: create the app + the data volume (in the same region as fly.toml's primary_region)
fly apps create rindle-wiki-demo
fly volumes create wiki_data --app rindle-wiki-demo --region iad --size 1 -y

# Optional hardening — the control plane is already private (localhost only), so this is
# belt-and-suspenders for the daemon bearer token:
# fly secrets set --app rindle-wiki-demo WIKI_DAEMON_TOKEN="$(openssl rand -hex 16)"
```

Cloudflare needs no per-deploy setup beyond `wrangler login` (the account id is pinned in
`product-page/wrangler.jsonc`).

## Deploy (both pieces)

Run deploy commands from the repo root so Fly uses the full workspace as the Docker build context.

```sh
apps/wiki-demo/deploy.sh        # Fly first (the origin), then Cloudflare (page + proxy)
apps/wiki-demo/deploy.sh fly    # backend only; creates a new image and resets demo data
apps/wiki-demo/deploy.sh cloudflare # frontend/proxy only; does not touch backend data
```

The selector matters when retrying a partial deploy: if Fly succeeded and Cloudflare failed, retry
with `apps/wiki-demo/deploy.sh cloudflare`. Running the Fly step again creates another image ref and
therefore intentionally resets the demo again.

Or invoke each tool directly:

```sh
fly deploy --config apps/wiki-demo/fly.toml        # Fly only
cd product-page && pnpm deploy                       # Cloudflare only (page + /demo/wiki proxy)
```

## Verify

```sh
fly logs --app rindle-wiki-demo
#   [wiki] pair up — follower control … public ws …; master write … (boot …)
#   [wiki] starting "wikimedia" live (no stored offset)

curl https://rindle-wiki-demo.fly.dev/metrics
#   {"pages":…,"editors":…,"edits":…,"editsInWindow":…,"materializations":…,"source":"wikimedia",…}
#   (materializations = the pinned boards, shared by every tab, plus a tiny per-client bookkeeping query)

# then open the page (same-origin through the Worker):
#   https://rindle.sh/wiki
```

The board fills within a minute or two on first boot. Ordinary restarts of the same image resume
from `/data/.offset` with no gap. A new Fly deployment image intentionally removes only the known
wiki database/config/offset files and backfills a fresh window; its history is disposable, and this
prevents stale engine/topology state from surviving an upgrade. Unrelated files on the volume are
never removed.

## Operate

- **Resize:** `fly scale vm shared-cpu-2x --memory 1024 --app rindle-wiki-demo` — a clean reconnect
  (the daemon holds no durable lease state; clients re-lease + re-hydrate).
- **Grow the volume:** `fly volumes extend <id> --size 3` (online; never shrinks).
- **Schema:** the fixed demo schema is inlined in `src/daemon.ts` as the master's base tables. The
  master's genesis DDL creates the bare follower; the Node tier then applies its versioned workload
  indexes through the master and waits for them to replicate before pin hydration.
- **Retention:** the master reclaims its HCTree journal to the colocated follower's durable cursor
  every minute. The two Rust metrics planes are separate (`9091` follower, `9092` master), so Fly
  can alert on follower/query and master/write/retention health independently.
- **Knobs** (env in `fly.toml`): `WIKI_WINDOW_MIN` (rolling prune window), `WIKI_NWORKERS`,
  `WIKI_DOMAIN` (which wiki to tail), `WIKI_SOURCE=synthetic` (offline, for debugging without
  egress).
- **Egress:** the ingester needs outbound to `stream.wikimedia.org` (open on Fly by default).

For a production-shaped local profile, build and run both release engines and sample each process
separately:

```sh
pnpm --filter @rindle/wiki-demo profile -- 420 wikimedia
# elapsed_s, Node/master/follower RSS + CPU, and the bounded-window app counters are emitted as CSV
```

Use `synthetic` instead of `wikimedia` for a deterministic run without network access.

## Self-hosting elsewhere

Nothing above is Fly-specific except `fly.toml` and the `fly` CLI calls. The `Dockerfile` is a
plain multi-stage build (build context = repo root) that emits one image exposing a single port
(`WIKI_API_PORT`, 7700) and expecting a writable `WIKI_DATA_DIR` volume. To run it on any other
host — a VM, your own OrchestratOR, a laptop — `docker build -f apps/wiki-demo/Dockerfile .` from
the repo root, mount a data volume at `/data`, publish port 7700, and give the container outbound
access to `stream.wikimedia.org`. Point whatever reverse proxy fronts it (or `WIKI_ORIGIN`) at that
port.
