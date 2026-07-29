#!/usr/bin/env bash
# One command for both pieces: deploy the Fly server tier, then the Cloudflare page + proxy.
# Run after the one-time setup in README.md. Requires `fly` (authed) and `wrangler` (authed).
#
#   apps/wiki-demo/deploy.sh
#
# Order matters: bring up Fly first (the origin), then Cloudflare (the page + /demo/wiki proxy that
# points at it). Cloudflare-only redeploys: `cd product-page && pnpm deploy`.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

# Stamp the deployed commit into the image (Dockerfile ARG → RINDLE_GIT_SHA → GET /version) so the
# top-level status script (./status.sh) can tell current from behind.
GIT_SHA="$(git rev-parse HEAD)"

echo "==> [1/2] Fly: rindled + Wikimedia ingester + api-server (commit ${GIT_SHA:0:12})"
fly deploy --config apps/wiki-demo/fly.toml --build-arg GIT_SHA="$GIT_SHA"

echo "==> [2/2] Cloudflare: product-page (static) + the /demo/wiki reverse proxy"
( cd product-page && pnpm deploy )

echo "==> deployed — live at https://rindle.sh/wiki"
