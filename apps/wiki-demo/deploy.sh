#!/usr/bin/env bash
# One command for both pieces: deploy the Fly server tier, then the Cloudflare page + proxy.
# Run after the one-time setup in README.md. Requires `fly` (authed) and `wrangler` (authed).
#
#   apps/wiki-demo/deploy.sh [all|fly|cloudflare]
#
# Order matters: bring up Fly first (the origin), then Cloudflare (the page + /demo/wiki proxy that
# points at it). Cloudflare-only redeploys: `cd product-page && pnpm deploy`.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

TARGET="${1:-all}"
case "$TARGET" in
  all | fly | cloudflare) ;;
  *)
    echo "usage: apps/wiki-demo/deploy.sh [all|fly|cloudflare]" >&2
    exit 2
    ;;
esac

# Stamp the deployed commit into the image (Dockerfile ARG → RINDLE_GIT_SHA → GET /version) so the
# top-level status script (./status.sh) can tell current from behind.
GIT_SHA="$(git rev-parse HEAD)"

if [ "$TARGET" = "all" ] || [ "$TARGET" = "fly" ]; then
  echo "==> Fly: rindled + Wikimedia ingester + api-server (commit ${GIT_SHA:0:12})"
  fly deploy --config apps/wiki-demo/fly.toml --build-arg GIT_SHA="$GIT_SHA"
fi

if [ "$TARGET" = "all" ] || [ "$TARGET" = "cloudflare" ]; then
  echo "==> Cloudflare: product-page (static) + the /demo/wiki reverse proxy"
  ( cd product-page && pnpm deploy )
fi

echo "==> ${TARGET} deploy complete — live at https://rindle.sh/wiki"
