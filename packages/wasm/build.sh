#!/usr/bin/env bash
# Build the rindle wasm engine into this package as a portable ESM artifact by default
# (--target web): works in browsers/bundlers (await init()) and Node (await init(bytes)) — see
# src/index.ts.
#
#   ./build.sh [profile]   # wasm-release (default) | release | dev
#
# Produces packages/wasm/pkg/{rindle.js, rindle_bg.wasm, rindle.d.ts}.
# Advanced wrappers may override:
#   RINDLE_WASM_OUT             output directory
#   RINDLE_WASM_BINDGEN_TARGET  wasm-bindgen target (default: web)
set -euo pipefail

PROFILE="${1:-wasm-release}"
TARGET=wasm32-unknown-unknown
FEATURES=wasm
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# The Cargo workspace lives under rust/; the wasm artifact ships into this JS package's pkg/ (which
# stays at the repo root). Build from rust/ so `-p rindle` and target/ resolve; emit into packages/wasm/pkg.
RUST_ROOT="$ROOT/rust"
OUT="${RINDLE_WASM_OUT:-$ROOT/packages/wasm/pkg}"
BINDGEN_TARGET="${RINDLE_WASM_BINDGEN_TARGET:-web}"
export PATH="$HOME/.cargo/bin:$PATH"

cd "$RUST_ROOT"

# `-p rindle`: build ONLY the C-toolchain-free core crate for wasm32 — not the whole workspace
# (the SQLite members `rindle-sqlite`/`rindle-replica` and `rindle-fuzz`'s `getrandom` don't compile to
# `wasm32-unknown-unknown`, and a bare `cargo build` would try to).
#
# `cargo rustc … --crate-type cdylib`: the lib's default crate-type is `rlib` (so native builds
# don't emit a colliding `librindle.so`, cargo#6313); the `.wasm` artifact needs `cdylib`, requested
# here just for this wasm32 build. Produces `target/$TARGET/<profile>/rindle.wasm` either way.
case "$PROFILE" in
  dev)     cargo rustc -p rindle               --target "$TARGET" --no-default-features --features "$FEATURES" --crate-type cdylib; WASM="$RUST_ROOT/target/$TARGET/debug/rindle.wasm" ;;
  release) cargo rustc -p rindle --release      --target "$TARGET" --no-default-features --features "$FEATURES" --crate-type cdylib; WASM="$RUST_ROOT/target/$TARGET/release/rindle.wasm" ;;
  *)       cargo rustc -p rindle --profile "$PROFILE" --target "$TARGET" --no-default-features --features "$FEATURES" --crate-type cdylib; WASM="$RUST_ROOT/target/$TARGET/$PROFILE/rindle.wasm" ;;
esac

wasm-bindgen --target "$BINDGEN_TARGET" --out-dir "$OUT" --out-name rindle "$WASM"

if command -v wasm-opt >/dev/null 2>&1 && [ "$PROFILE" != "dev" ]; then
  # Guard the binaryen version. Since Rust 1.82, wasm32 enables the `reference-types` feature by
  # default, so wasm-bindgen emits a second (externref) table that `__wbindgen_init_externref_table`
  # grows by 4 at startup. binaryen < 110 has a multi-table export bug (WebAssembly/binaryen#4711,
  # fixed in v110) that corrupts that table — the optimized wasm then dies at runtime with
  # "WebAssembly.Table.grow(): failed to grow table by 4" (wasm-bindgen#4528). Ubuntu's apt ships an
  # old binaryen (22.04 -> 105, 24.04 -> 108), so fail loudly rather than ship a broken wasm.
  wo_ver="$(wasm-opt --version 2>/dev/null | grep -oE '[0-9]+' | head -1 || true)"
  if [ -n "$wo_ver" ] && [ "$wo_ver" -lt 110 ]; then
    echo "error: wasm-opt (binaryen) version $wo_ver is too old; need >= 110." >&2
    echo "       binaryen < 110 corrupts the reference-types externref table, so the wasm fails at" >&2
    echo "       runtime with 'WebAssembly.Table.grow(): failed to grow table by 4'." >&2
    echo "       Install a newer binaryen: https://github.com/WebAssembly/binaryen/releases" >&2
    exit 1
  fi
  wasm-opt -Oz "$OUT/rindle_bg.wasm" -o "$OUT/rindle_bg.wasm"
  echo "wasm-opt -Oz applied"
fi

echo "built $OUT  (target=$BINDGEN_TARGET, profile=$PROFILE, $(wc -c < "$OUT/rindle_bg.wasm") bytes)"
