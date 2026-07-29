import { strict as assert } from "node:assert";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  binaryName,
  binaryPath,
  platformKey,
  platformPackageName,
  rindleBinaryPath,
  rindledBinaryPath,
  UnsupportedPlatformError,
} from "../src/index.ts";

// Save/restore every override env so tests don't leak into each other (or the runner's env).
const OVERRIDES = [
  "RINDLE_BINARY_PATH",
  "RINDLED_BINARY_PATH",
  "RINDLE_REPLICATOR_BINARY_PATH",
  "RINDLE_DEV_EDGE_BINARY_PATH",
  "RINDLE_BIN_DIR",
] as const;
function withEnv(vars: Partial<Record<(typeof OVERRIDES)[number], string | undefined>>, fn: () => void) {
  const prev = Object.fromEntries(OVERRIDES.map((k) => [k, process.env[k]]));
  try {
    for (const k of OVERRIDES) delete process.env[k];
    for (const [k, v] of Object.entries(vars)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
    fn();
  } finally {
    for (const k of OVERRIDES) {
      if (prev[k] === undefined) delete process.env[k];
      else process.env[k] = prev[k];
    }
  }
}

test("platformKey maps the shipped targets (incl. linux arm64 + musl)", () => {
  assert.equal(platformKey("darwin", "arm64"), "darwin-arm64");
  assert.equal(platformKey("darwin", "x64"), "darwin-x64");
  assert.equal(platformKey("linux", "x64", "gnu"), "linux-x64-gnu");
  assert.equal(platformKey("linux", "arm64", "gnu"), "linux-arm64-gnu");
  assert.equal(platformKey("linux", "x64", "musl"), "linux-x64-musl");
  assert.equal(platformKey("linux", "arm64", "musl"), "linux-arm64-musl");
});

// Windows ships no binaries (the storage engine is Linux-only), so the resolver must reject it
// rather than name a package that was never published. Pinned with the WSL2 hint because that
// error message IS the Windows onboarding path — see follow-ups/windows-hctree.md.
test("win32 is rejected and points at WSL2 rather than naming an unpublished package", () => {
  assert.throws(() => platformKey("win32", "x64"), UnsupportedPlatformError);
  assert.throws(() => platformKey("win32", "x64"), /WSL2/);
  assert.throws(() => platformKey("win32", "x64"), /not.*\/mnt\/c/i);
});

test("platformPackageName / binaryName cover the native toolchain", () => {
  assert.equal(platformPackageName("linux-x64-gnu"), "@rindle/cli-linux-x64-gnu");
  assert.equal(binaryName("rindled", "win32"), "rindled.exe");
  assert.equal(binaryName("rindle", "win32"), "rindle.exe");
  assert.equal(binaryName("rindle-dev-edge", "win32"), "rindle-dev-edge.exe");
  assert.equal(binaryName("rindled", "linux"), "rindled");
  assert.equal(binaryName("rindle", "darwin"), "rindle");
});

test("unsupported platform throws UnsupportedPlatformError", () => {
  assert.throws(() => platformKey("aix" as NodeJS.Platform, "ppc64"), UnsupportedPlatformError);
});

test("<BIN>_BINARY_PATH overrides resolution and is existence-checked", () => {
  const dir = mkdtempSync(join(tmpdir(), "rindle-test-"));
  const rindle = join(dir, "rindle");
  const rindled = join(dir, "rindled");
  writeFileSync(rindle, "#!/bin/sh\nexit 0\n");
  writeFileSync(rindled, "#!/bin/sh\nexit 0\n");

  withEnv({ RINDLE_BINARY_PATH: rindle, RINDLED_BINARY_PATH: rindled }, () => {
    assert.equal(rindleBinaryPath(), rindle);
    assert.equal(rindledBinaryPath(), rindled);
    assert.equal(binaryPath("rindle"), rindle);
  });

  withEnv({ RINDLED_BINARY_PATH: join(dir, "does-not-exist") }, () => {
    assert.throws(() => rindledBinaryPath(), /missing file/);
  });
});

test("RINDLE_BIN_DIR resolves all binaries co-located", () => {
  const dir = mkdtempSync(join(tmpdir(), "rindle-bindir-"));
  // Mirror what `cargo build` lays into target/release for the host (no .exe off win32).
  for (const bin of ["rindle", "rindled", "rindle-replicator", "rindle-dev-edge"] as const) {
    writeFileSync(join(dir, binaryName(bin)), "#!/bin/sh\nexit 0\n");
  }

  withEnv({ RINDLE_BIN_DIR: dir }, () => {
    assert.equal(binaryPath("rindle"), join(dir, binaryName("rindle")));
    assert.equal(binaryPath("rindled"), join(dir, binaryName("rindled")));
    assert.equal(binaryPath("rindle-dev-edge"), join(dir, binaryName("rindle-dev-edge")));
  });
});
