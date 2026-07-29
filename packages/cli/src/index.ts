// @rindle/cli — the Rindle CLI and supervised local-fleet components as prebuilt npm binaries.
//
// `rindled` is the *network front* of the engine: the `rindle-server` crate — the SQLite-backed
// `rindle-replica` live-query engine plus the public subscription/lease plane. `rindle` is the CLI
// that inspects and manages a deployed daemon (`rindle status`/`migrate`/…) and, for local dev,
// scaffolds and supervises one (`rindle init` / `rindle dev` / `rindle up`). Where
// `@rindle/replica` is a napi
// addon that embeds the engine *in-process*, this package ships the standalone executables,
// prebuilt per platform by dist (cargo-dist), **co-located** so `rindle dev` finds every component
// beside it. This module resolves the right binary for the host; the `rindle` bin (dist/cli.js)
// execs the CLI. `rindle dev` owns the normal app lifecycle, `rindle up` runs only the fleet, and
// `spawnRindled()` embeds a follower in another supervisor (it is not exposed as its own bin).
import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

import {
  binaryName,
  platformKey,
  platformPackageName,
  UnsupportedPlatformError,
  type Binary,
} from "./platform.ts";

export {
  binaryName,
  platformKey,
  platformPackageName,
  UnsupportedPlatformError,
} from "./platform.ts";
export type { Binary, Libc } from "./platform.ts";

const require = createRequire(import.meta.url);

/**
 * Absolute path to a native Rindle toolchain binary for the current platform.
 *
 * Resolution order:
 *  1. The binary's explicit `*_BINARY_PATH` override.
 *  2. `RINDLE_BIN_DIR` — a directory holding the co-located toolchain. Point it at
 *     `target/release` after building the four binary crates and one env var lights up the fleet.
 *  3. The matching `@rindle/cli-<key>` optional dependency that npm/pnpm installed.
 *
 * Throws `UnsupportedPlatformError` for an un-targeted host, or a descriptive Error if the
 * platform package isn't installed (e.g. installed with `--no-optional`, or on a host the last
 * publish didn't target) and no override is set.
 */
export function binaryPath(bin: Binary): string {
  const explicitName: Record<Binary, string> = {
    rindle: "RINDLE_BINARY_PATH",
    rindled: "RINDLED_BINARY_PATH",
    "rindle-replicator": "RINDLE_REPLICATOR_BINARY_PATH",
    "rindle-dev-edge": "RINDLE_DEV_EDGE_BINARY_PATH",
  };
  const explicit = process.env[explicitName[bin]];
  if (explicit) {
    if (!existsSync(explicit)) {
      throw new Error(`${explicitName[bin]} points at a missing file: ${explicit}`);
    }
    return explicit;
  }

  const file = binaryName(bin);
  const binDir = process.env.RINDLE_BIN_DIR;
  if (binDir) {
    const candidate = join(binDir, file);
    if (existsSync(candidate)) return candidate;
    // Fall through to the installed package — `RINDLE_BIN_DIR` may legitimately hold only one bin.
  }

  const key = platformKey(); // throws UnsupportedPlatformError for un-targeted hosts
  const pkg = platformPackageName(key);
  let manifest: string;
  try {
    manifest = require.resolve(`${pkg}/package.json`);
  } catch {
    throw new Error(
      `${pkg} is not installed — the prebuilt Rindle binaries for "${key}" are missing.\n` +
        `They ship as an optional dependency of @rindle/cli; reinstall without --no-optional, ` +
        `or set RINDLE_BIN_DIR to a directory of locally built binaries.`,
    );
  }
  return join(dirname(manifest), "bin", file);
}

/** Absolute path to the `rindle` CLI binary for the current platform. See {@link binaryPath}. */
export function rindleBinaryPath(): string {
  return binaryPath("rindle");
}

/** Absolute path to the `rindled` daemon binary for the current platform. See {@link binaryPath}. */
export function rindledBinaryPath(): string {
  return binaryPath("rindled");
}

/** Spawn `bin` with `args`, inheriting stdio by default. A thin wrapper over `child_process.spawn`. */
export function spawnBinary(bin: Binary, args: string[] = [], options: SpawnOptions = {}): ChildProcess {
  return spawn(binaryPath(bin), args, { stdio: "inherit", ...options });
}

/** Spawn the `rindle` CLI with `args`. See {@link spawnBinary}. */
export function spawnRindle(args: string[] = [], options: SpawnOptions = {}): ChildProcess {
  return spawnBinary("rindle", args, options);
}

/** Spawn the `rindled` daemon with `args`, for embedding it in a Node supervisor. See {@link spawnBinary}. */
export function spawnRindled(args: string[] = [], options: SpawnOptions = {}): ChildProcess {
  return spawnBinary("rindled", args, options);
}
