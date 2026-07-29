#!/usr/bin/env node
// The `rindle` CLI bin (package.json "bin" → dist/cli.js; tsc preserves this shebang). Resolves the
// prebuilt `rindle` binary for this host and execs it, forwarding argv + stdio and propagating its
// exit code (or re-raising its terminating signal). The daemon (`rindled`) is intentionally NOT a
// bin — run the normal app lifecycle via `rindle dev`, the fleet alone via `rindle up`, or embed it
// with `spawnRindled()`; its binary still ships co-located so both supervisors find it.
import { spawnSync } from "node:child_process";

import { binaryPath, UnsupportedPlatformError } from "./index.ts";

let bin: string;
try {
  bin = binaryPath("rindle");
} catch (err) {
  process.stderr.write(`rindle: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(err instanceof UnsupportedPlatformError ? 64 : 1); // 64 = EX_USAGE
}

const { status, signal, error } = spawnSync(bin, process.argv.slice(2), { stdio: "inherit" });
if (error) {
  process.stderr.write(`rindle: failed to launch ${bin}: ${error.message}\n`);
  process.exit(1);
}
if (typeof status === "number") {
  process.exit(status);
}
// Killed by a signal — re-raise it on ourselves so the parent observes the same cause.
process.kill(process.pid, signal ?? "SIGTERM");
