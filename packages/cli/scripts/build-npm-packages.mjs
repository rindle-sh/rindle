#!/usr/bin/env node
// Build the per-platform npm packages for @rindle/cli from dist's (cargo-dist) prebuilt Rindle
// binaries. Each configured target (package.json "rindle".targets) becomes a tiny
// `@rindle/cli-<key>` package carrying that platform's binaries (package.json "rindle".binaries —
// `rindle` + `rindled` + `rindle-replicator` + `rindle-dev-edge`) **co-located** under `bin/`, gated
// by `os`/`cpu` (and `libc` on linux). Co-location is required: `rindle up` finds every native
// component as a sibling of the CLI executable.
// The umbrella @rindle/cli lists every per-platform package as optionalDependencies, so npm/pnpm
// install only the one matching the host — the esbuild/napi "prebuilt binary per platform" pattern.
// The umbrella's `rindle` bin (dist/cli.js) execs whichever got installed; the supervised follower,
// write-master, and dev-edge ship co-located but are not exposed as npm bins.
//
// Binary source (one required):
//   --bin-dir <dir>       use local binaries laid out as <dir>/<rust-target>/<bin>[.exe]
//                         (hand-assembled, or copied out of `dist build` output). Simple + offline.
//   --from-release <tag>  download from the GitHub release <tag>, driven by the release's
//                         dist-manifest.json (no archive-name guessing). Needs network + `tar`.
//
// Options:
//   --version <v>     version for the generated packages (default: the umbrella's version)
//   --out <dir>       output directory (default: <package>/npm)
//   --repo <o/r>      GitHub repo for --from-release (default: package.json "rindle".repository)
//   --update-root     also write the umbrella's optionalDependencies + version in place
//                     (release prep). Without it, the optionalDependencies block is just printed.
//
// Examples:
//   node scripts/build-npm-packages.mjs --bin-dir /tmp/bins --version 0.1.0
//   node scripts/build-npm-packages.mjs --from-release rindle-cli-v0.1.0 --update-root
import { spawnSync } from "node:child_process";
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const umbrella = JSON.parse(readFileSync(join(pkgRoot, "package.json"), "utf8"));
const { binaries, targets, repository: defaultRepo } = umbrella.rindle ?? {};
if (!Array.isArray(binaries) || binaries.length === 0) {
  fail('package.json "rindle".binaries is missing or empty');
}
if (!Array.isArray(targets) || targets.length === 0) {
  fail('package.json "rindle".targets is missing or empty');
}

const args = parseArgs(process.argv.slice(2));
const version = args["version"] ?? umbrella.version;
const outDir = resolve(args["out"] ?? join(pkgRoot, "npm"));
const repo = args["repo"] ?? defaultRepo;
const binDir = args["bin-dir"] ? resolve(args["bin-dir"]) : undefined;
const releaseTag = args["from-release"];

if (!binDir && !releaseTag) fail("provide a binary source: --bin-dir <dir> or --from-release <tag>");
if (binDir && releaseTag) fail("--bin-dir and --from-release are mutually exclusive");

// The `<key>` scheme — MUST mirror src/platform.ts (the runtime resolver) exactly.
function keyFor(t) {
  if (t.os === "darwin") return `darwin-${t.cpu}`;
  if (t.os === "linux") return `linux-${t.cpu}-${t.libc ?? "gnu"}`;
  if (t.os === "win32") return `win32-${t.cpu}-msvc`;
  fail(`unsupported os in targets config: ${t.os}`);
}

const exeName = (bin, os) => (os === "win32" ? `${bin}.exe` : bin);

const manifest = releaseTag ? await fetchDistManifest(repo, releaseTag) : null;
const optionalDependencies = {};

rmSync(outDir, { recursive: true, force: true });
for (const t of targets) {
  const key = keyFor(t);
  const name = `@rindle/cli-${key}`;
  const dest = join(outDir, key);
  mkdirSync(join(dest, "bin"), { recursive: true });

  // Download cache (per target): an artifact archive may carry more than one binary, so extract
  // each unique archive once. `--bin-dir` ignores it (binaries are already on disk).
  const archiveCache = new Map();
  for (const bin of binaries) {
    const exe = exeName(bin, t.os);
    const srcBinary = binDir
      ? requireFile(join(binDir, t.rust, exe), `${bin} binary for ${t.rust}`)
      : await downloadBinary(manifest, repo, releaseTag, t.rust, bin, exe, archiveCache);
    cpSync(srcBinary, join(dest, "bin", exe));
    if (t.os !== "win32") chmodSync(join(dest, "bin", exe), 0o755);
  }

  // npm manifest `os`/`cpu`/`libc` are arrays; the `libc` *field* uses glibc/musl (the package
  // *name* keeps the napi-style `-gnu`/`-musl` suffix), so map the key's libc to the field value.
  const pkg = {
    name,
    version,
    description: `Prebuilt Rindle binaries (${binaries.join(", ")}) for ${key}.`,
    license: "Apache-2.0",
    repository: repo,
    os: [t.os],
    cpu: [t.cpu],
    ...(t.os === "linux" ? { libc: [t.libc === "musl" ? "musl" : "glibc"] } : {}),
    files: ["bin/"],
    engines: { node: ">=22" },
  };
  writeFileSync(join(dest, "package.json"), JSON.stringify(pkg, null, 2) + "\n");
  optionalDependencies[name] = version;
  console.log(`  ✓ ${name}@${version}  (${binaries.map((b) => exeName(b, t.os)).join(", ")})`);
}

const sortedOptionalDeps = Object.fromEntries(Object.entries(optionalDependencies).sort());
if (args["update-root"]) {
  umbrella.version = version;
  umbrella.optionalDependencies = sortedOptionalDeps;
  writeFileSync(join(pkgRoot, "package.json"), JSON.stringify(umbrella, null, 2) + "\n");
  console.log(`\nupdated umbrella package.json → version ${version} + optionalDependencies.`);
  if (umbrella.private) console.log('NOTE: umbrella is still "private": true — clear it to publish.');
} else {
  console.log(`\nGenerated ${targets.length} package(s) in ${outDir}.`);
  console.log("Add to @rindle/cli (or re-run with --update-root):");
  console.log(JSON.stringify({ version, optionalDependencies: sortedOptionalDeps }, null, 2));
}
console.log("\nTo publish: `npm publish` each npm/<key>, then publish the umbrella.");

// --- helpers ---------------------------------------------------------------

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) fail(`unexpected argument: ${a}`);
    const flag = a.slice(2);
    if (flag === "update-root") out[flag] = true;
    else out[flag] = argv[++i];
  }
  return out;
}

function requireFile(p, what) {
  if (!existsSync(p)) fail(`missing ${what}: ${p}`);
  return p;
}

async function fetchDistManifest(repo, tag) {
  if (!repo) fail("--from-release needs a repo (--repo or package.json rindle.repository)");
  const url = `https://github.com/${repo}/releases/download/${tag}/dist-manifest.json`;
  const res = await fetch(url);
  if (!res.ok) fail(`could not fetch dist-manifest.json (${res.status}) at ${url}`);
  return res.json();
}

// Find the executable-archive artifact carrying `bin` for `rustTarget` in the dist manifest,
// download+extract it (once per archive via `archiveCache`), and return the path to the binary.
// cargo-dist puts each dist-enabled package's binary in its own archive, so `rindle` and `rindled`
// are matched independently by their asset name.
async function downloadBinary(manifest, repo, tag, rustTarget, bin, exe, archiveCache) {
  const artifacts = manifest?.artifacts ?? {};
  const entry = Object.entries(artifacts).find(
    ([, a]) =>
      a.kind === "executable-zip" &&
      (a.target_triples ?? []).includes(rustTarget) &&
      (a.assets ?? []).some((as) => as.kind === "executable" && (as.name === bin || as.name === exe)),
  );
  if (!entry) fail(`no executable archive carrying ${exe} for ${rustTarget} in dist-manifest.json`);
  const [filename] = entry;

  let tmp = archiveCache.get(filename);
  if (!tmp) {
    tmp = mkdtempSync(join(tmpdir(), "rindle-dl-"));
    const archive = join(tmp, filename);
    const url = `https://github.com/${repo}/releases/download/${tag}/${filename}`;
    const res = await fetch(url);
    if (!res.ok) fail(`download failed (${res.status}): ${url}`);
    writeFileSync(archive, Buffer.from(await res.arrayBuffer()));

    if (filename.endsWith(".zip")) run("unzip", ["-oq", archive, "-d", tmp]);
    else if (filename.endsWith(".tar.xz")) run("tar", ["-xJf", archive, "-C", tmp]);
    else if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) run("tar", ["-xzf", archive, "-C", tmp]);
    else fail(`don't know how to extract ${filename}`);
    archiveCache.set(filename, tmp);
  }

  // cargo-dist archives put the binary either at the root or under a <name>/ dir.
  const found = run("find", [tmp, "-name", exe, "-type", "f"]).stdout.split("\n").find(Boolean);
  if (!found) fail(`extracted ${filename} but found no ${exe} inside`);
  const stable = join(tmp, `__${exe}`);
  cpSync(found, stable);
  return stable;
}

function run(cmd, cmdArgs) {
  const r = spawnSync(cmd, cmdArgs, { encoding: "utf8" });
  if (r.status !== 0) fail(`${cmd} ${cmdArgs.join(" ")} failed: ${r.stderr || r.error?.message}`);
  return r;
}

function fail(msg) {
  console.error(`build-npm-packages: ${msg}`);
  process.exit(1);
}
