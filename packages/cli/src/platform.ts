// Single source of truth for mapping the *running* Node platform to the per-platform package that
// carries the prebuilt Rindle binaries (`rindle`, `rindled`, `rindle-replicator`, and the local
// `rindle-dev-edge`) for it. Those
// packages (`@rindle/cli-<key>`) are generated at release time by `scripts/build-npm-packages.mjs`
// from dist's (cargo-dist) release archives, and declared as optionalDependencies of this umbrella
// package — so npm/pnpm install only the one whose `os`/`cpu` match the host. All binaries ship
// **co-located** in that one package's `bin/`, which is required: the `rindle` CLI's `rindle up`
// finds every component as a sibling of its own executable (rindle-cli §7.1) so it can supervise
// the local topology. This module and the generator MUST agree on the `<key>`
// scheme; it is defined here and mirrored in the generator.

export type Libc = "gnu" | "musl";

/** A native binary shipped by the Rindle development toolchain. */
export type Binary = "rindle" | "rindled" | "rindle-replicator" | "rindle-dev-edge";

/**
 * The package-name suffix identifying a build target for the current (or given) host:
 * `darwin-{arm64,x64}`, `linux-{x64,arm64}-{gnu,musl}`, `win32-x64-msvc`. Throws
 * `UnsupportedPlatformError` for a host we don't publish binaries for.
 */
export function platformKey(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
  libc: Libc = detectLibc(platform),
): string {
  switch (platform) {
    case "darwin":
      return `darwin-${arch}`;
    case "linux":
      return `linux-${arch}-${libc}`;
    // `win32` is deliberately absent, and must stay in step with `"rindle".targets` in
    // package.json: we publish no Windows binaries, so returning a key here would only trade this
    // actionable error for a MODULE_NOT_FOUND on a package that was never published. Restoring
    // Windows means re-adding the target there AND a `case "win32"` here — see
    // follow-ups/windows-hctree.md.
    default:
      throw new UnsupportedPlatformError(platform, arch);
  }
}

/** The npm package name carrying the binaries for `key` (default: the current host's key). */
export function platformPackageName(key: string = platformKey()): string {
  return `@rindle/cli-${key}`;
}

/** The on-disk filename of `bin` inside a platform package (`rindle` / `rindled.exe`). */
export function binaryName(bin: Binary, platform: NodeJS.Platform = process.platform): string {
  return platform === "win32" ? `${bin}.exe` : bin;
}

export class UnsupportedPlatformError extends Error {
  constructor(platform: string, arch: string) {
    super(
      `@rindle/cli ships no prebuilt Rindle binaries for ${platform}-${arch}` +
        (platform === "win32"
          ? ". Rindle's storage engine is Linux-only, so there is no native Windows build. Run the " +
            "toolchain under WSL2 and keep the database on the WSL filesystem (~/), NOT under " +
            "/mnt/c — the Windows drive mount cannot back a memory-mapped database. Your app and " +
            "browser can stay on Windows: WSL2 forwards localhost."
          : ""),
    );
    this.name = "UnsupportedPlatformError";
  }
}

// glibc vs musl: Node reports the runtime glibc version in its process report on glibc systems;
// its absence on linux implies musl. We publish both `-gnu` and `-musl` Linux packages, so this
// picks the right one (and avoids a `detect-libc` dependency — the launcher stays dep-free).
function detectLibc(platform: NodeJS.Platform): Libc {
  if (platform !== "linux") return "gnu";
  try {
    const report = process.report?.getReport() as
      | { header?: { glibcVersionRuntime?: string } }
      | undefined;
    return report?.header?.glibcVersionRuntime ? "gnu" : "musl";
  } catch {
    return "gnu";
  }
}
