// The scaffold engine: copy a template tree into a target directory, substituting the project name
// and renaming the `_`-prefixed dotfiles npm refuses to publish (`_gitignore` → `.gitignore`). Pure
// Node built-ins, no dependencies — so `npm create rindle` runs with nothing installed.
//
// `--link` mode (internal/dev) rewrites the generated app to consume the @rindle/* packages from the
// surrounding pnpm WORKSPACE instead of npm: deps become `workspace:*`, and vite/tsconfig switch to the
// `@rindle/source` aliases the in-repo examples use. That lets us scaffold straight into `apps/` and
// typecheck the output against the real source — the "CI typechecks the generated app" discipline.

import { cp, mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { basename, join, relative } from "node:path";

/** The token the template files carry wherever the project's package name belongs. */
export const PROJECT_NAME_TOKEN = "__PROJECT_NAME__";

// Files we run name-substitution over. Everything in the template is UTF-8 text, but be explicit so a
// future binary asset (an icon, say) is copied verbatim rather than corrupted by a string replace.
const TEXT_EXT = new Set([
  ".ts", ".tsx", ".js", ".mjs", ".cjs", ".json", ".jsonc", ".css", ".html", ".sql", ".md", ".txt",
  ".ncl", ".gitignore", ".npmrc", ".env",
]);

const isTextFile = (name) => {
  const dot = name.lastIndexOf(".");
  const ext = dot === -1 ? name : name.slice(dot);
  return TEXT_EXT.has(ext) || name === "_gitignore" || name === "_npmrc";
};

/** `_gitignore` → `.gitignore`, but never touch `__root.tsx` (a TanStack route, double underscore). */
const realName = (name) => (/^_[^_]/.test(name) ? `.${name.slice(1)}` : name);

const substitute = (text, projectName) => text.split(PROJECT_NAME_TOKEN).join(projectName);

/**
 * Recursively copy `templateDir` → `targetDir`, substituting {@link PROJECT_NAME_TOKEN} in text files
 * and de-underscoring dotfiles. Returns the list of created file paths (relative to targetDir).
 */
export async function copyTemplate(templateDir, targetDir, projectName) {
  const created = [];
  async function walk(srcDir, outDir) {
    await mkdir(outDir, { recursive: true });
    for (const entry of await readdir(srcDir, { withFileTypes: true })) {
      const src = join(srcDir, entry.name);
      const out = join(outDir, realName(entry.name));
      if (entry.isDirectory()) {
        await walk(src, out);
      } else if (isTextFile(entry.name)) {
        await writeFile(out, substitute(await readFile(src, "utf8"), projectName));
        created.push(relative(targetDir, out));
      } else {
        await cp(src, out);
        created.push(relative(targetDir, out));
      }
    }
  }
  await walk(templateDir, targetDir);
  return created.sort();
}

// ---------------------------------------------------------------- workspace (--link) rewrites

// vite.config.ts for an app scaffolded INTO this repo's apps/: compile the sibling @rindle/* libraries
// straight from source (the `@rindle/source` condition) so dev never runs against a stale dist build —
// exactly what the in-repo examples do.
const WORKSPACE_VITE_CONFIG = `import { fileURLToPath } from "node:url";

import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// --link build: this app lives under apps/, so the sibling @rindle/* libraries are two levels up under
// ../../packages. Aliasing straight to each src/index.ts (the same TS the \`@rindle/source\` export
// condition points at) compiles them from source in every Vite environment, including the Start
// prerender/server pass — so dev never runs against a stale dist build.
const wasmBin = fileURLToPath(new URL("../../packages/wasm/pkg/rindle_bg.wasm", import.meta.url));
const src = (p: string) => fileURLToPath(new URL(\`../../packages/\${p}/src/index.ts\`, import.meta.url));

export default defineConfig({
  build: { target: "es2022" },
  resolve: {
    conditions: ["@rindle/source"],
    alias: [
      { find: /^rindle-wasm-bin/, replacement: wasmBin },
      { find: /^@rindle\\/client$/, replacement: src("client") },
      { find: /^@rindle\\/react$/, replacement: src("react") },
      { find: /^@rindle\\/tanstack$/, replacement: src("tanstack") },
      { find: /^@rindle\\/optimistic$/, replacement: src("optimistic") },
      { find: /^@rindle\\/normalized$/, replacement: src("normalized") },
      { find: /^@rindle\\/remote$/, replacement: src("remote") },
      { find: /^@rindle\\/wasm$/, replacement: src("wasm") },
      { find: /^@rindle\\/api-server$/, replacement: src("api-server") },
      { find: /^@rindle\\/daemon-client$/, replacement: src("daemon-client") },
      { find: /^@rindle\\/devtools$/, replacement: src("devtools") },
      { find: /^@rindle\\/react-devtools$/, replacement: src("react-devtools") },
    ],
  },
  ssr: { noExternal: [/^@rindle\\//] },
  // No proxy: /api/rindle/* is a Start server route (src/routes/api/rindle/$.ts) on this same server.
  plugins: [tanstackStart(), react()],
});
`;

// tsconfig for --link: resolve @rindle/* through the `@rindle/source` condition (their src/index.ts),
// matching the vite aliases above so tsc and Vite see the same modules.
const WORKSPACE_TSCONFIG = `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "jsx": "react-jsx",
    "verbatimModuleSyntax": true,
    "allowImportingTsExtensions": true,
    "erasableSyntaxOnly": true,
    "customConditions": ["@rindle/source"],
    "types": ["node", "vite/client"]
  },
  "include": ["src", "shared", "server", "vite-env.d.ts", "vite.config.ts"]
}
`;

/**
 * Repoint a freshly scaffolded app at the surrounding pnpm workspace: every `@rindle/*` dependency
 * becomes `workspace:*`, and vite/tsconfig switch to the source aliases. Use only when the target is
 * inside this monorepo's `apps/` (the wasm/source paths are `../../packages`).
 */
export async function applyWorkspaceLinks(targetDir) {
  const pkgPath = join(targetDir, "package.json");
  const pkg = JSON.parse(await readFile(pkgPath, "utf8"));
  for (const field of ["dependencies", "devDependencies"]) {
    const deps = pkg[field];
    if (!deps) continue;
    for (const name of Object.keys(deps)) {
      if (name.startsWith("@rindle/")) deps[name] = "workspace:*";
    }
  }
  await writeFile(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`);
  await writeFile(join(targetDir, "vite.config.ts"), WORKSPACE_VITE_CONFIG);
  await writeFile(join(targetDir, "tsconfig.json"), WORKSPACE_TSCONFIG);
}

/** Full scaffold: validate the target, copy the template, optionally apply workspace links. */
export async function scaffold({ templateDir, targetDir, projectName, link = false }) {
  if (existsSync(targetDir)) {
    const entries = await readdir(targetDir).catch(() => []);
    if (entries.length > 0) {
      throw new Error(`target directory is not empty: ${targetDir}`);
    }
  }
  const created = await copyTemplate(templateDir, targetDir, projectName);
  if (link) await applyWorkspaceLinks(targetDir);
  return created;
}

// Re-exported for the CLI's "is this a sane package name?" check.
export function isValidPackageName(name) {
  return /^(?:@[a-z0-9-*~][a-z0-9-*._~]*\/)?[a-z0-9-~][a-z0-9-._~]*$/.test(name);
}

/** Derive a default package name from a target path (its last segment, lower-kebabed). */
export function packageNameFromDir(targetDir) {
  return basename(targetDir)
    .toLowerCase()
    .replace(/[^a-z0-9-~._]+/g, "-")
    .replace(/^-+|-+$/g, "") || "rindle-app";
}
