// Smoke tests for the scaffold engine: scaffold the template into a temp dir and assert the tree, the
// project-name substitution, the dotfile rename, and the --link workspace rewrite. These run with no
// installed dependencies (the scaffolder is dep-free), so they're a fast guard against template rot.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, readdir, rm, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { PROJECT_NAME_TOKEN, scaffold } from "../lib/scaffold.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const templateDir = join(here, "..", "templates", "minimal");

async function withScaffold(opts, fn) {
  const base = await mkdtemp(join(tmpdir(), "create-rindle-"));
  const targetDir = join(base, "app");
  try {
    const created = await scaffold({ templateDir, targetDir, projectName: opts.projectName, link: opts.link });
    await fn(targetDir, created);
  } finally {
    await rm(base, { recursive: true, force: true });
  }
}

/** Every file in a tree, relative to root. */
async function walk(root, dir = root, out = []) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) await walk(root, p, out);
    else out.push(p.slice(root.length + 1));
  }
  return out;
}

test("scaffolds the expected core files", async () => {
  await withScaffold({ projectName: "demo-app" }, async (dir) => {
    for (const f of [
      "package.json",
      "rindle.ncl",
      "tsconfig.json",
      "vite.config.ts",
      "vite-env.d.ts",
      "migrations/0001_init.sql",
      "shared/schema.gen.ts",
      "shared/app-def.ts",
      "shared/auth.ts",
      "server/app-api.ts",
      "server/rindle-http.ts",
      "src/router.tsx",
      "src/routeTree.gen.ts",
      "src/routes/api.rindle.query.tsx",
      "src/routes/api.rindle.read.tsx",
      "src/routes/api.rindle.mutate.tsx",
      "src/tanstack-start.d.ts",
      "src/ssr.ts",
      "src/rindle-tanstack.ts",
      "src/rindle-client.ts",
      "src/styles.css",
      "src/routes/__root.tsx",
      "src/routes/index.tsx",
      "src/routes/r.$id.tsx",
      "src/components/RoomCard.queries.ts",
      "src/components/RoomView.queries.ts",
      "src/components/MessageCard.tsx",
      "README.md",
      "AGENTS.md",
    ]) {
      assert.ok(existsSync(join(dir, f)), `expected ${f} to exist`);
    }
  });
});

test("substitutes the project name and leaves no token behind", async () => {
  await withScaffold({ projectName: "my-cool-app" }, async (dir) => {
    const pkg = JSON.parse(await readFile(join(dir, "package.json"), "utf8"));
    assert.equal(pkg.name, "my-cool-app");

    const readme = await readFile(join(dir, "README.md"), "utf8");
    assert.match(readme, /# my-cool-app/);

    // No file may still contain the raw token.
    for (const rel of await walk(dir)) {
      const text = await readFile(join(dir, rel), "utf8");
      assert.ok(!text.includes(PROJECT_NAME_TOKEN), `${rel} still contains ${PROJECT_NAME_TOKEN}`);
    }
  });
});

test("renames _gitignore but never the __root route", async () => {
  await withScaffold({ projectName: "demo-app" }, async (dir) => {
    assert.ok(existsSync(join(dir, ".gitignore")), ".gitignore should exist");
    assert.ok(!existsSync(join(dir, "_gitignore")), "_gitignore should have been renamed");
    // The double-underscore TanStack route must NOT be de-underscored.
    assert.ok(existsSync(join(dir, "src/routes/__root.tsx")), "__root.tsx must keep its name");
    assert.ok(!existsSync(join(dir, "src/routes/.root.tsx")), "__root.tsx must not become .root.tsx");
  });
});

test("refuses a non-empty target directory", async () => {
  await withScaffold({ projectName: "demo-app" }, async (dir) => {
    await assert.rejects(
      () => scaffold({ templateDir, targetDir: dir, projectName: "again" }),
      /not empty/,
    );
  });
});

test("--link rewrites @rindle deps to workspace and switches vite/tsconfig to source", async () => {
  await withScaffold({ projectName: "linked-app", link: true }, async (dir) => {
    const pkg = JSON.parse(await readFile(join(dir, "package.json"), "utf8"));
    const allDeps = { ...pkg.dependencies, ...pkg.devDependencies };
    for (const [name, range] of Object.entries(allDeps)) {
      if (name.startsWith("@rindle/")) assert.equal(range, "workspace:*", `${name} should be workspace:*`);
    }
    // Non-rindle deps untouched.
    assert.notEqual(allDeps["react"], "workspace:*");

    const vite = await readFile(join(dir, "vite.config.ts"), "utf8");
    assert.match(vite, /@rindle\/source/, "link-mode vite should use the @rindle/source condition");
    assert.match(vite, /src\("tanstack"\)/, "link-mode vite should resolve @rindle/tanstack from source");
    assert.match(vite, /\.\.\/\.\.\/packages\/wasm\/pkg\/rindle_bg\.wasm/, "link-mode vite resolves wasm from the workspace");

    const tsconfig = JSON.parse(await readFile(join(dir, "tsconfig.json"), "utf8"));
    assert.deepEqual(tsconfig.compilerOptions.customConditions, ["@rindle/source"]);
  });
});

test("standalone (no --link) keeps published version ranges", async () => {
  await withScaffold({ projectName: "standalone-app" }, async (dir) => {
    const pkg = JSON.parse(await readFile(join(dir, "package.json"), "utf8"));
    assert.equal(pkg.dependencies["@rindle/client"], "latest");
    assert.equal(pkg.dependencies["@rindle/tanstack"], "latest");
    assert.equal(pkg.devDependencies["@rindle/cli"], "latest");
    assert.equal(
      pkg.devDependencies["@rindle/dev-edge"],
      undefined,
      "the native edge ships with the CLI",
    );
    const vite = await readFile(join(dir, "vite.config.ts"), "utf8");
    assert.doesNotMatch(vite, /@rindle\/source/, "standalone vite should not use the source condition");
    assert.match(vite, /require\.resolve\("@rindle\/wasm"\)/, "standalone vite resolves wasm from node_modules");
  });
});

test("generated app uses the first-class TanStack integration", async () => {
  await withScaffold({ projectName: "tanstack-app" }, async (dir) => {
    const integration = await readFile(join(dir, "src/rindle-tanstack.ts"), "utf8");
    assert.match(integration, /createRindleTanStack/);
    assert.match(integration, /boot: bootClient/);
    assert.match(integration, /if \(!import\.meta\.env\.SSR\) return \{\}/);
    assert.match(integration, /import\("\.\/ssr\.ts"\)/);

    const root = await readFile(join(dir, "src/routes/__root.tsx"), "utf8");
    assert.match(root, /<rindle\.Provider>/);
    assert.doesNotMatch(root, /useMatches|Object\.assign|RindleApp/);

    for (const route of ["index.tsx", "r.$id.tsx"]) {
      const source = await readFile(join(dir, "src/routes", route), "utf8");
      assert.match(source, /loader: rindle\.loader\(/, `${route} should use rindle.loader`);
      assert.doesNotMatch(source, /preloadRindle|import\.meta\.env\.SSR/);
    }

    assert.ok(!existsSync(join(dir, "src/RindleApp.tsx")), "manual RindleSSR wrapper is obsolete");
  });
});

test("returned file list is sorted and relative", async () => {
  await withScaffold({ projectName: "demo-app" }, async (dir, created) => {
    assert.ok(Array.isArray(created) && created.length > 20);
    const sorted = [...created].sort();
    assert.deepEqual(created, sorted, "created list should be sorted");
    // entries are relative (no leading slash, resolve under the target)
    for (const rel of created) {
      assert.ok(!rel.startsWith("/"), `${rel} should be relative`);
      assert.ok((await stat(join(dir, rel))).isFile());
    }
  });
});

test("generated authority uses the unified Rindle connection", async () => {
  await withScaffold({ projectName: "unified-app" }, async (dir) => {
    const api = await readFile(join(dir, "server/app-api.ts"), "utf8");
    assert.match(api, /rindle: \{ url: opts\.url, token: opts\.token/);
    assert.match(api, /RINDLE_URL \+ RINDLE_DATABASE_TOKEN are required/);
    assert.match(api, /RINDLE_WS_URL/);
    assert.doesNotMatch(api, /SplitDaemonClient|RINDLE_REPLICATOR_URL/);
  });
});

test("generated app discovers subscriptions from leases and uses one dev lifecycle", async () => {
  await withScaffold({ projectName: "fleet-app" }, async (dir) => {
    const client = await readFile(join(dir, "src/rindle-client.ts"), "utf8");
    assert.doesNotMatch(client, /VITE_FLEET_WS|wsUrl:/);
    assert.match(client, /first query lease returns the public WebSocket endpoint/);

    const env = await readFile(join(dir, "vite-env.d.ts"), "utf8");
    assert.doesNotMatch(env, /VITE_(FLEET|DAEMON)_WS/);

    const pkg = JSON.parse(await readFile(join(dir, "package.json"), "utf8"));
    assert.match(pkg.scripts.dev, /^rindle dev --migrate --gen .* -- vite dev --port 3000$/);
    assert.equal(pkg.scripts.fleet, undefined);
    assert.equal(pkg.scripts["dev:web"], undefined);
    assert.doesNotMatch(JSON.stringify(pkg.scripts), /rindle exec/);
    assert.equal(pkg.devDependencies.concurrently, undefined);
    assert.doesNotMatch(pkg.scripts.dev, /127\.0\.0\.1:(7600|7601|7611|7650)/);
  });
});
