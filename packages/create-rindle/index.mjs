#!/usr/bin/env node
// `npm create rindle@latest [dir]` / `npx create-rindle [dir]` — scaffold a SQL-first Rindle app on
// TanStack Start. Zero runtime dependencies: argument parsing, a minimal interactive prompt, and the
// template copy all run on Node built-ins, so this boots with nothing installed.
//
//   create-rindle my-app                 # scaffold ./my-app (the only template today: "minimal")
//   create-rindle my-app --no-install    # skip the package-manager install
//   create-rindle my-app --pm pnpm       # force the package manager (else auto-detected)
//   create-rindle apps/foo --link        # internal: consume @rindle/* from this repo's workspace

import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline/promises";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";

import { isValidPackageName, packageNameFromDir, scaffold } from "./lib/scaffold.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const TEMPLATE = "minimal";

function parseArgs(argv) {
  const opts = { dir: undefined, install: true, pm: undefined, link: false, help: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--help" || a === "-h") opts.help = true;
    else if (a === "--no-install") opts.install = false;
    else if (a === "--link") opts.link = true;
    else if (a === "--pm") opts.pm = argv[++i];
    else if (a.startsWith("--pm=")) opts.pm = a.slice("--pm=".length);
    else if (a.startsWith("-")) fail(`unknown flag: ${a}`);
    else if (opts.dir === undefined) opts.dir = a;
    else fail(`unexpected argument: ${a}`);
  }
  return opts;
}

function fail(message) {
  process.stderr.write(`\ncreate-rindle: ${message}\n`);
  process.exit(1);
}

const HELP = `create-rindle — scaffold a SQL-first Rindle app on TanStack Start

Usage:
  npm create rindle@latest [directory] [options]
  npx create-rindle [directory] [options]

Options:
  --no-install      Don't run the package-manager install after scaffolding
  --pm <name>       Package manager to use (npm | pnpm | yarn | bun); default: auto-detect
  --link            Internal: wire @rindle/* to this monorepo's workspace (for apps/* in the repo)
  -h, --help        Show this help
`;

/** Which package manager invoked us (npm/pnpm/yarn/bun set npm_config_user_agent), else npm. */
function detectPackageManager() {
  const ua = process.env.npm_config_user_agent ?? "";
  if (ua.startsWith("pnpm")) return "pnpm";
  if (ua.startsWith("yarn")) return "yarn";
  if (ua.startsWith("bun")) return "bun";
  return "npm";
}

async function prompt(question, fallback) {
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    const answer = (await rl.question(`${question} (${fallback}) `)).trim();
    return answer || fallback;
  } finally {
    rl.close();
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    process.stdout.write(HELP);
    return;
  }

  // Resolve the target directory — prompt for one if not given and we have a TTY.
  let dir = opts.dir;
  if (!dir) {
    dir = process.stdin.isTTY ? await prompt("Where should we create your app?", "./rindle-app") : "./rindle-app";
  }
  const targetDir = resolve(process.cwd(), dir);
  const projectName = packageNameFromDir(targetDir);
  if (!isValidPackageName(projectName)) {
    fail(`"${projectName}" is not a valid npm package name — rename the target directory`);
  }

  const templateDir = join(here, "templates", TEMPLATE);

  process.stdout.write(`\n  Scaffolding a Rindle app in ${targetDir}\n`);
  let created;
  try {
    created = await scaffold({ templateDir, targetDir, projectName, link: opts.link });
  } catch (err) {
    fail(err instanceof Error ? err.message : String(err));
  }
  process.stdout.write(`  Created ${created.length} files (template: ${TEMPLATE}${opts.link ? ", workspace-linked" : ""})\n`);

  const pm = opts.pm ?? detectPackageManager();
  if (opts.install) {
    process.stdout.write(`\n  Installing dependencies with ${pm}…\n\n`);
    const install = spawnSync(pm, ["install"], { cwd: targetDir, stdio: "inherit", shell: process.platform === "win32" });
    if (install.status !== 0) {
      process.stdout.write(`\n  Install failed or was skipped — run \`${pm} install\` yourself.\n`);
    }
  }

  const run = pm === "npm" ? "npm run" : pm;
  process.stdout.write(
    `\n  Done. Next steps:\n\n` +
      `    cd ${dir}\n` +
      (opts.install ? "" : `    ${pm} install\n`) +
      `    ${run} dev\n\n` +
      `  The migrations in migrations/*.sql are the source of truth — \`${run} dev\` applies them,\n` +
      `  generates shared/schema.gen.ts, and boots the topology pair + Vite (app + /api/rindle routes).\n` +
      `  Open two browser windows, create a room, and watch writes sync live.\n\n`,
  );
}

main().catch((err) => fail(err instanceof Error ? err.stack ?? err.message : String(err)));
