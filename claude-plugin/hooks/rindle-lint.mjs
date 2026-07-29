#!/usr/bin/env node
// PreToolUse guardrail: reject Rindle anti-patterns before they reach disk.
//
// Scope guard first: rules only run when the file being written is Rindle app
// code (it, or its on-disk version, references `@rindle/`). Everything else —
// including non-Rindle projects with this plugin enabled — passes untouched.
//
// Exit 0 = allow. Exit 2 = block; stderr goes back to Claude as the reason.
// Rules are (pattern, message) entries — extend RULES as the API surface grows.

import { readFileSync } from "node:fs";

const RULES = [
  {
    pattern: /\bfromShared\s*\(/,
    message:
      "`fromShared(...)` was removed from @rindle/api-server. Pair the generator " +
      "with its arg schema via `shared(args, gen)` in the shared contract and " +
      "drive all of them on the server with `sharedApiMutators(...)`. " +
      "See the rindle plugin skill: references/mutators.md.",
  },
  {
    // Nondeterminism inside an isomorphic mutator body: mutators re-run on
    // every rebase, so wall clocks and RNGs diverge between prediction and
    // authority. Only checked when the content also yields logical ops.
    pattern: /\b(?:Date\.now|Math\.random|crypto\.randomUUID)\s*\(/,
    onlyIf: /\byield\s+tx\./,
    message:
      "Non-deterministic call in a file with isomorphic mutator bodies " +
      "(`yield tx....`). Mutators re-run on every rebase — generate ids and " +
      "timestamps at the CALLSITE and pass them in as args instead. " +
      "See the rindle plugin skill: 'The rules that keep it correct', rule 3.",
  },
];

function readStdin() {
  try {
    return readFileSync(0, "utf8");
  } catch {
    return "";
  }
}

let input;
try {
  input = JSON.parse(readStdin());
} catch {
  process.exit(0); // malformed hook input — never block on our own failure
}

const ti = input.tool_input ?? {};
const filePath = ti.file_path ?? "";
if (!/\.(ts|tsx|js|jsx|mjs|mts)$/.test(filePath)) process.exit(0);

// The text this tool call is about to introduce.
const newText = [
  ti.content, // Write
  ti.new_string, // Edit
  ...(Array.isArray(ti.edits) ? ti.edits.map((e) => e?.new_string) : []), // MultiEdit
]
  .filter((s) => typeof s === "string")
  .join("\n");
if (newText === "") process.exit(0);

// Scope guard: is this Rindle app code at all?
let onDisk = "";
try {
  onDisk = readFileSync(filePath, "utf8");
} catch {
  /* new file — judge by the written content alone */
}
if (!/@rindle\//.test(newText) && !/@rindle\//.test(onDisk)) process.exit(0);

const haystackForGate = newText + "\n" + onDisk;
for (const rule of RULES) {
  if (rule.onlyIf && !rule.onlyIf.test(haystackForGate)) continue;
  if (rule.pattern.test(newText)) {
    process.stderr.write(`rindle-lint: ${rule.message}\n`);
    process.exit(2);
  }
}
process.exit(0);
