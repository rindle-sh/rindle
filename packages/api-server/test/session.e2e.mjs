// api-server ↔ LIVE rindle-replicator: interactive mutation sessions end to end
// (REPLICATOR-INTERACTIVE-TXN-DESIGN.md; the daemon-hosted twin retired with design 214 —
// the replicator is the ONE write master). Proves against the real master what the unit
// fakes assert in shape: the lazy upgrade at first read, read-your-writes inside one
// transaction (point `row` AND full-shape `tx.query` with a nested relationship), begin-time
// dedup on a redelivered envelope (the body must NOT re-run), the §2.4 rejection path
// (rollback → lmid advances alone → the client's next mid applies), and the write-path LIKE
// parity pragma.
//
// Needs cargo + the C toolchain (builds rindle-replicator) — run via `pnpm test:rust-daemon`.

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import { createSchema, number, string, table } from "@rindle/client";
import { createRindleApiServer, daemonBackend, defineApiMutators } from "@rindle/api-server";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../../..");

console.log("building rindle-replicator (cargo)...");
const build = spawnSync("cargo", ["build", "-p", "rindle-replicator", "--bin", "rindle-replicator"], {
  cwd: join(repoRoot, "rust"),
  stdio: ["ignore", "inherit", "inherit"],
});
if (build.status !== 0) {
  console.error("cargo build failed");
  process.exit(1);
}

const dir = mkdtempSync(join(tmpdir(), "session-e2e-"));
writeFileSync(
  join(dir, "master.json"),
  JSON.stringify({
    db: join(dir, "master.db"),
    name: "rindle-master",
    httpPort: 0,
    wsPort: 0,
    bindHost: "127.0.0.1",
    tables: [
      { name: "issue", columns: ["id", "title", "votes"], pk: [0], types: ["string", "string", "number"] },
      { name: "comment", columns: ["id", "issueId", "body"], pk: [0], types: ["string", "string", "string"] },
    ],
  }),
);

const child = spawn(
  join(repoRoot, "rust/target/debug/rindle-replicator"),
  ["--config", join(dir, "master.json")],
  { stdio: ["ignore", "pipe", "inherit"] },
);

const ready = await new Promise((resolveReady, reject) => {
  const timer = setTimeout(
    () => reject(new Error("rindle-replicator did not become ready in 30s")),
    30_000,
  );
  let buffer = "";
  child.stdout.on("data", (chunk) => {
    buffer += String(chunk);
    const line = buffer.split("\n").find((l) => l.includes('"ready"'));
    if (line) {
      clearTimeout(timer);
      resolveReady(JSON.parse(line));
    }
  });
  child.on("exit", (code) => {
    clearTimeout(timer);
    reject(new Error(`rindle-replicator exited early with code ${code}`));
  });
});

try {
  const daemon = new HttpRindleDaemonClient({
    baseUrl: `http://127.0.0.1:${ready.httpPort}`,
    headers: { authorization: "Bearer secret" },
  });

  const schema = createSchema({
    tables: [
      table("issue").columns({ id: string(), title: string(), votes: number() }).primaryKey("id"),
      table("comment").columns({ id: string(), issueId: string(), body: string() }).primaryKey("id"),
    ],
  });

  /** What each mutator observed mid-transaction — the read-your-writes evidence. */
  const observed = {};
  let guardedRuns = 0;

  const api = createRindleApiServer({
    daemon,
    backend: daemonBackend(daemon),
    schema,
    mutators: defineApiMutators({
      // The classic read-dependent shape (and the pre-session TOCTOU poster child §3.3).
      voteOnce: async (tx, args) => {
        const existing = await tx.row("issue", { id: args.id });
        if (!existing) {
          await tx.insert("issue", { id: args.id, title: args.title, votes: 1 });
          return;
        }
        await tx.update("issue", { id: args.id, votes: Number(existing.votes) + 1 });
      },
      // Full-shape tx.query with a nested relationship, reading its OWN uncommitted insert.
      addCommentAndReport: async (tx, args) => {
        await tx.insert("comment", { id: args.commentId, issueId: args.issueId, body: args.body });
        observed.report = await tx.query({
          table: "issue",
          where: {
            type: "simple",
            op: "=",
            left: { type: "column", name: "id" },
            right: { type: "literal", value: args.issueId },
          },
          related: [
            {
              correlation: { parentField: ["id"], childField: ["issueId"] },
              subquery: { table: "comment", alias: "comments" },
            },
          ],
        });
      },
      guarded: async (tx) => {
        guardedRuns += 1;
        await tx.row("issue", { id: "absent" }); // upgrade to a session first
        throw new Error("not yours");
      },
    }),
  });

  const push = (mid, name, args) =>
    api.pushMutation({ user: "u1", envelope: { clientID: "e2e-client", mid, name, args } });

  const count = async (sql, params = []) => {
    const out = await daemon.executeSqlRead({ sql, params });
    return Number(out.rows[0][0]);
  };

  // 1. Insert path: row() finds nothing inside the session, insert commits with lmid 1.
  let out = await push(1, "voteOnce", { id: "i1", title: "boat" });
  assert.equal(out.accepted, true);
  assert.equal(await count(`SELECT votes FROM issue WHERE id = 'i1'`), 1);

  // 2. Update path: the next session reads the COMMITTED row and increments it.
  out = await push(2, "voteOnce", { id: "i1", title: "boat" });
  assert.equal(out.accepted, true);
  assert.equal(await count(`SELECT votes FROM issue WHERE id = 'i1'`), 2);

  // 3. Redelivery of mid 2 (a lost commit response): absorbed AT BEGIN — applied:false and,
  //    critically, the body did not re-run (votes unchanged).
  out = await push(2, "voteOnce", { id: "i1", title: "boat" });
  assert.equal(out.accepted, true);
  assert.equal(out.output.applied, false);
  assert.equal(await count(`SELECT votes FROM issue WHERE id = 'i1'`), 2);

  // 4. tx.query reads its own uncommitted write: the nested report carries the comment the
  //    SAME transaction just inserted, before anything committed.
  out = await push(3, "addCommentAndReport", { issueId: "i1", commentId: "c1", body: "ship it" });
  assert.equal(out.accepted, true);
  assert.equal(observed.report.length, 1);
  assert.equal(observed.report[0].id, "i1");
  assert.deepEqual(
    observed.report[0].comments.map((c) => c.id),
    ["c1"],
  );
  assert.equal(await count(`SELECT COUNT(*) FROM comment`), 1);

  // 5. Business rejection: data rolls back, lmid still advances (§2.4) — the NEXT mid applies.
  out = await push(4, "guarded", {});
  assert.equal(out.accepted, false);
  assert.match(out.reason, /not yours/);
  assert.equal(guardedRuns, 1);
  out = await push(5, "voteOnce", { id: "i2", title: "note" });
  assert.equal(out.accepted, true);
  assert.equal(await count(`SELECT COUNT(*) FROM issue`), 2);

  // 6. LIKE parity over the wire: every cluster connection runs case_sensitive_like=ON
  //    (open_wal2), matching the engine matcher and rindle-d2s.
  assert.equal(await count(`SELECT COUNT(*) FROM issue WHERE title LIKE 'b%'`), 1);
  assert.equal(await count(`SELECT COUNT(*) FROM issue WHERE title LIKE 'B%'`), 0);

  console.log("session e2e: all assertions passed");
} finally {
  child.kill("SIGKILL");
}
