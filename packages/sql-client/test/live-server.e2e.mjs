// Live-server conformance for @rindle/sql-client (designs 222 §3.5 / §10).
//
// Every other test in this package terminates at a stubbed `fetch`, and the Rust-side wire tests
// (`rindle-replicator/tests/sql_client_wire.rs`) hand-build HTTP bytes. So the TypeScript encoder
// and the Rust decoder are two independent implementations of Appendix A that have never met.
// 222 §3.5 says so directly: the two suites "are not generated from one shared machine-readable
// oracle", and names an SDK-to-live-server lane as a GA hardening gate. This is that lane.
//
// It is a CONFORMANCE lane, not a fuzzer: it drives the real client against a real
// `rindle-replicator` and asserts the round trip end to end. Generated statement-level
// differential coverage lives in Rust (`rindle-fuzz`'s `sql_v1_parity`), where the oracle is the
// same vendored SQLite the master links and there is no version skew.

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { createSqlClient, RindleSqlError } from "@rindle/sql-client";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

function buildReplicator() {
  console.log("building rindle-replicator (cargo)...");
  const build = spawnSync(
    "cargo",
    ["build", "-p", "rindle-replicator", "--bin", "rindle-replicator"],
    { cwd: join(repoRoot, "rust"), stdio: ["ignore", "inherit", "inherit"] },
  );
  if (build.status !== 0) {
    console.error("cargo build failed");
    process.exit(1);
  }
}

// The binary prints exactly one `{"ready":true,...}` line, which is also how it reports the port
// it actually bound. Polling the port instead would race the SQL surface coming up.
function spawnReady(bin, args, label) {
  const child = spawn(bin, args, { stdio: ["ignore", "pipe", "inherit"] });
  const ready = new Promise((resolveReady, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${label} did not become ready in 30s`)),
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
      reject(new Error(`${label} exited early with code ${code}`));
    });
  });
  return { child, ready };
}

async function startMaster() {
  const dir = mkdtempSync(join(tmpdir(), "rindle-sql-client-"));
  const configPath = join(dir, "master.json");
  // No `backup` / `localRetention`: journal GC never runs, which is fine for a short-lived test
  // and keeps the config to the SQL surface under test.
  writeFileSync(
    configPath,
    JSON.stringify({
      db: join(dir, "master.db"),
      name: "rindle-master",
      wsPort: 0,
      httpPort: 0,
      bindHost: "127.0.0.1",
      tables: [],
    }),
  );
  const bin = join(repoRoot, "rust/target/debug/rindle-replicator");
  const master = spawnReady(bin, ["--config", configPath], "rindle-replicator");
  const info = await master.ready;
  return {
    url: `http://127.0.0.1:${info.httpPort}`,
    cleanup() {
      master.child.kill("SIGKILL");
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

async function main() {
  buildReplicator();
  const master = await startMaster();
  try {
    // The master here has no `authToken`, so the bearer is inert on loopback — but the client
    // requires one, and sending it exercises the header path.
    const client = createSqlClient({ url: master.url, authToken: "sql-secret" });

    // --- schema ------------------------------------------------------------------------------
    await client.migrate({
      id: "conformance-schema",
      checksum: "fnv:live-server-1",
      statements: [
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, slug TEXT NOT NULL, body TEXT, score REAL)",
      ],
    });

    // --- execute: args encode, results decode ------------------------------------------------
    const inserted = await client.execute({
      sql: "INSERT INTO docs (slug, body, score) VALUES (?1, ?2, ?3) RETURNING id, slug, score",
      args: ["first", "hello", 1.5],
    });
    // `ExecuteResult` wraps the statement result alongside the cursor and routing metadata.
    assert.equal(inserted.result.rows.length, 1, "RETURNING must surface its row through the client");
    assert.equal(inserted.result.rows[0][1], "first");
    assert.equal(inserted.result.rows[0][2], 1.5);
    assert.equal(inserted.result.rowsAffected, 1);
    assert.notEqual(inserted.result.lastInsertRowid, null, "rowid alias must report an insert id");
    assert.equal(inserted.result.columns[0].name, "id");
    assert.equal(inserted.result.columns[0].decltype, "INTEGER");
    assert.equal(inserted.routing.servedBy, "master");

    // A read reports no affected rows and no rowid — the sticky-connection-state rule, observed
    // through the client rather than asserted in Rust.
    const read = await client.execute("SELECT id, slug FROM docs ORDER BY id");
    assert.equal(read.result.rowsAffected, 0);
    assert.equal(read.result.lastInsertRowid, null);
    assert.equal(read.result.rows.length, 1);

    // --- exact i64 round trip (222 §3.2 tagged values) ----------------------------------------
    // The default intMode is "bigint", so this is the client's own tag/untag path end to end.
    const big = 9_007_199_254_740_993n; // 2^53 + 1: not representable as an f64
    const echoed = await client.execute({ sql: "SELECT ?1", args: [big] });
    assert.equal(
      echoed.result.rows[0][0],
      big,
      "an i64 above 2^53 must survive the round trip exactly",
    );

    // --- batch --------------------------------------------------------------------------------
    const batched = await client.batch([
      { sql: "INSERT INTO docs (slug, body) VALUES (?1, ?2)", args: ["b1", "x"] },
      { sql: "INSERT INTO docs (slug, body) VALUES (?1, ?2)", args: ["b2", "y"] },
    ]);
    assert.equal(batched.results.length, 2);
    assert.equal(batched.results[0].rowsAffected, 1);

    // --- interactive transaction: commit ------------------------------------------------------
    const committed = await client.withTransaction(async (tx) => {
      await tx.execute({
        sql: "INSERT INTO docs (slug, body) VALUES (?1, ?2)",
        args: ["tx-commit", "kept"],
      });
      const inside = await tx.execute("SELECT count(*) FROM docs");
      return inside.rows[0][0];
    });
    assert.equal(committed, 4n, "a transaction must read its own writes");

    // --- interactive transaction: rollback ----------------------------------------------------
    const tx = await client.begin();
    await tx.execute({
      sql: "INSERT INTO docs (slug, body) VALUES (?1, ?2)",
      args: ["tx-rollback", "discarded"],
    });
    await tx.rollback();
    const after = await client.execute({
      sql: "SELECT count(*) FROM docs WHERE slug = ?1",
      args: ["tx-rollback"],
    });
    assert.equal(after.result.rows[0][0], 0n, "a rolled-back write must not be visible");

    // --- error contract -----------------------------------------------------------------------
    await assert.rejects(
      () => client.execute("SELECT * FROM table_that_does_not_exist"),
      (error) => {
        assert.ok(error instanceof RindleSqlError, `expected RindleSqlError, got ${error}`);
        assert.equal(typeof error.code, "string");
        assert.ok(error.code.length > 0, "a server error must carry a stable code");
        return true;
      },
    );

    // A declared envelope refusal (222 §0.1): Blob binds fail in the client, before the wire.
    await assert.rejects(
      () => client.execute({ sql: "SELECT ?1", args: [new Uint8Array([0, 1])] }),
      (error) => {
        assert.ok(error instanceof RindleSqlError, `expected RindleSqlError, got ${error}`);
        assert.equal(error.code, "VALUE_UNSUPPORTED");
        return true;
      },
    );

    // --- session cursor -----------------------------------------------------------------------
    assert.notEqual(
      client.getSessionCursor(),
      null,
      "a committed write must advance the session cursor",
    );

    await client.ping();
    await client.close();
    console.log("sql-client live-server conformance: ok");
  } finally {
    master.cleanup();
  }
}

await main();
