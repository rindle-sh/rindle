import assert from "node:assert/strict";
import {
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { resetWikiDataForImage, startPair } from "../src/daemon.ts";

test("deployment reset removes only wiki-owned data once per image", () => {
  const dataDir = mkdtempSync(join(tmpdir(), "wiki-reset-test-"));
  try {
    const owned = [
      "master.db",
      "master.db-pagemap",
      "master.db-log-1",
      "follower.db",
      "follower.db-wal",
      "follower.db-shm",
      "wiki.db",
      "wiki.db-wal",
      ".offset",
      "master.json",
      "follower.json",
      "daemon.json",
    ];
    for (const name of owned) writeFileSync(join(dataDir, name), name);
    writeFileSync(join(dataDir, "operator-notes.txt"), "preserve me");
    mkdirSync(join(dataDir, "unrelated"));
    writeFileSync(join(dataDir, "unrelated", "state"), "preserve me too");

    const first = resetWikiDataForImage(dataDir, "registry/app:deployment-001");
    assert.equal(first.reset, true);
    assert.deepEqual(first.removed.sort(), owned.sort());
    for (const name of owned)
      assert.equal(existsSync(join(dataDir, name)), false, name);
    assert.equal(
      readFileSync(join(dataDir, "operator-notes.txt"), "utf8"),
      "preserve me",
    );
    assert.equal(
      readFileSync(join(dataDir, "unrelated", "state"), "utf8"),
      "preserve me too",
    );

    // An ordinary restart of the same image retains its database and source cursor.
    writeFileSync(join(dataDir, "follower.db"), "new state");
    writeFileSync(join(dataDir, ".offset"), "event-123");
    assert.deepEqual(
      resetWikiDataForImage(dataDir, "registry/app:deployment-001"),
      {
        reset: false,
        removed: [],
      },
    );
    assert.equal(
      readFileSync(join(dataDir, "follower.db"), "utf8"),
      "new state",
    );
    assert.equal(readFileSync(join(dataDir, ".offset"), "utf8"), "event-123");

    // A different deployment image starts clean again but still preserves unrelated volume data.
    const next = resetWikiDataForImage(dataDir, "registry/app:deployment-002");
    assert.equal(next.reset, true);
    assert.deepEqual(next.removed.sort(), [".offset", "follower.db"].sort());
    assert.equal(
      readFileSync(join(dataDir, "operator-notes.txt"), "utf8"),
      "preserve me",
    );
  } finally {
    rmSync(dataDir, { recursive: true, force: true });
  }
});

test("local runs without an image ref never reset data", () => {
  const dataDir = mkdtempSync(join(tmpdir(), "wiki-local-reset-test-"));
  try {
    writeFileSync(join(dataDir, "wiki.db"), "local state");
    assert.deepEqual(resetWikiDataForImage(dataDir, undefined), {
      reset: false,
      removed: [],
    });
    assert.equal(readFileSync(join(dataDir, "wiki.db"), "utf8"), "local state");
  } finally {
    rmSync(dataDir, { recursive: true, force: true });
  }
});

test("colocated pair bounds retention and isolates child tuning/metrics", async () => {
  const dataDir = mkdtempSync(join(tmpdir(), "wiki-pair-config-test-"));
  const fakeBin = join(dataDir, "fake-daemon.mjs");
  const capturePath = join(dataDir, "captures.ndjson");
  writeFileSync(
    fakeBin,
    `#!/usr/bin/env node
import { appendFileSync, readFileSync } from "node:fs";
const config = JSON.parse(readFileSync(process.argv[3], "utf8"));
appendFileSync(process.env.WIKI_TEST_CAPTURE, JSON.stringify({ config, metrics: process.env.RINDLE_METRICS_ADDR ?? null, writeConnections: process.env.RINDLE_WRITE_CONNECTIONS ?? null }) + "\\n");
console.log(JSON.stringify({ ready: true, httpPort: config.httpPort, wsPort: config.wsPort, bootId: "test-boot" }));
process.on("SIGTERM", () => process.exit(0));
setInterval(() => {}, 1000);
`,
    { mode: 0o755 },
  );

  const envNames = [
    "RINDLE_REPLICATOR_BIN",
    "RINDLED_BIN",
    "RINDLE_METRICS_ADDR",
    "RINDLE_MASTER_METRICS_ADDR",
    "RINDLE_FOLLOWER_METRICS_ADDR",
    "WIKI_TEST_CAPTURE",
  ] as const;
  const previous = new Map(envNames.map((name) => [name, process.env[name]]));
  let pair;
  try {
    process.env.RINDLE_REPLICATOR_BIN = fakeBin;
    process.env.RINDLED_BIN = fakeBin;
    process.env.RINDLE_METRICS_ADDR = "127.0.0.1:9999"; // must never leak to the master
    process.env.RINDLE_MASTER_METRICS_ADDR = "127.0.0.1:9092";
    process.env.RINDLE_FOLLOWER_METRICS_ADDR = "127.0.0.1:9091";
    process.env.WIKI_TEST_CAPTURE = capturePath;

    pair = await startPair({ dataDir });
    const captures = readFileSync(capturePath, "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const master = captures.find(
      (capture) => capture.config.name === "rindle-master",
    );
    const follower = captures.find((capture) => capture.config.sources);

    assert.equal(
      master.config.localRetention.followerDb,
      join(dataDir, "follower.db"),
    );
    assert.equal(master.config.localRetention.source, "rindle-master");
    assert.equal(master.config.localRetention.intervalSecs, 60);
    assert.equal(master.config.localRetention.graceSecs, 0);
    assert.equal(master.writeConnections, "1");
    assert.equal(master.metrics, "127.0.0.1:9092");
    assert.equal(follower.metrics, "127.0.0.1:9091");
    assert.equal(follower.config.nWorkers, 2, "one follower worker per pinned board");
    assert.ok(
      pair.masterPid && pair.followerPid && pair.masterPid !== pair.followerPid,
    );
  } finally {
    pair?.close();
    for (const name of envNames) {
      const value = previous.get(name);
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    rmSync(dataDir, { recursive: true, force: true });
  }
});
