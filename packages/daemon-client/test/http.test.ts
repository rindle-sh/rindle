import { test } from "node:test";
import assert from "node:assert/strict";

import { DaemonHttpError, HttpRindleDaemonClient, type FetchLike } from "../src/index.ts";

function jsonResponse(
  body: unknown,
  status = 200,
  bootId?: string,
): Awaited<ReturnType<FetchLike>> {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Bad Request",
    text: async () => JSON.stringify(body),
    headers: bootId
      ? { get: (name) => (name.toLowerCase() === "rindle-boot-id" ? bootId : null) }
      : undefined,
  };
}

test("HttpRindleDaemonClient posts materialize requests to the configured daemon", async () => {
  const calls: unknown[] = [];
  const fetch: FetchLike = async (url, init) => {
    calls.push({ url, init });
    return jsonResponse({ materializationId: "mat-1", leaseToken: "lease-1" });
  };
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal/control",
    fetch,
    headers: { authorization: "Bearer token", ignored: undefined },
  });

  const out = await client.materialize({ ast: { table: "issue" }, mode: "normalized" });

  assert.deepEqual(out, { materializationId: "mat-1", leaseToken: "lease-1" });
  assert.deepEqual(calls, [
    {
      url: "https://daemon.internal/control/materialize",
      init: {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer token" },
        body: JSON.stringify({ ast: { table: "issue" }, mode: "normalized" }),
      },
    },
  ]);
});

test("materialize lifts the affinity ticket into the Rindle-Affinity header, not the body", async () => {
  const calls: Array<{ headers: Record<string, string>; body: string }> = [];
  const fetch: FetchLike = async (_url, init) => {
    calls.push({ headers: init.headers, body: init.body });
    return jsonResponse({ materializationId: "mat-1", leaseToken: "lease-1" });
  };
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://fleet.internal/control",
    fetch,
    headers: { authorization: "Bearer token" },
  });

  await client.materialize({ ast: { table: "issue" }, mode: "normalized", affinity: "aff.p.s" });

  assert.equal(calls[0].headers["Rindle-Affinity"], "aff.p.s");
  assert.equal(calls[0].headers.authorization, "Bearer token");
  // The ticket is a header, never a body field (the daemon's MaterializeInput never sees it).
  assert.deepEqual(JSON.parse(calls[0].body), { ast: { table: "issue" }, mode: "normalized" });
});

test("no affinity ticket ⇒ no Rindle-Affinity header (byte-identical to a single-daemon deploy)", async () => {
  const calls: Array<{ headers: Record<string, string> }> = [];
  const fetch: FetchLike = async (_url, init) => {
    calls.push({ headers: init.headers });
    return jsonResponse({ materializationId: "mat-1", leaseToken: "lease-1" });
  };
  const client = new HttpRindleDaemonClient({ baseUrl: "https://daemon.internal/control", fetch });

  await client.materialize({ ast: { table: "issue" } });
  await client.query({ ast: { table: "issue" } });

  assert.ok(!("Rindle-Affinity" in calls[0].headers));
  assert.ok(!("Rindle-Affinity" in calls[1].headers));
});

test("query forwards the affinity ticket as a header and strips it from the body", async () => {
  const calls: Array<{ headers: Record<string, string>; body: string }> = [];
  const fetch: FetchLike = async (_url, init) => {
    calls.push({ headers: init.headers, body: init.body });
    return jsonResponse({ rows: [] });
  };
  const client = new HttpRindleDaemonClient({ baseUrl: "https://fleet.internal/control", fetch });

  await client.query({ ast: { table: "issue" }, visibilityKey: "v1", affinity: "aff.q.z" });

  assert.equal(calls[0].headers["Rindle-Affinity"], "aff.q.z");
  assert.deepEqual(JSON.parse(calls[0].body), { ast: { table: "issue" }, visibilityKey: "v1" });
});

test("HttpRindleDaemonClient posts SSR one-shot /query reads", async () => {
  const calls: Array<{ url: string; body: string }> = [];
  const fetch: FetchLike = async (url, init) => {
    calls.push({ url, body: init.body });
    return jsonResponse({
      queryKey: "abc",
      cvMin: 7,
      rows: [{ cols: { id: 1, title: "first" } }],
    });
  };
  const client = new HttpRindleDaemonClient({ baseUrl: "https://daemon.internal/control", fetch });

  const out = await client.query({ ast: { table: "issue" }, visibilityKey: "v1", ttlMs: 9000 });

  assert.equal(out.cvMin, 7);
  assert.deepEqual(out.rows, [{ cols: { id: 1, title: "first" } }]);
  assert.deepEqual(calls, [
    {
      url: "https://daemon.internal/control/query",
      body: JSON.stringify({ ast: { table: "issue" }, visibilityKey: "v1", ttlMs: 9000 }),
    },
  ]);
});

test("HttpRindleDaemonClient posts schema migrations to /migrate", async () => {
  const calls: Array<{ url: string; body: string }> = [];
  const overrideHash = "a".repeat(64);
  const fetch: FetchLike = async (url, init) => {
    calls.push({ url, body: init.body });
    return jsonResponse({
      applied: false,
      hashOverridden: true,
      schemaVersion: "0001_init",
    });
  };
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal/control",
    fetch,
    headers: { authorization: "Bearer token" },
  });

  const out = await client.migrate({
    id: "0001_init",
    checksum: "b".repeat(64),
    overrideHash,
    statements: ["CREATE TABLE issue (id TEXT, PRIMARY KEY (id))"],
  });

  assert.deepEqual(out, {
    applied: false,
    hashOverridden: true,
    schemaVersion: "0001_init",
  });
  assert.equal(out.hashOverridden, true);
  assert.deepEqual(calls, [
    {
      url: "https://daemon.internal/control/migrate",
      body: JSON.stringify({
        id: "0001_init",
        checksum: "b".repeat(64),
        overrideHash,
        statements: ["CREATE TABLE issue (id TEXT, PRIMARY KEY (id))"],
      }),
    },
  ]);
});

test("HttpRindleDaemonClient posts a migration batch (array) to /migrate", async () => {
  const calls: Array<{ url: string; body: string }> = [];
  const fetch: FetchLike = async (url, init) => {
    calls.push({ url, body: init.body });
    return jsonResponse({
      results: [
        { id: "0001_init", applied: true },
        {
          id: "0002_notes",
          applied: false,
          hashOverridden: true,
          schemaVersion: "0002_notes",
        },
      ],
      applied: 1,
      schemaVersion: "0002_notes",
      restarting: false,
    });
  };
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal/control",
    fetch,
    headers: { authorization: "Bearer token" },
  });

  const inputs = [
    { id: "0001_init", statements: ["CREATE TABLE issue (id TEXT, PRIMARY KEY (id))"] },
    {
      id: "0002_notes",
      checksum: "c".repeat(64),
      overrideHash: "d".repeat(64),
      statements: ["CREATE TABLE note (id TEXT, PRIMARY KEY (id))"],
    },
  ];
  const out = await client.migrateBatch(inputs);

  assert.equal(out.applied, 1);
  assert.equal(out.schemaVersion, "0002_notes");
  assert.equal(out.results.length, 2);
  assert.equal(out.results[1]?.hashOverridden, true);
  assert.deepEqual(calls, [
    {
      url: "https://daemon.internal/control/migrate",
      body: JSON.stringify(inputs),
    },
  ]);
});

test("HttpRindleDaemonClient GETs /schema with the bearer token and no body", async () => {
  const calls: Array<{ url: string; method: string; auth?: string; body?: string }> = [];
  const doc = {
    tables: [{ name: "page", columns: [{ name: "id", type: "string" }], primaryKey: ["id"] }],
  };
  const fetch: FetchLike = async (url, init) => {
    calls.push({ url, method: init.method, auth: init.headers.authorization, body: init.body });
    return jsonResponse(doc);
  };
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal/control",
    fetch,
    headers: { authorization: "Bearer token" },
  });

  const out = await client.schema();

  assert.deepEqual(out, doc);
  assert.deepEqual(calls, [
    {
      url: "https://daemon.internal/control/schema",
      method: "GET",
      auth: "Bearer token",
      body: "", // the client sends "" for a GET; defaultFetch drops it before the real fetch
    },
  ]);
});

test("onBootId fires on the first response and again only when the boot id CHANGES", async () => {
  let boot = "boot-a";
  const fetch: FetchLike = async () => jsonResponse({ applied: true }, 200, boot);
  const observed: string[] = [];
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal",
    fetch,
    onBootId: (id) => observed.push(id),
  });

  await client.executeSqlTxn({ statements: [] });
  await client.executeSqlTxn({ statements: [] }); // same boot id → no re-fire
  assert.deepEqual(observed, ["boot-a"], "fires once for a stable daemon");

  boot = "boot-b"; // daemon restarted
  await client.executeSqlTxn({ statements: [] });
  assert.deepEqual(observed, ["boot-a", "boot-b"], "fires again on a restart");
});

test("onBootId stays silent when the daemon sends no boot-id header", async () => {
  const fetch: FetchLike = async () => jsonResponse({ applied: true }); // no header
  const observed: string[] = [];
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal",
    fetch,
    onBootId: (id) => observed.push(id),
  });
  await client.executeSqlTxn({ statements: [] });
  assert.deepEqual(observed, []);
});

test("HttpRindleDaemonClient uses custom paths and surfaces daemon errors", async () => {
  const fetch: FetchLike = async () => ({
    ok: false,
    status: 400,
    statusText: "Bad Request",
    text: async () => "bad query",
  });
  const client = new HttpRindleDaemonClient({
    baseUrl: "https://daemon.internal",
    fetch,
    paths: { materialize: "/private/materialize" },
  });

  await assert.rejects(
    () => client.materialize({ ast: { table: "issue" } }),
    (err: unknown) =>
      err instanceof DaemonHttpError &&
      err.status === 400 &&
      err.body === "bad query",
  );
});
