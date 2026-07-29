// The unified `rindle: { url, token }` connection: one origin + one key derive BOTH layers — the
// SQL database below and the sync/IVM control plane above, both under the same server-side bearer.
// These tests run a real loopback HTTP
// server as the fake single-ingress edge because the DERIVED clients expose no fetch seam — that
// is the point of the derivation.

import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import type { MaterializeInput, MaterializeOutput, RindleDaemonClient } from "@rindle/daemon-client";
import type { Fetch } from "@rindle/sql-client";

import { createRindleApiServer, defineApiMutators, defineApiQueries } from "../src/index.ts";

interface Seen {
  method: string;
  path: string;
  auth: string | undefined;
}

/** A loopback stand-in for the single-ingress fleet edge: records every request's path and
 *  bearer, and answers the two endpoints these tests drive. */
async function fakeEdge(): Promise<{ url: string; seen: Seen[]; close(): Promise<void> }> {
  const seen: Seen[] = [];
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk: Buffer) => (body += chunk));
    req.on("end", () => {
      seen.push({ method: req.method ?? "", path: req.url ?? "", auth: req.headers.authorization });
      res.setHeader("content-type", "application/json");
      if (req.url === "/v1/sql/mutations/execute") {
        const mid = (JSON.parse(body) as { mid?: number }).mid ?? 0;
        res.end(JSON.stringify({ applied: true, cursor: `w:000000000000000${mid}`, lmid: mid }));
      } else if (req.url === "/materialize") {
        res.end(
          JSON.stringify({
            materializationId: "mat-1",
            leaseToken: "lease-1",
            affinity: "aff.follower-1.ticket",
          }),
        );
      } else {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: `unexpected ${req.method} ${req.url}` }));
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    seen,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}

const mutators = () =>
  defineApiMutators({
    touch: (tx, args: { id: string }) => tx.exec("UPDATE issue SET touched = 1 WHERE id = ?", [args.id]),
  });

const queries = () => defineApiQueries({ allIssues: () => ({ table: "issue" }) });

test("rindle: { url, token } derives both layers against the one origin", async () => {
  const edge = await fakeEdge();
  try {
    const api = createRindleApiServer({
      rindle: { url: edge.url, token: "sekret" },
      queries: queries(),
      mutators: mutators(),
    });

    const receipt = await api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "touch", args: { id: "i1" } },
    });
    assert.equal(receipt.accepted, true);
    const lease = await api.createQueryLease({ user: "u1", name: "allIssues", args: null });

    const sql = edge.seen.find((s) => s.path.startsWith("/v1/sql/"));
    assert.ok(sql, "the derived SQL layer reaches the single ingress");
    assert.equal(sql.auth, "Bearer sekret", "the token authenticates the SQL layer");
    const materialize = edge.seen.find((s) => s.path === "/materialize");
    assert.ok(materialize, "the derived control plane reaches the SAME ingress");
    assert.equal(materialize.auth, "Bearer sekret", "the same token authenticates the control plane");
    assert.equal(lease.wsEndpoint, edge.url.replace(/^http:/, "ws:"));
    assert.equal(lease.affinity, "aff.follower-1.ticket", "the follower's placement ticket reaches the browser");
    api.close();
  } finally {
    await edge.close();
  }
});

test("rindle: {} resolves the connection from the rindle dev environment", async () => {
  const edge = await fakeEdge();
  const saved = { url: process.env.RINDLE_URL, token: process.env.RINDLE_DATABASE_TOKEN };
  process.env.RINDLE_URL = edge.url;
  process.env.RINDLE_DATABASE_TOKEN = "env-sekret";
  try {
    const api = createRindleApiServer({ rindle: {}, mutators: mutators() });
    await api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "touch", args: { id: "i1" } },
    });
    const sql = edge.seen.find((s) => s.path.startsWith("/v1/sql/"));
    assert.ok(sql, "the env-resolved URL is the ingress");
    assert.equal(sql.auth, "Bearer env-sekret", "the env-resolved token authenticates SQL");
    api.close();
  } finally {
    process.env.RINDLE_URL = saved.url ?? "";
    if (saved.url === undefined) delete process.env.RINDLE_URL;
    process.env.RINDLE_DATABASE_TOKEN = saved.token ?? "";
    if (saved.token === undefined) delete process.env.RINDLE_DATABASE_TOKEN;
    await edge.close();
  }
});

test("explicitly configured fields win over the derivation", async () => {
  const edge = await fakeEdge();
  try {
    // Explicit database: an injected-fetch SQL client that never touches the network.
    const sqlPaths: string[] = [];
    const fetch: Fetch = async (input, init = {}) => {
      sqlPaths.push(new URL(String(input)).pathname);
      const mid = (JSON.parse(String(init.body ?? "{}")) as { mid?: number }).mid ?? 0;
      return new Response(
        JSON.stringify({ applied: true, cursor: `w:000000000000000${mid}`, lmid: mid }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    };
    // Explicit daemon: an in-memory recorder that must receive the control-plane calls.
    const materialized: MaterializeInput[] = [];
    const daemon = {
      async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
        materialized.push(input);
        return { materializationId: "mat-mem", leaseToken: "lease-mem" };
      },
    } as unknown as RindleDaemonClient;

    const api = createRindleApiServer({
      rindle: { url: edge.url, token: "ignored" },
      database: { url: "https://db.example", authToken: "explicit", fetch },
      daemon,
      queries: queries(),
      mutators: mutators(),
    });
    await api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "touch", args: { id: "i1" } },
    });
    await api.createQueryLease({ user: "u1", name: "allIssues", args: null });

    assert.ok(
      sqlPaths.some((p) => p.startsWith("/v1/sql/")),
      "SQL rides the explicit database client",
    );
    assert.equal(materialized.length, 1, "the control plane rides the explicit daemon");
    assert.deepEqual(edge.seen, [], "nothing reaches the derived origin when both fields are explicit");
    api.close();
  } finally {
    await edge.close();
  }
});

test("an explicit daemon does not advertise the rindle SQL origin as its socket", async () => {
  const edge = await fakeEdge();
  try {
    const materialized: MaterializeInput[] = [];
    const daemon = {
      async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
        materialized.push(input);
        return { materializationId: "mat-mem", leaseToken: "lease-mem" };
      },
    } as unknown as RindleDaemonClient;
    const api = createRindleApiServer({
      rindle: { url: edge.url, token: "sekret" },
      daemon,
      queries: queries(),
      mutators: mutators(),
    });

    await api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "touch", args: { id: "i1" } },
    });
    const lease = await api.createQueryLease({ user: "u1", name: "allIssues", args: null });

    assert.ok(edge.seen.some((s) => s.path.startsWith("/v1/sql/")), "the SQL layer still derives from rindle");
    assert.equal(materialized.length, 1, "the lease comes from the explicit daemon");
    assert.equal("wsEndpoint" in lease, false, "the lease does not advertise the unrelated SQL origin");
    api.close();
  } finally {
    await edge.close();
  }
});

test("an explicit rindle.wsUrl accompanies an explicit daemon", async () => {
  const daemon = {
    async materialize(): Promise<MaterializeOutput> {
      return { materializationId: "mat-mem", leaseToken: "lease-mem" };
    },
  } as unknown as RindleDaemonClient;
  const api = createRindleApiServer({
    rindle: {
      url: "https://sql.example",
      token: "sekret",
      wsUrl: "wss://explicit-daemon.example/subscribe",
    },
    daemon,
    queries: queries(),
  });
  try {
    const lease = await api.createQueryLease({ user: "u1", name: "allIssues", args: null });
    assert.equal(lease.wsEndpoint, "wss://explicit-daemon.example/subscribe");
  } finally {
    api.close();
  }
});

test("rindle is inert when every leg is explicit — no unified URL demanded", () => {
  const savedUrl = process.env.RINDLE_URL;
  const savedToken = process.env.RINDLE_DATABASE_TOKEN;
  delete process.env.RINDLE_URL;
  delete process.env.RINDLE_DATABASE_TOKEN;
  try {
    const fetch: Fetch = async () =>
      new Response(JSON.stringify({}), { status: 200, headers: { "content-type": "application/json" } });
    const daemon = {} as unknown as RindleDaemonClient;
    // Explicit daemon + database: nothing to derive, so `rindle: {}` outside `rindle dev`
    // must not throw — explicit fields always win.
    const api = createRindleApiServer({
      rindle: {},
      daemon,
      database: { url: "https://db.example", authToken: "explicit", fetch },
    });
    api.close();
  } finally {
    if (savedUrl !== undefined) process.env.RINDLE_URL = savedUrl;
    if (savedToken !== undefined) process.env.RINDLE_DATABASE_TOKEN = savedToken;
  }
});

test("an explicit database still needs the unified token when the daemon is derived", async () => {
  const savedToken = process.env.RINDLE_DATABASE_TOKEN;
  delete process.env.RINDLE_DATABASE_TOKEN;
  const edge = await fakeEdge();
  try {
    const fetch: Fetch = async () =>
      new Response(JSON.stringify({}), { status: 200, headers: { "content-type": "application/json" } });
    assert.throws(
      () =>
        createRindleApiServer({
          rindle: { url: edge.url },
          database: { url: "https://db.example", authToken: "explicit", fetch },
          queries: queries(),
        }),
      /RINDLE_DATABASE_TOKEN/,
    );
  } finally {
    if (savedToken !== undefined) process.env.RINDLE_DATABASE_TOKEN = savedToken;
    await edge.close();
  }
});

test("construction without any connection fails with an actionable error", () => {
  assert.throws(() => createRindleApiServer({}), /rindle: \{ url, token \}/);
});

test("rindle without a resolvable url names the missing export", () => {
  const saved = process.env.RINDLE_URL;
  delete process.env.RINDLE_URL;
  try {
    assert.throws(() => createRindleApiServer({ rindle: {} }), /RINDLE_URL/);
  } finally {
    if (saved !== undefined) process.env.RINDLE_URL = saved;
  }
});

test("rindle without a resolvable token fails loudly instead of downgrading to the daemon plane", () => {
  const saved = process.env.RINDLE_DATABASE_TOKEN;
  delete process.env.RINDLE_DATABASE_TOKEN;
  try {
    assert.throws(
      () => createRindleApiServer({ rindle: { url: "http://127.0.0.1:1" } }),
      /RINDLE_DATABASE_TOKEN/,
    );
  } finally {
    if (saved !== undefined) process.env.RINDLE_DATABASE_TOKEN = saved;
  }
});

test("rindle.wsUrl overrides the endpoint returned on query leases", async () => {
  const edge = await fakeEdge();
  try {
    const api = createRindleApiServer({
      rindle: { url: edge.url, token: "sekret", wsUrl: "wss://subscriptions.example/rindle" },
      queries: queries(),
    });
    const lease = await api.createQueryLease({ user: "u1", name: "allIssues", args: null });
    assert.equal(lease.wsEndpoint, "wss://subscriptions.example/rindle");
    api.close();
  } finally {
    await edge.close();
  }
});
