// The ONE topology (design 214) read/write split. These tests pin the app-level seam: `createForumApi`
// must route writes to the `rindle-replicator` write-master and reads/control to the follower, and the
// env resolver must produce that split from `RINDLE_FOLLOWER_URL` + `RINDLE_REPLICATOR_URL`. The
// missing-master configuration must fail closed. The deep `SplitDaemonClient` routing contract itself
// is covered in `@rindle/api-server`'s routing.test.ts; here we assert the forum's wiring + resolver.

import { test } from "node:test";
import assert from "node:assert/strict";

import type { FetchLike, FetchResponseLike } from "@rindle/daemon-client";

import { createForumApi, resolveForumDaemon } from "../server/app-api.ts";

const FOLLOWER = "http://follower.local";
const REPLICATOR = "http://replicator.local";

/** A `FetchLike` that records the base origin each control-plane path was POSTed to, and answers with
 *  the minimal valid JSON the daemon-client expects per endpoint. */
function recordingFetch(hits: { url: string; path: string }[]): FetchLike {
  return async (input, _init): Promise<FetchResponseLike> => {
    const u = new URL(input);
    hits.push({ url: `${u.protocol}//${u.host}`, path: u.pathname });
    const body =
      u.pathname === "/materialize"
        ? { materializationId: "m1", leaseToken: "L1" }
        : u.pathname === "/execute-sql-txn"
          ? { cv: 1 }
          : // The public mutation receipt: effects + the server-owned lmid watermark.
            u.pathname.startsWith("/v1/sql/mutations/")
            ? { applied: true, lmid: 1, cursor: null }
            : {};
    return { ok: true, status: 200, statusText: "OK", text: async () => JSON.stringify(body) };
  };
}

test("resolveForumDaemon: no replicator url → configuration error", () => {
  assert.throws(
    () =>
      resolveForumDaemon(
        { RINDLE_DAEMON_URL: "http://d.local", RINDLE_DAEMON_TOKEN: "tok" },
        { daemonUrl: "http://default", daemonToken: "default-tok" },
      ),
    /RINDLE_REPLICATOR_URL is required/,
  );
});

test("resolveForumDaemon: follower + replicator → split, token defaults to the follower's", () => {
  const d = resolveForumDaemon(
    {
      RINDLE_FOLLOWER_URL: FOLLOWER,
      RINDLE_REPLICATOR_URL: REPLICATOR,
      RINDLE_DAEMON_TOKEN: "shared",
      RINDLE_DATABASE_TOKEN: "sql-tok",
    },
    { daemonUrl: "http://default", daemonToken: "default-tok" },
  );
  assert.equal(d.daemonUrl, FOLLOWER);
  assert.deepEqual(d.writeDaemon, { url: REPLICATOR, token: "shared" });
});

test("resolveForumDaemon: an explicit replicator token overrides the follower's", () => {
  const d = resolveForumDaemon(
    {
      RINDLE_FOLLOWER_URL: FOLLOWER,
      RINDLE_REPLICATOR_URL: REPLICATOR,
      RINDLE_REPLICATOR_TOKEN: "write-only",
      RINDLE_DATABASE_TOKEN: "sql-tok",
    },
    { daemonUrl: "http://default", daemonToken: "default-tok" },
  );
  assert.equal(d.writeDaemon?.token, "write-only");
});

test("createForumApi (split): reads hit the follower, writes hit the replicator", async () => {
  const hits: { url: string; path: string }[] = [];
  const api = createForumApi({
    daemonUrl: FOLLOWER,
    daemonToken: "follower-tok",
    writeDaemon: { url: REPLICATOR, token: "replicator-tok" },
    databaseToken: "sql-tok",
    fetch: recordingFetch(hits),
  });

  // A read mints a lease — materialize must go to the follower, never the replicator.
  await api.createQueryLease({ user: undefined, name: "categories", args: null });
  const materialize = hits.find((h) => h.path === "/materialize");
  assert.equal(materialize?.url, FOLLOWER, "reads route to the follower");

  // A write runs the mutator's SQL — the public mutation surface must go to the write-master.
  await api.pushMutation({
    user: { subject: "user-1", displayName: "Ann" },
    envelope: {
      clientID: "c1",
      mid: 1,
      name: "upvote",
      args: { postId: "p1", author: "user-1", createdAt: 1 },
    },
  });
  const write = hits.find((h) => h.path === "/v1/sql/mutations/execute");
  assert.equal(write?.url, REPLICATOR, "writes route to the replicator");
  assert.equal(
    hits.some((h) => h.path.startsWith("/v1/sql/") && h.url === FOLLOWER),
    false,
    "a write must never reach the follower",
  );
});
