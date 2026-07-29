// The room write-authority host (RINDLE-REALTIME-DESIGN.md §5.3.1): the API server is
// the room's SOLE flush counterpart. Three properties gated here: the endpoints are
// DISABLED until the app opts in with `authorizeRoom` (hosting an authority is never a
// default); the gate actually gates; and the store's verdict — fence, CAS conflict,
// the loud identity 500 — passes through VERBATIM (status + body), because the room's
// `httpAuthority` decodes those exact shapes.

import assert from "node:assert/strict";
import test from "node:test";

import { DaemonHttpError } from "@rindle/daemon-client";
import type {
  ClaimRoomEpochInput,
  ClaimRoomEpochOutput,
  RindleDaemonClient,
  RoomLmidsInput,
  RoomLmidsOutput,
  RowChangeTxn,
  RowChangeTxnOutput,
} from "@rindle/daemon-client";

import { createRindleApiServer, RindleApiError } from "../src/index.ts";

/** A daemon that answers the room trio and records what reached it; `verdict` makes
 *  the next apply throw the given daemon HTTP error (the store's refusal). */
function roomDaemon() {
  const applied: RowChangeTxn[] = [];
  let verdict: DaemonHttpError | null = null;
  const daemon = {
    applied,
    setVerdict(status: number, body: unknown) {
      verdict = new DaemonHttpError(status, "refused", JSON.stringify(body));
    },
    async applyRowChangeTxn(input: RowChangeTxn): Promise<RowChangeTxnOutput> {
      if (verdict) {
        const v = verdict;
        verdict = null;
        throw v;
      }
      applied.push(input);
      return { applied: true, cv: 7 };
    },
    async claimRoomEpoch(input: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput> {
      assert.equal(input.doc, "doc-1");
      return { epoch: 3 };
    },
    async roomLmids(input: RoomLmidsInput): Promise<RoomLmidsOutput> {
      return { lmids: Object.fromEntries(input.clients.map((c) => [c, 5])) };
    },
    // The viewer surface is irrelevant here.
    materialize: () => Promise.reject(new Error("unused")),
    dematerialize: () => Promise.reject(new Error("unused")),
    executeSqlTxn: () => Promise.reject(new Error("unused")),
    executeSqlRead: () => Promise.reject(new Error("unused")),
    rejectMutation: () => Promise.reject(new Error("unused")),
    query: () => Promise.reject(new Error("unused")),
    migrate: () => Promise.reject(new Error("unused")),
  };
  return daemon as typeof daemon & RindleDaemonClient;
}

const txnBody = {
  source: "room:doc-1:3",
  offset: "00000000000000000001",
  doc: "doc-1",
  epoch: 3,
  batchHash: "1111111111111111",
  cas: true,
  changes: [{ table: "issue", op: "add", row: [1, "a"] }],
};

test("room endpoints are disabled until authorizeRoom is configured", async () => {
  const api = createRindleApiServer({ daemon: roomDaemon() });
  await assert.rejects(
    api.handleApplyRowChangeTxnJson(txnBody, { user: "room" }),
    (e: unknown) =>
      e instanceof RindleApiError && e.status === 403 && /not configured/.test(e.message),
  );
});

test("the gate gates, and a pass forwards the txn verbatim", async () => {
  const daemon = roomDaemon();
  const api = createRindleApiServer<string>({
    daemon,
    authorizeRoom: ({ user }) => user === "placed-room",
  });
  await assert.rejects(
    api.handleApplyRowChangeTxnJson(txnBody, { user: "intruder" }),
    (e: unknown) => e instanceof RindleApiError && e.status === 403,
  );
  const out = await api.handleApplyRowChangeTxnJson(txnBody, { user: "placed-room" });
  assert.deepEqual(out, { status: 200, body: { applied: true, cv: 7 } });
  assert.equal(daemon.applied.length, 1);
  assert.deepEqual(daemon.applied[0], txnBody, "the txn crossed unmodified");

  const claim = await api.handleClaimRoomEpochJson({ doc: "doc-1" }, { user: "placed-room" });
  assert.deepEqual(claim, { status: 200, body: { epoch: 3 } });
  const lmids = await api.handleRoomLmidsJson(
    { doc: "doc-1", clients: ["c1", "c2"] },
    { user: "placed-room" },
  );
  assert.deepEqual(lmids, { status: 200, body: { lmids: { c1: 5, c2: 5 } } });
});

test("the store's verdicts pass through verbatim: fence, conflict, identity", async () => {
  const daemon = roomDaemon();
  const api = createRindleApiServer<string>({ daemon, authorizeRoom: () => true });
  const ctx = { user: "room" };

  daemon.setVerdict(409, { error: "fenced", currentEpoch: 4 });
  assert.deepEqual(await api.handleApplyRowChangeTxnJson(txnBody, ctx), {
    status: 409,
    body: { error: "fenced", currentEpoch: 4 },
  });

  const conflicts = [{ table: "issue", pk: [1], current: [1, "theirs"] }];
  daemon.setVerdict(409, { error: "conflict", conflicts });
  assert.deepEqual(await api.handleApplyRowChangeTxnJson(txnBody, ctx), {
    status: 409,
    body: { error: "conflict", conflicts },
  });

  daemon.setVerdict(500, { error: "batch identity mismatch: same flush id, different body" });
  const identity = await api.handleApplyRowChangeTxnJson(txnBody, ctx);
  assert.equal(identity.status, 500);
  assert.match(JSON.stringify(identity.body), /batch identity mismatch/);
});
