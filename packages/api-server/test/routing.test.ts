import { test } from "node:test";
import assert from "node:assert/strict";

import type {
  DematerializeInput,
  DematerializeOutput,
  ClaimRoomEpochInput,
  ClaimRoomEpochOutput,
  MaterializeInput,
  MaterializeOutput,
  MigrateInput,
  MigrateOutput,
  MutationRejection,
  MutationRejectionOutput,
  QueryOnceInput,
  QueryOnceOutput,
  RindleDaemonClient,
  RoomLmidsInput,
  RoomLmidsOutput,
  RowChangeTxn,
  RowChangeTxnOutput,
  SqlRead,
  SqlReadOutput,
  SqlTxn,
  SqlTxnOutput,
} from "@rindle/daemon-client";

import {
  SplitDaemonClient,
  createRindleApiServer,
  defineApiMutators,
  defineApiQueries,
  type PinFanout,
} from "../src/index.ts";

/** A recording daemon: captures the inputs each control-plane call receives. */
class RecordingDaemon implements RindleDaemonClient {
  materialized: MaterializeInput[] = [];
  reads: QueryOnceInput[] = [];
  sqlTxns: SqlTxn[] = [];
  sqlReads: SqlRead[] = [];
  roomApplies: RowChangeTxn[] = [];
  roomClaims: ClaimRoomEpochInput[] = [];
  roomLmidReads: RoomLmidsInput[] = [];
  async materialize(i: MaterializeInput): Promise<MaterializeOutput> {
    this.materialized.push(i);
    return { materializationId: `m${this.materialized.length}`, leaseToken: `L${this.materialized.length}` };
  }
  async query(i: QueryOnceInput): Promise<QueryOnceOutput> {
    this.reads.push(i);
    return { rows: [{ cols: { id: 1 } }], cvMin: 5 };
  }
  async dematerialize(_i: DematerializeInput): Promise<DematerializeOutput> {
    return { removed: true };
  }
  async executeSqlTxn(i: SqlTxn): Promise<SqlTxnOutput> {
    this.sqlTxns.push(i);
    return { cv: this.sqlTxns.length };
  }
  async executeSqlRead(i: SqlRead): Promise<SqlReadOutput> {
    this.sqlReads.push(i);
    return { cols: [], rows: [] };
  }
  async rejectMutation(i: MutationRejection): Promise<MutationRejectionOutput> {
    return { lmid: i.mid };
  }
  async applyRowChangeTxn(i: RowChangeTxn): Promise<RowChangeTxnOutput> {
    this.roomApplies.push(i);
    return { cv: 1 };
  }
  async claimRoomEpoch(i: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput> {
    this.roomClaims.push(i);
    return { epoch: this.roomClaims.length };
  }
  async roomLmids(i: RoomLmidsInput): Promise<RoomLmidsOutput> {
    this.roomLmidReads.push(i);
    return { lmids: {} };
  }
  async migrate(_i: MigrateInput): Promise<MigrateOutput> {
    return { applied: true };
  }
}

const allIssues = defineApiQueries({ all: () => ({ table: "issue" }) });

test("the browser clientId becomes metadata.routingKey when there is no subject", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({ daemon, queries: allIssues });
  await api.handleQueryJson({ name: "all", clientId: "client-xyz" }, { user: "u1" });
  assert.deepEqual(daemon.materialized[0].metadata, { routingKey: "client-xyz" });
});

test("the browser's affinity ticket is forwarded (opaquely) on materialize", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({ daemon, queries: allIssues });
  await api.handleQueryJson({ name: "all", clientId: "c1", affinity: "aff.p.s" }, { user: "u1" });
  assert.equal(daemon.materialized[0].affinity, "aff.p.s");
});

test("no affinity ticket ⇒ materialize carries none (single-daemon / off, byte-identical)", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({ daemon, queries: allIssues });
  await api.handleQueryJson({ name: "all", clientId: "c1" }, { user: "u1" });
  assert.equal("affinity" in daemon.materialized[0], false);
});

test("the affinity ticket is forwarded on an SSR one-shot read too", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({ daemon, queries: allIssues });
  await api.handleReadJson({ name: "all", clientId: "c1", affinity: "aff.q.z" }, { user: "u1" });
  assert.equal(daemon.reads[0].affinity, "aff.q.z");
});

test("SplitDaemonClient forwards the affinity ticket to the READS leg (never the master)", async () => {
  const master = new RecordingDaemon();
  const reads = new RecordingDaemon();
  const api = createRindleApiServer({ daemon: new SplitDaemonClient(master, reads), queries: allIssues });
  await api.handleQueryJson({ name: "all", clientId: "c1", affinity: "aff.pin.m2" }, { user: "u1" });
  assert.equal(reads.materialized[0].affinity, "aff.pin.m2");
  assert.equal(master.materialized.length, 0, "the ticket rides the read leg only");
});

test("subject is passed for routing; a custom routingKey resolver overrides clientId", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: allIssues,
    subject: ({ user }) => `user:${user}`,
    routingKey: ({ request }) => (request as { cookie?: string } | undefined)?.cookie,
  });
  await api.createQueryLease({ user: "alice", name: "all", args: null, request: { cookie: "sess-abc" }, clientId: "ignored" });
  assert.equal(daemon.materialized[0].subject, "user:alice");
  assert.deepEqual(daemon.materialized[0].metadata, { routingKey: "sess-abc" });
});

test("readQuery uses subject ?? routingKey as the visibilityKey", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({ daemon, queries: allIssues });
  await api.handleReadJson({ name: "all", clientId: "client-7" }, { user: "u1" });
  assert.equal(daemon.reads[0].visibilityKey, "client-7");
});

test("readQuery prefers the authenticated subject as the visibility key", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({ daemon, queries: allIssues, subject: ({ user }) => `user:${user}` });
  await api.handleReadJson({ name: "all", clientId: "client-7" }, { user: "bob" });
  assert.equal(daemon.reads[0].visibilityKey, "user:bob");
});

test("lazy pin tier: a leased query that is also a pin is forced to a pinned policy", async () => {
  const daemon = new RecordingDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({ hot: () => ({ table: "item" }), cold: () => ({ table: "item2" }) }),
    pinnedQueries: [{ name: "hot" }],
    materializationPolicy: { kind: "whileSubscribed" },
  });
  await api.createQueryLease({ user: "u1", name: "hot", args: null });
  await api.createQueryLease({ user: "u1", name: "cold", args: null });
  assert.deepEqual(daemon.materialized[0].policy, { kind: "pinned", name: "hot" });
  assert.deepEqual(daemon.materialized[1].policy, { kind: "whileSubscribed" });
});

test("SplitDaemonClient routes writes to the master and reads to the fleet", async () => {
  const master = new RecordingDaemon();
  const reads = new RecordingDaemon();
  const api = createRindleApiServer({
    daemon: new SplitDaemonClient(master, reads),
    queries: allIssues,
    mutators: defineApiMutators({ create: (tx) => tx.exec("INSERT INTO issue DEFAULT VALUES") }),
  });

  await api.createQueryLease({ user: "u1", name: "all", args: null });
  assert.equal(reads.materialized.length, 1);
  assert.equal(master.materialized.length, 0, "reads never hit the master");

  await api.pushMutation({ user: "u1", envelope: { clientID: "c1", mid: 1, name: "create", args: null } });
  assert.equal(master.sqlTxns.length, 1);
  assert.equal(reads.sqlTxns.length, 0, "writes never hit the read follower");
});

test("SplitDaemonClient defaults raw reads to the follower, routes strong reads to the master", async () => {
  const master = new RecordingDaemon();
  const reads = new RecordingDaemon();
  const daemon = new SplitDaemonClient(master, reads);

  // default (eventual): scaled off the master, onto the read router
  await daemon.executeSqlRead({ sql: "SELECT 1", params: [] });
  assert.equal(reads.sqlReads.length, 1, "default read goes to the router");
  assert.equal(master.sqlReads.length, 0, "default read never touches the master");

  // explicit eventual is the same as default
  await daemon.executeSqlRead({ sql: "SELECT 2", consistency: "eventual" });
  assert.equal(reads.sqlReads.length, 2);

  // strong: read-your-writes → the master
  await daemon.executeSqlRead({ sql: "SELECT 3", consistency: "strong" });
  assert.equal(master.sqlReads.length, 1, "strong read goes to the master");
  assert.equal(reads.sqlReads.length, 2, "strong read does not touch the router");
});

test("SplitDaemonClient routes room authority calls to the master", async () => {
  const master = new RecordingDaemon();
  const reads = new RecordingDaemon();
  const daemon = new SplitDaemonClient(master, reads);

  await daemon.applyRowChangeTxn({ source: "room:d:1", offset: "1", changes: [] });
  await daemon.claimRoomEpoch({ doc: "d" });
  await daemon.roomLmids({ doc: "d", clients: ["c1"] });

  assert.equal(master.roomApplies.length, 1);
  assert.equal(master.roomClaims.length, 1);
  assert.equal(master.roomLmidReads.length, 1, "the authoritative boot probe cannot read a lagging follower");
  assert.equal(reads.roomApplies.length + reads.roomClaims.length + reads.roomLmidReads.length, 0);
});

test("assertPins fans out via the configured pinFanout, not the per-pin daemon path", async () => {
  const daemon = new RecordingDaemon();
  const fanned: MaterializeInput[][] = [];
  const pinFanout: PinFanout = { assertPins: async (pins) => void fanned.push([...pins]) };
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({
      hot: () => ({ table: "item" }),
      latest: () => ({ table: "item", orderBy: [["t", "desc"]] }),
    }),
    pinnedQueries: [{ name: "hot", args: null }, { name: "latest" }],
    pinFanout,
    pinUser: "system",
  });

  await api.assertPins();

  assert.equal(fanned.length, 1, "one fleet fan-out call");
  assert.equal(fanned[0].length, 2, "both pins in it");
  assert.ok(fanned[0].every((r) => r.policy?.kind === "pinned"));
  assert.equal(daemon.materialized.length, 0, "the single-daemon path was NOT used");
});
