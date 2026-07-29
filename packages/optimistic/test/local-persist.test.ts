// Local-table persistence (207-LOCAL-TABLE-PERSISTENCE-DESIGN.md) — the §11.5 test/oracle plan
// over the real wasm engine + a FAKE IDB/BroadcastChannel/Web-Locks environment (the `PersistEnv`
// seam), so multi-tab topologies, leader crashes, and persist-window timing are all deterministic:
//   (a) reload survival — write, "reload" (new backend over the same fake IDB), restored == pre;
//   (b) two-tab convergence — interleaved writes; engines, mirrors, and IDB agree row-for-row;
//   (c) leader-kill in each window (pre-persist / post-persist-pre-broadcast / post-broadcast) —
//       no loss except the documented unacked-tail; P5's promotion diff recovers the P1 gap;
//   (d) schema-hash mismatch clears and never half-loads (P7);
//   (e) M2 corruption probe — a corrupt record naming a SYNCED table never reaches the engine
//       (restore drops it, §3.1; a corrupt commit dies loudly at WasmBackend.writeLocal, P8);
//   (f) the 201 suite is untouched (local-tables.test.ts runs unchanged in this same run).
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  initWasm,
  localSchemaHash,
  number,
  string,
  table,
  type MutationEnvelope,
  type NormalizedEvent,
  type NormalizedTableSchema,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
  type WireValue,
} from "@rindle/wasm";
import { rowsEqual } from "@rindle/client";
import { createOptimisticStore } from "../src/index.ts";
import {
  attachLocalPersistence,
  defaultEnv,
  deleteLocalPersistence,
  type PersistChannel,
  type PersistDb,
  type PersistEnv,
  type PersistMeta,
  type RowState,
  type StoredRow,
} from "../src/local-persist.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const selection = table("selection", { local: true }).columns({ id: number(), issueId: number() }).primaryKey("id");
const draft = table("draft", { local: true }).columns({ id: number(), text: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, selection, draft] });

const USER = "u1";
const DB = `rindle-local:${USER}`;

// ---------------------------------------------------------------------------------------------
// The fake environment: one Hub = one "browser profile" (shared IDB data, channel bus, lock table)
// ---------------------------------------------------------------------------------------------

interface HubDbData {
  rows: Map<string, StoredRow>; // key `${table}\0${pkKey}`
  meta?: PersistMeta;
}

interface ChannelInst {
  tab: Tab;
  closed: boolean;
  handler: ((msg: unknown) => void) | null;
}

interface LockGrant {
  tab: Tab;
  onAcquired: () => Promise<void>;
}

interface LockRec {
  holder: LockGrant | null;
  queue: LockGrant[];
}

class Hub {
  readonly dbs = new Map<string, HubDbData>();
  readonly channels = new Map<string, Set<ChannelInst>>();
  readonly locks = new Map<string, LockRec>();

  db(name: string): HubDbData {
    let d = this.dbs.get(name);
    if (!d) this.dbs.set(name, (d = { rows: new Map() }));
    return d;
  }

  lock(name: string): LockRec {
    let l = this.locks.get(name);
    if (!l) this.locks.set(name, (l = { holder: null, queue: [] }));
    return l;
  }

  grant(rec: LockRec, g: LockGrant): void {
    rec.holder = g;
    queueMicrotask(() => {
      if (!g.tab.alive) {
        this.release(rec, g);
        return;
      }
      void g.onAcquired().then(() => this.release(rec, g));
    });
  }

  release(rec: LockRec, g: LockGrant): void {
    if (rec.holder !== g) return;
    rec.holder = null;
    const next = rec.queue.shift();
    if (next) this.grant(rec, next);
  }

  /** IDB row snapshot as `{table pkKey} → row`, for row-for-row assertions. */
  idbRows(): Map<string, WireValue[]> {
    const out = new Map<string, WireValue[]>();
    const d = this.dbs.get(DB);
    if (d) for (const [k, rec] of d.rows) out.set(k, rec.row);
    return out;
  }
}

/** One simulated tab: its env + a `crash()` (unclean death: channel unhooked, locks force-released,
 *  in-flight IDB ops hang forever) and per-putBatch hooks to hit the §11.5(c) windows exactly. */
class Tab {
  alive = true;
  beforePut: (() => void) | null = null;
  afterPut: (() => void) | null = null;
  readonly env: PersistEnv;
  private readonly channels: Array<{ name: string; inst: ChannelInst }> = [];
  private readonly grants: Array<{ rec: LockRec; grant: LockGrant }> = [];

  private readonly hub: Hub;

  constructor(hub: Hub) {
    this.hub = hub;
    const tab = this;
    this.env = {
      openDatabase: (name) => Promise.resolve(tab.makeDb(hub.db(name))),
      deleteDatabase: (name) => {
        hub.dbs.delete(name);
        return Promise.resolve();
      },
      createChannel: (name) => tab.makeChannel(name),
      requestLock: (name, signal, onAcquired) => {
        const rec = hub.lock(name);
        const grant: LockGrant = { tab, onAcquired };
        tab.grants.push({ rec, grant });
        signal.addEventListener("abort", () => {
          const i = rec.queue.indexOf(grant);
          if (i >= 0) rec.queue.splice(i, 1);
        });
        if (rec.holder) rec.queue.push(grant);
        else hub.grant(rec, grant);
      },
    };
  }

  crash(): void {
    this.alive = false;
    for (const { name, inst } of this.channels) {
      inst.closed = true;
      this.hub.channels.get(name)?.delete(inst);
    }
    for (const { rec, grant } of this.grants) {
      const i = rec.queue.indexOf(grant);
      if (i >= 0) rec.queue.splice(i, 1);
      this.hub.release(rec, grant); // force-release: the browser drops a dead tab's lock
    }
  }

  private makeChannel(name: string): PersistChannel {
    const tab = this;
    let set = this.hub.channels.get(name);
    if (!set) this.hub.channels.set(name, (set = new Set()));
    const inst: ChannelInst = { tab, closed: false, handler: null };
    set.add(inst);
    this.channels.push({ name, inst });
    const peers = set;
    return {
      post(msg: unknown): void {
        if (!tab.alive || inst.closed) return;
        const copy = structuredClone(msg);
        for (const other of peers) {
          if (other === inst) continue; // BroadcastChannel never self-delivers
          queueMicrotask(() => {
            if (!other.closed && other.tab.alive) other.handler?.(structuredClone(copy));
          });
        }
      },
      onMessage(h: (msg: unknown) => void): void {
        inst.handler = h;
      },
      close(): void {
        inst.closed = true;
        peers.delete(inst);
      },
    };
  }

  private makeDb(data: HubDbData): PersistDb {
    const tab = this;
    const alive = <T>(v: T): Promise<T> => (tab.alive ? Promise.resolve(v) : new Promise<T>(() => {}));
    return {
      getMeta: () => alive(data.meta ? structuredClone(data.meta) : undefined),
      putMeta: (meta) => {
        if (!tab.alive) return new Promise(() => {});
        data.meta = structuredClone(meta);
        return Promise.resolve();
      },
      getAllRows: () => alive([...data.rows.values()].map((r) => structuredClone(r))),
      putBatch: (batch: RowState[]) => {
        tab.beforePut?.(); // window (c1): crash BEFORE the txn commits
        if (!tab.alive) return new Promise(() => {});
        for (const s of batch) {
          const key = `${s.table}\0${s.pkKey}`;
          if (s.row === null) data.rows.delete(key);
          else data.rows.set(key, structuredClone({ table: s.table, pkKey: s.pkKey, row: s.row }));
        }
        tab.afterPut?.(); // window (c2): txn committed, crash BEFORE the broadcast
        return Promise.resolve();
      },
      reset: (meta) => {
        if (!tab.alive) return new Promise(() => {});
        data.rows.clear();
        data.meta = structuredClone(meta);
        return Promise.resolve();
      },
      deleteRows: (keys) => {
        if (!tab.alive) return new Promise(() => {});
        for (const k of keys) data.rows.delete(`${k.table}\0${k.pkKey}`);
        return Promise.resolve();
      },
      close: () => {},
    };
  }
}

/** Drain microtask+macrotask churn (channel delivery, persist chains, promotions). */
async function settle(rounds = 30): Promise<void> {
  for (let i = 0; i < rounds; i++) await new Promise((r) => setImmediate(r));
}

// ---------------------------------------------------------------------------------------------
// A client "tab": real wasm backend + scripted source + the layer over the tab's fake env
// ---------------------------------------------------------------------------------------------

class TestSource implements OptimisticSource {
  registerQuery(_qid: QueryId, _remote: RemoteQuery): void {}
  unregisterQuery(_qid: QueryId): void {}
  pushMutation(_env: MutationEnvelope): Promise<void> {
    return Promise.resolve();
  }
  onNormalized(_h: (qid: QueryId, ev: NormalizedEvent) => void): void {}
  onProgress(_h: (frame: ProgressFrame) => void): void {}
  expectClientSchema(_tables: NormalizedTableSchema[]): void {}
}

let nextClient = 0;

function makeClientTab(hub: Hub, opts?: { schema?: typeof schema; env?: PersistEnv }) {
  const tab = new Tab(hub);
  const s = opts?.schema ?? schema;
  const { store, backend } = createOptimisticStore(s, new TestSource(), {}, { clientID: `c${nextClient++}` });
  const errors: Error[] = [];
  const layer = attachLocalPersistence(backend, s, {
    user: USER,
    env: opts?.env ?? tab.env,
    onError: (e) => errors.push(e),
  });
  const sel = store.query.selection.orderBy("id", "asc").materialize();
  const drafts = store.query.draft.orderBy("id", "asc").materialize();
  return { tab, store, backend, layer, errors, sel, drafts };
}

type ClientTab = ReturnType<typeof makeClientTab>;

function assertConverged(tabs: ClientTab[], msg: string): void {
  const first = tabs[0];
  for (const t of tabs.slice(1)) {
    assert.deepEqual(t.sel.data, first.sel.data, `${msg}: selection views agree`);
    assert.deepEqual(t.drafts.data, first.drafts.data, `${msg}: draft views agree`);
  }
}

/** The engine view rendered as the IDB key space, to assert view == IDB row-for-row. */
function viewAsIdb(t: ClientTab): Map<string, WireValue[]> {
  const out = new Map<string, WireValue[]>();
  for (const r of t.sel.data) out.set(`selection\0${JSON.stringify([r.id])}`, [r.id, r.issueId]);
  for (const r of t.drafts.data) out.set(`draft\0${JSON.stringify([r.id])}`, [r.id, r.text]);
  return out;
}

// ---------------------------------------------------------------------------------------------
// (a) reload survival
// ---------------------------------------------------------------------------------------------

test("(a) reload survival — restored view == pre-reload view (adds, edits, removes, pk-change)", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await settle(); // promotion is async behind ready (§4.6 — restore never waits on the lock)
  assert.equal(t1.layer.role(), "leader", "the only tab is the leader");

  await t1.store.writeLocal((tx) => {
    tx.add("selection", { id: 1, issueId: 10 });
    tx.add("selection", { id: 2, issueId: 20 });
    tx.add("draft", { id: 1, text: "hello" });
  });
  await t1.store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "hello" }, { id: 1, text: "edited" }));
  await t1.store.writeLocal((tx) => tx.remove("selection", { id: 2, issueId: 20 }));
  // A pk-CHANGING edit → tombstone(old) + put(new) (§4.3).
  await t1.store.writeLocal((tx) => tx.edit("selection", { id: 1, issueId: 10 }, { id: 3, issueId: 10 }));
  await settle();

  const preSel = structuredClone(t1.sel.data);
  const preDrafts = structuredClone(t1.drafts.data);
  assert.deepEqual(preSel, [{ id: 3, issueId: 10 }]);
  assert.deepEqual(preDrafts, [{ id: 1, text: "edited" }]);
  t1.layer.close();

  // "Reload": a fresh engine + layer over the SAME durable state.
  const t2 = makeClientTab(hub);
  await t2.layer.ready;
  assert.deepEqual(t2.sel.data, preSel, "selection restored == pre-reload");
  assert.deepEqual(t2.drafts.data, preDrafts, "drafts restored == pre-reload");
  assert.deepEqual(hub.idbRows(), viewAsIdb(t2), "IDB == restored view, row-for-row");
  assert.deepEqual(t2.errors, []);
  t2.layer.close();
});

test("(a) restore never resurrects a pre-restore remove (§4.6 step 4 session tombstones)", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 7, text: "v1" }));
  await settle();
  t1.layer.close();

  // Session 2: write + remove the SAME pk in the pre-restore gap (attach not yet awaited).
  const t2 = makeClientTab(hub);
  await t2.store.writeLocal((tx) => tx.add("draft", { id: 7, text: "v2" }));
  await t2.store.writeLocal((tx) => tx.remove("draft", { id: 7, text: "v2" }));
  await t2.layer.ready;
  await settle();
  assert.deepEqual(t2.drafts.data, [], "the snapshot's stale v1 did not resurrect the removed row");
  assert.equal(hub.idbRows().has(`draft\0${JSON.stringify([7])}`), false, "the tombstone reached IDB");
  assert.deepEqual(t2.errors, []);
  t2.layer.close();
});

// ---------------------------------------------------------------------------------------------
// (b) cross-tab convergence
// ---------------------------------------------------------------------------------------------

test("(b) two-tab convergence — engines, mirrors, and IDB agree row-for-row after quiescence", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  const b = makeClientTab(hub);
  await a.layer.ready;
  await b.layer.ready;
  await settle();
  assert.equal(a.layer.role(), "leader");
  assert.equal(b.layer.role(), "follower");

  // Interleaved writes from both tabs.
  await a.store.writeLocal((tx) => tx.add("selection", { id: 1, issueId: 1 }));
  await b.store.writeLocal((tx) => tx.add("selection", { id: 2, issueId: 2 }));
  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "from-a" }));
  await b.store.writeLocal((tx) => tx.add("draft", { id: 2, text: "b2" }));
  await settle();
  assertConverged([a, b], "first round");

  // A same-row RACE: both tabs edit draft 1 concurrently, before seeing each other's write.
  // LWW in leader commit order (§9): converged, not merged — both engines end identical.
  await a.store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "from-a" }, { id: 1, text: "a-edit" }));
  await b.store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "from-a" }, { id: 1, text: "b-edit" }));
  await a.store.writeLocal((tx) => tx.remove("selection", { id: 1, issueId: 1 }));
  await settle();

  assertConverged([a, b], "after quiescence");
  assert.deepEqual(
    a.drafts.data.find((r) => r.id === 1),
    { id: 1, text: "b-edit" },
    "the later-sequenced edit won on BOTH tabs (LWW in commit order)",
  );
  assert.deepEqual(hub.idbRows(), viewAsIdb(a), "IDB agrees with the converged engines (P1/P2/P3)");
  assert.deepEqual(a.errors, []);
  assert.deepEqual(b.errors, []);

  // A LATE JOINER sees the same state (the §4.6 snapshot+replay join).
  const c = makeClientTab(hub);
  await c.layer.ready;
  await settle();
  assertConverged([a, b, c], "late joiner");
  a.layer.close();
  b.layer.close();
  c.layer.close();
});

test("(b) follower writes are optimistic locally and durable via the leader", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  const b = makeClientTab(hub);
  await b.layer.ready;

  await b.store.writeLocal((tx) => tx.add("draft", { id: 9, text: "follower-write" }));
  // Local-first: b's own view shows it immediately, before any leader round-trip.
  assert.deepEqual(b.drafts.data, [{ id: 9, text: "follower-write" }]);
  await settle();
  assert.deepEqual(a.drafts.data, [{ id: 9, text: "follower-write" }], "leader applied the forwarded op");
  assert.ok(hub.idbRows().has(`draft\0${JSON.stringify([9])}`), "persisted by the leader");
  a.layer.close();
  b.layer.close();
});

// ---------------------------------------------------------------------------------------------
// (c) leader-kill windows
// ---------------------------------------------------------------------------------------------

test("(c1) leader dies PRE-PERSIST — the origin's unacked op survives via resend-on-announce (P4)", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub); // leader
  await a.layer.ready;
  const b = makeClientTab(hub); // next in the lock queue
  const c = makeClientTab(hub); // the op's origin
  await b.layer.ready;
  await c.layer.ready;
  await settle();

  a.tab.beforePut = () => a.tab.crash(); // the accepted batch never reaches disk
  await c.store.writeLocal((tx) => tx.add("selection", { id: 5, issueId: 50 }));
  await settle();

  assert.equal(b.layer.role(), "leader", "the next queued tab was promoted");
  assert.deepEqual(b.sel.data, [{ id: 5, issueId: 50 }], "c's resent op reached the new leader");
  assertConverged([b, c], "after failover");
  assert.deepEqual(hub.idbRows(), viewAsIdb(b), "the new leader persisted it (no loss)");
  b.layer.close();
  c.layer.close();
});

test("(c2) leader dies POST-PERSIST PRE-BROADCAST — the promotion diff recovers the P1 gap (P5)", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub); // leader
  await a.layer.ready;
  const b = makeClientTab(hub);
  await b.layer.ready;
  await settle();

  // The leader's OWN write: persisted, then death before the commit broadcast. No live tab ever
  // saw it — only IDB holds it (the §4.5 case-b gap).
  a.tab.afterPut = () => a.tab.crash();
  await a.store.writeLocal((tx) => tx.add("draft", { id: 42, text: "persisted-unannounced" }));
  await settle();

  assert.equal(b.layer.role(), "leader");
  assert.deepEqual(
    b.drafts.data,
    [{ id: 42, text: "persisted-unannounced" }],
    "promotion re-read IDB and folded the unannounced row into the live engine (P5)",
  );
  assert.deepEqual(hub.idbRows(), viewAsIdb(b));
  assert.deepEqual(b.errors, []);
  b.layer.close();
});

test("(c3) leader dies POST-BROADCAST — clean failover, no duplication, followers keep serving", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  const b = makeClientTab(hub);
  const c = makeClientTab(hub);
  await b.layer.ready;
  await c.layer.ready;

  await a.store.writeLocal((tx) => tx.add("selection", { id: 1, issueId: 1 }));
  await settle(); // commit fully delivered everywhere
  a.tab.crash();
  await settle();

  assert.equal(b.layer.role(), "leader");
  // New writes keep flowing through the new leader.
  await c.store.writeLocal((tx) => tx.add("selection", { id: 2, issueId: 2 }));
  await settle();
  assertConverged([b, c], "after post-broadcast failover");
  assert.deepEqual(
    b.sel.data,
    [
      { id: 1, issueId: 1 },
      { id: 2, issueId: 2 },
    ],
    "no loss, no duplication",
  );
  assert.deepEqual(hub.idbRows(), viewAsIdb(b));
  b.layer.close();
  c.layer.close();
});

test("(c) close() releases leadership to the next tab (P10)", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  const b = makeClientTab(hub);
  await b.layer.ready;
  assert.equal(b.layer.role(), "follower");
  a.layer.close();
  await settle();
  assert.equal(b.layer.role(), "leader", "a graceful close hands the lock over");
  await b.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "post-handover" }));
  await settle();
  assert.ok(hub.idbRows().has(`draft\0${JSON.stringify([1])}`), "the new leader persists");
  b.layer.close();
});

// ---------------------------------------------------------------------------------------------
// (d) the schema-hash gate (P7)
// ---------------------------------------------------------------------------------------------

test("(d) schema-hash mismatch clears and never half-loads (P7)", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "old-shape" }));
  await settle();
  t1.layer.close();
  assert.equal(hub.idbRows().size, 1);

  // The local `draft` table changes shape (an extra column) — a NEW local schema hash.
  const draft2 = table("draft", { local: true })
    .columns({ id: number(), text: string(), kind: string() })
    .primaryKey("id");
  const schema2 = createSchema({ tables: [issue, selection, draft2] });
  assert.notEqual(localSchemaHash(schema2), localSchemaHash(schema), "the local shape moved the hash");

  const tab = new Tab(hub);
  const { store, backend } = createOptimisticStore(schema2, new TestSource(), {}, { clientID: "c-d" });
  const errors: Error[] = [];
  const layer = attachLocalPersistence(backend, schema2, { user: USER, env: tab.env, onError: (e) => errors.push(e) });
  const drafts = store.query.draft.materialize();
  await layer.ready;
  assert.deepEqual(drafts.data, [], "started empty — no wrong-shaped row ever loaded");
  await settle(); // promotion performs the deferred on-disk clear (single-writer, P2)
  assert.equal(hub.idbRows().size, 0, "rows cleared on disk");
  assert.equal(hub.dbs.get(DB)?.meta?.schemaHash, localSchemaHash(schema2), "new hash written");

  // The store is immediately usable under the new shape.
  await store.writeLocal((tx) => tx.add("draft", { id: 1, text: "new-shape", kind: "note" }));
  await settle();
  assert.ok(hub.idbRows().has(`draft\0${JSON.stringify([1])}`), "new-shape rows persist");
  assert.deepEqual(errors, []);
  layer.close();
});

test("(d) a SYNCED-table change does not move the local schema hash (§3.3)", () => {
  const issue2 = table("issue").columns({ id: number(), title: string(), status: string() }).primaryKey("id");
  const schema2 = createSchema({ tables: [issue2, selection, draft] });
  assert.equal(localSchemaHash(schema2), localSchemaHash(schema), "synced changes never invalidate local data");
});

// ---------------------------------------------------------------------------------------------
// (e) the M2 corruption probe (P8)
// ---------------------------------------------------------------------------------------------

test("(e) a hand-planted IDB record naming a SYNCED table never reaches the engine", async () => {
  const hub = new Hub();
  // Seed a valid session so meta carries the right hash, then plant the corrupt record.
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "legit" }));
  await settle();
  t1.layer.close();
  hub.db(DB).rows.set(`issue\0${JSON.stringify([1])}`, { table: "issue", pkKey: JSON.stringify([1]), row: [1, "hax"] });

  const t2 = makeClientTab(hub);
  const issues = t2.store.query.issue.materialize();
  await t2.layer.ready;
  await settle();
  assert.deepEqual(issues.data, [], "the synced table never saw the planted row");
  assert.deepEqual(t2.drafts.data, [{ id: 1, text: "legit" }], "legit rows loaded fine beside it");
  // §3.1: a record whose table is not a CURRENT local table is dropped at restore and swept from
  // disk by the leader.
  assert.equal(hub.idbRows().has(`issue\0${JSON.stringify([1])}`), false, "the corrupt record was swept");
  t2.layer.close();
});

test("(e) a corrupt COMMIT naming a synced table dies loudly at WasmBackend.writeLocal (M2/P8)", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  const issues = t1.store.query.issue.materialize();
  await t1.layer.ready;
  await settle();

  // An adversarial peer posts a commit for a SYNCED table straight onto the channel (the layer's
  // channel is partitioned by schema hash, so a same-version peer is the only one that can).
  const evil = new Tab(hub);
  const ch = evil.env.createChannel(`${DB}::${localSchemaHash(schema)}`)!;
  ch.post({
    t: "commit",
    epoch: 1,
    lseq: 999,
    origin: "evil",
    seq: 1,
    p: true,
    batch: [{ table: "issue", pkKey: JSON.stringify([1]), row: [1, "hax"] }],
  });
  await settle();

  assert.deepEqual(issues.data, [], "the tracked source is untouched");
  assert.equal(t1.errors.length, 1, "the rejection surfaced via onError, loudly");
  assert.match(t1.errors[0].message, /not local-only/, "…with the M2 chokepoint's message");
  t1.layer.close();
});

// ---------------------------------------------------------------------------------------------
// Degrades (§3.2) + lifecycle
// ---------------------------------------------------------------------------------------------

test("no IDB — broadcast-only degrade: coherence without durability, and no throw anywhere", async () => {
  const hub = new Hub();
  const tabA = new Tab(hub);
  const tabB = new Tab(hub);
  const noIdb = (env: PersistEnv): PersistEnv => ({ ...env, openDatabase: () => Promise.resolve(null) });
  const a = makeClientTab(hub, { env: noIdb(tabA.env) });
  const b = makeClientTab(hub, { env: noIdb(tabB.env) });
  await a.layer.ready;
  await b.layer.ready;

  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "ephemeral" }));
  await settle();
  assert.deepEqual(b.drafts.data, [{ id: 1, text: "ephemeral" }], "cross-tab coherence still runs");
  assert.equal(hub.dbs.size, 0, "nothing persisted");
  assert.deepEqual(a.errors, []);
  assert.deepEqual(b.errors, []);
  a.layer.close();
  b.layer.close();
});

test("deleteLocalPersistence drops the user's database (the sanctioned logout hook, §3.2)", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "secret" }));
  await settle();
  t1.layer.close();
  assert.equal(hub.idbRows().size, 1);

  const tab = new Tab(hub);
  await deleteLocalPersistence(USER, tab.env);
  assert.equal(hub.dbs.has(DB), false, "the database is gone");
});

// ---------------------------------------------------------------------------------------------
// local: "session" — the per-table opt-out (§5.4)
// ---------------------------------------------------------------------------------------------

const scratch = table("scratch", { local: "session" }).columns({ id: number(), v: string() }).primaryKey("id");
const sessionSchema = createSchema({ tables: [issue, draft, scratch] });

function makeSessionTab(hub: Hub) {
  const tab = new Tab(hub);
  const { store, backend } = createOptimisticStore(sessionSchema, new TestSource(), {}, { clientID: `s${nextClient++}` });
  const errors: Error[] = [];
  const layer = attachLocalPersistence(backend, sessionSchema, { user: USER, env: tab.env, onError: (e) => errors.push(e) });
  const drafts = store.query.draft.orderBy("id", "asc").materialize();
  const scratchView = store.query.scratch.orderBy("id", "asc").materialize();
  return { tab, store, layer, errors, drafts, scratchView };
}

test('(§5.4) a local:"session" table is never persisted and never replicated — a local:true sibling is both', async () => {
  const hub = new Hub();
  const a = makeSessionTab(hub);
  const b = makeSessionTab(hub);
  await a.layer.ready;
  await b.layer.ready;

  // Session tables still take the full 201 write path (M2 admits them — they ARE local).
  await a.store.writeLocal((tx) => {
    tx.add("scratch", { id: 1, v: "tab-a-only" });
    tx.add("draft", { id: 1, text: "durable" });
  });
  assert.deepEqual(a.scratchView.data, [{ id: 1, v: "tab-a-only" }], "local-first in the writing tab");
  await settle();

  assert.deepEqual(b.drafts.data, [{ id: 1, text: "durable" }], "the persisted sibling replicated");
  assert.deepEqual(b.scratchView.data, [], "the session table did NOT follow to the other tab");
  assert.equal(hub.idbRows().has(`draft\0${JSON.stringify([1])}`), true, "draft persisted");
  assert.equal([...hub.idbRows().keys()].some((k) => k.startsWith("scratch\0")), false, "scratch never persisted");

  // "Reload": the session table comes back empty; the durable one restores.
  a.layer.close();
  b.layer.close();
  const c = makeSessionTab(hub);
  await c.layer.ready;
  assert.deepEqual(c.drafts.data, [{ id: 1, text: "durable" }]);
  assert.deepEqual(c.scratchView.data, [], "session-scoped: empties on reload (the 201 baseline)");
  assert.deepEqual([a.errors, b.errors, c.errors], [[], [], []]);
  c.layer.close();
});

test('(§5.4) reshaping a local:"session" table does not move the schema hash (no P7 wipe)', () => {
  const scratch2 = table("scratch", { local: "session" })
    .columns({ id: number(), v: string(), extra: number() })
    .primaryKey("id");
  const reshaped = createSchema({ tables: [issue, draft, scratch2] });
  assert.equal(localSchemaHash(reshaped), localSchemaHash(sessionSchema), "session tables stay out of the hash");
  assert.equal(localSchemaHash(sessionSchema), localSchemaHash(createSchema({ tables: [issue, draft] })));
});

test('(§5.4) flipping a table local:true → "session" drops and sweeps its persisted rows', async () => {
  const hub = new Hub();
  // Session 1: `draft` is persisted (the module schema) and gets a row.
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "was-durable" }));
  await settle();
  t1.layer.close();

  // Session 2: the app now declares `draft` session-scoped (schema without any persisted table
  // keeps the SAME hash axis — only `local: true` tables count).
  const draftSession = table("draft", { local: "session" }).columns({ id: number(), text: string() }).primaryKey("id");
  const flipped = createSchema({ tables: [issue, selection, draftSession] });
  const tab = new Tab(hub);
  const { store, backend } = createOptimisticStore(flipped, new TestSource(), {}, { clientID: "flip" });
  const errors: Error[] = [];
  const layer = attachLocalPersistence(backend, flipped, { user: USER, env: tab.env, onError: (e) => errors.push(e) });
  const drafts = store.query.draft.materialize();
  await layer.ready;
  await settle();
  assert.deepEqual(drafts.data, [], "the old persisted row was not loaded into a now-session table");
  assert.equal([...hub.idbRows().keys()].some((k) => k.startsWith("draft\0")), false, "…and was swept from disk");
  assert.deepEqual(errors, []);
  layer.close();
});

test('(§5.4) an inbound commit naming a session table is dropped — per-tab isolation holds', async () => {
  const hub = new Hub();
  const t1 = makeSessionTab(hub);
  await t1.layer.ready;
  await settle();

  // A same-hash peer commits a row naming a session table (a corrupt peer: an honest
  // mixed-version peer — `local: true` for scratch — lands on a DIFFERENT channel partition
  // and never reaches this tab at all).
  const evil = new Tab(hub);
  const ch = evil.env.createChannel(`${DB}::${localSchemaHash(sessionSchema)}`)!;
  ch.post({
    t: "commit",
    epoch: 1,
    lseq: 99,
    origin: "peer",
    seq: 1,
    p: true,
    batch: [{ table: "scratch", pkKey: JSON.stringify([9]), row: [9, "leaked"] }],
  });
  await settle();
  assert.deepEqual(t1.scratchView.data, [], "the session table stayed this tab's own");
  assert.deepEqual(t1.errors, [], "dropped silently — mid-deploy skew is normal, not an error");
  t1.layer.close();
});

// ---------------------------------------------------------------------------------------------
// Review regressions (the 2026-07-07 xhigh findings): lifecycle drains, P9 repair, the P7 gate's
// failure modes, echo hold-back, subscriber throws, env split-brain, non-finite keys
// ---------------------------------------------------------------------------------------------

/** An env whose putBatch takes one macrotask — the real-IDB latency window the sync fake hides
 *  (`setImmediate`, not a timer, so `settle()`'s rounds cover it deterministically). */
function slowPutEnv(env: PersistEnv): PersistEnv {
  return {
    ...env,
    openDatabase: async (name) => {
      const db = await env.openDatabase(name);
      if (!db) return db;
      return {
        ...db,
        putBatch: (batch: RowState[]) =>
          new Promise<void>((res, rej) => {
            setImmediate(() => db.putBatch(batch).then(res, rej));
          }),
      };
    },
  };
}

test("close() drains queued persists — a write followed by an immediate close is not lost ([25])", async () => {
  const hub = new Hub();
  const tabA = new Tab(hub);
  const a = makeClientTab(hub, { env: slowPutEnv(tabA.env) });
  await a.layer.ready;
  await settle();
  assert.equal(a.layer.role(), "leader");
  const b = makeClientTab(hub);
  await b.layer.ready;

  // Two rapid writes: v2's persist step queues behind v1's still-in-flight IDB txn; close() lands
  // before either broadcast. The old `closed` gate cancelled both putBatch AND the commit posts.
  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "v1" }));
  await a.store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "v1" }, { id: 1, text: "v2" }));
  a.layer.close(); // same task — no settle first (every pre-fix test settled, hiding this)
  await settle();

  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([1])}`), [1, "v2"], "both writes reached IDB");
  assert.deepEqual(b.drafts.data, [{ id: 1, text: "v2" }], "the commits still broadcast to other tabs");
  assert.equal(b.layer.role(), "leader", "leadership handed over only after the drain");
  assert.deepEqual(a.errors, []);
  b.layer.close();
});

test("a leader's putBatch failure never becomes a failover DELETE — promotion repairs IDB from the mirror (P9/P5)", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  await settle();
  const b = makeClientTab(hub);
  await b.layer.ready;

  // One transient storage error (quota/eviction): the commit still broadcasts (P9) with p: false.
  a.tab.beforePut = () => {
    a.tab.beforePut = null;
    throw new Error("QuotaExceededError (simulated)");
  };
  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "important" }));
  await settle();
  assert.equal(a.errors.length, 1, "the degrade surfaced via onError");
  assert.deepEqual(b.drafts.data, [{ id: 1, text: "important" }], "coherence continued (P9)");
  assert.equal(hub.idbRows().has(`draft\0${JSON.stringify([1])}`), false, "…but IDB missed the batch");

  // The leader dies uncleanly; the follower promotes. Pre-fix, promotionDiff read the IDB gap as
  // a persisted-but-unannounced REMOVE and deleted the live row from every tab.
  a.tab.crash();
  await settle();
  assert.equal(b.layer.role(), "leader");
  assert.deepEqual(b.drafts.data, [{ id: 1, text: "important" }], "the committed row SURVIVED failover");
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([1])}`), [1, "important"], "and was repaired to disk");
  b.layer.close();
});

test("a failed putBatch is retried on the next chain step — same-session durability heals (P9 backlog)", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  await settle();
  a.tab.beforePut = () => {
    a.tab.beforePut = null;
    throw new Error("transient IDB error");
  };
  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "first" }));
  await settle();
  assert.equal(hub.idbRows().has(`draft\0${JSON.stringify([1])}`), false, "first persist failed");

  await a.store.writeLocal((tx) => tx.add("draft", { id: 2, text: "second" }));
  await settle();
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([1])}`), [1, "first"], "backlog repaired");
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([2])}`), [2, "second"]);
  assert.equal(a.errors.length, 1);
  a.layer.close();
});

test("the P7 gate fails CLOSED: a meta read that keeps failing resets instead of stamping the new hash ([2])", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "old-shape" }));
  await settle();
  t1.layer.close();

  // Same row WIDTH, renamed column — the shape guard cannot catch this; only the hash can.
  const draftB = table("draft", { local: true }).columns({ id: number(), body: string() }).primaryKey("id");
  const schemaB = createSchema({ tables: [issue, selection, draftB] });
  const tab = new Tab(hub);
  const flakyEnv: PersistEnv = {
    ...tab.env,
    openDatabase: async (name) => {
      const db = (await tab.env.openDatabase(name))!;
      return { ...db, getMeta: () => Promise.reject(new Error("meta flake")) };
    },
  };
  const { store, backend } = createOptimisticStore(schemaB, new TestSource(), {}, { clientID: "c-p7" });
  const errors: Error[] = [];
  const layer = attachLocalPersistence(backend, schemaB, { user: USER, env: flakyEnv, onError: (e) => errors.push(e) });
  const drafts = store.query.draft.materialize();
  await layer.ready;
  await settle();

  // Pre-fix, the flake returned `needsReset() === false`: putMeta stamped the NEW hash over the
  // OLD-shape rows and promotionDiff folded them into the engine with misassigned columns.
  assert.deepEqual(drafts.data, [], "no old-shape row was ever legitimized into the new-schema engine");
  assert.equal(hub.idbRows().size, 0, "the unverifiable store was cleared (fail closed)");
  assert.equal(hub.dbs.get(DB)?.meta?.schemaHash, localSchemaHash(schemaB), "new hash stamped over a CLEAN store");
  assert.ok(errors.length >= 1);
  layer.close();
});

test("a boot-only meta flake RECOVERS at promotion — rows restore via the P5 diff, no reset", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "keep" }));
  await settle();
  t1.layer.close();

  const tab = new Tab(hub);
  let metaCalls = 0;
  const onceFlakyEnv: PersistEnv = {
    ...tab.env,
    openDatabase: async (name) => {
      const db = (await tab.env.openDatabase(name))!;
      return {
        ...db,
        getMeta: () => (++metaCalls === 1 ? Promise.reject(new Error("boot flake")) : db.getMeta()),
      };
    },
  };
  const t2 = makeClientTab(hub, { env: onceFlakyEnv });
  await t2.layer.ready;
  assert.deepEqual(t2.drafts.data, [], "boot restored nothing (the read failed)");
  await settle(); // promotion re-reads meta authoritatively (under the lock): match → diff, not wipe
  assert.deepEqual(t2.drafts.data, [{ id: 1, text: "keep" }], "promotion folded the store back in");
  assert.equal(hub.idbRows().size, 1, "nothing was wiped");
  assert.equal(t2.errors.length, 1);
  t2.layer.close();
});

test("rapid same-pk writes never rubber-band — the commit echo is held back by the pending tail ([10]/[12])", async () => {
  const hub = new Hub();
  const tabA = new Tab(hub);
  const a = makeClientTab(hub, { env: slowPutEnv(tabA.env) });
  await a.layer.ready;
  await settle();
  assert.equal(a.layer.role(), "leader");

  const history: string[] = [];
  a.drafts.subscribe((rows) => {
    const text = rows.find((r) => r.id === 1)?.text;
    if (text !== undefined) history.push(text); // subscribe() invokes immediately with [] — skip it
  });

  // Three keystrokes, each inside the previous write's IDB-txn window (the design's flagship
  // draft-typing case). Pre-fix the echo of each commit rewound the view: a,ab,abc,a,ab,abc…
  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "a" }));
  await a.store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "a" }, { id: 1, text: "ab" }));
  await a.store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "ab" }, { id: 1, text: "abc" }));
  await settle();

  assert.deepEqual(history, ["a", "ab", "abc"], "monotonic — no snap-back, no spurious re-deliveries");
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([1])}`), [1, "abc"], "IDB folded to the final value");
  assert.deepEqual(a.errors, []);
  a.layer.close();
});

test("a subscriber throwing during a writeLocal's delivery cannot hide the committed batch from the plane ([0])", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  await settle();
  const b = makeClientTab(hub);
  await b.layer.ready;

  let boom = true;
  a.drafts.subscribe((rows) => {
    if (boom && rows.length > 0) {
      // not the immediate subscribe() call — the first real delivery
      boom = false;
      throw new Error("render boom");
    }
  });
  // The engine commits, THEN delivery re-raises the subscriber's error out of writeLocal. The tap
  // fired in between (post-commit, pre-delivery), so persistence/broadcast still happen.
  await assert.rejects(
    Promise.resolve().then(() => a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "kept" }))),
    /render boom/,
  );
  await settle();
  assert.deepEqual(a.drafts.data, [{ id: 1, text: "kept" }], "the commit IS in the origin engine");
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([1])}`), [1, "kept"], "…and in IDB");
  assert.deepEqual(b.drafts.data, [{ id: 1, text: "kept" }], "…and in the other tab");
  a.layer.close();
  b.layer.close();
});

test("a subscriber throwing during a REPLICATED apply cannot desync mirror from engine ([1])", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  await settle();
  const b = makeClientTab(hub);
  await b.layer.ready;

  let boom = true;
  b.drafts.subscribe((rows) => {
    if (boom && rows.length > 0) {
      // not the immediate subscribe() call — the first real (replicated) delivery
      boom = false;
      throw new Error("follower render boom");
    }
  });
  await a.store.writeLocal((tx) => tx.add("draft", { id: 5, text: "v1" }));
  await settle();
  assert.equal(b.errors.length, 1, "the follower's subscriber throw surfaced via onError");
  assert.deepEqual(b.drafts.data, [{ id: 5, text: "v1" }], "the replicated commit is in the follower engine");

  // Pre-fix the mirror missed the batch: this edit would re-diff as an `add` for an existing pk
  // (an engine error / silent IVM corruption). Post-fix it applies as a clean edit.
  await a.store.writeLocal((tx) => tx.edit("draft", { id: 5, text: "v1" }, { id: 5, text: "v2" }));
  await settle();
  assert.deepEqual(b.drafts.data, [{ id: 5, text: "v2" }], "the follower stayed coherent after the throw");
  assert.equal(b.errors.length, 1, "no repeating error loop");
  assertConverged([a, b], "after the throw");
  a.layer.close();
  b.layer.close();
});

test("a lock granted in the close() race window is released, not leaked ([24])", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub);
  await a.layer.ready;
  await settle();
  const b = makeClientTab(hub);
  const c = makeClientTab(hub);
  await b.layer.ready;
  await c.layer.ready;

  // a releases the lock; the manager grants b on a microtask. b closes SYNCHRONOUSLY inside that
  // window (granted, callback not yet fired — abort() is a no-op once granted). Pre-fix, b's
  // `held` promise never resolved: the lock leaked and no tab could ever lead again.
  a.layer.close();
  b.layer.close();
  await settle();
  assert.equal(c.layer.role(), "leader", "the lock moved past the closed tab");
  await c.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "post-race" }));
  await settle();
  assert.ok(hub.idbRows().has(`draft\0${JSON.stringify([1])}`), "the new leader persists normally");
  c.layer.close();
});

test("close() during start()'s open releases the fresh IDB connection ([5])", async () => {
  const hub = new Hub();
  const tab = new Tab(hub);
  let closeCalls = 0;
  let resolveOpen!: (db: PersistDb) => void;
  const gatedEnv: PersistEnv = {
    ...tab.env,
    openDatabase: () => new Promise<PersistDb | null>((r) => (resolveOpen = r)),
  };
  const { backend } = createOptimisticStore(schema, new TestSource(), {}, { clientID: "c-race" });
  const layer = attachLocalPersistence(backend, schema, { user: USER, env: gatedEnv, onError: () => {} });
  layer.close(); // before the open resolves — close() sees db === null
  const real = (await tab.env.openDatabase(DB))!;
  resolveOpen({ ...real, close: () => closeCalls++ });
  await settle();
  assert.equal(closeCalls, 1, "start() closed the handle it could no longer hand to close()");
});

test("mid-deploy schema skew: partitioned channels — old-shape commits never reach a new-schema tab ([11])", async () => {
  const hub = new Hub();
  const a = makeClientTab(hub); // the OLD schema (draft.text), current leader
  await a.layer.ready;
  await settle();

  // A tab on the NEW schema: same row width, renamed column — only the hash differs.
  const draftB = table("draft", { local: true }).columns({ id: number(), body: string() }).primaryKey("id");
  const schemaB = createSchema({ tables: [issue, selection, draftB] });
  const tabB = new Tab(hub);
  const { store: storeB, backend: backendB } = createOptimisticStore(schemaB, new TestSource(), {}, { clientID: "c-new" });
  const errorsB: Error[] = [];
  const layerB = attachLocalPersistence(backendB, schemaB, { user: USER, env: tabB.env, onError: (e) => errorsB.push(e) });
  const draftsB = storeB.query.draft.materialize();
  await layerB.ready;

  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "old-meaning" }));
  await settle();
  assert.deepEqual(draftsB.data, [], "the old leader's live commits never crossed the version boundary");

  // Old tab closes; the new tab promotes, resets the store under ITS hash (P7), and works.
  a.layer.close();
  await settle();
  assert.equal(layerB.role(), "leader");
  assert.equal(hub.dbs.get(DB)?.meta?.schemaHash, localSchemaHash(schemaB), "P7 reset under the new hash");
  assert.equal([...hub.idbRows().keys()].some((k) => k.startsWith("draft\0")), false, "no old-shape row persisted under it");
  await storeB.writeLocal((tx) => tx.add("draft", { id: 9, body: "new-shape" }));
  await settle();
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([9])}`), [9, "new-shape"]);
  assert.deepEqual(errorsB, []);
  layerB.close();
});

test("tabs-without-locks runs the plane INERT — no multi-leader split-brain ([19]/[20])", async () => {
  const hub = new Hub();
  const tabA = new Tab(hub);
  const a = makeClientTab(hub, { env: { ...tabA.env, leaderElectionUnavailable: true } });
  await a.layer.ready; // resolves immediately — construction never hangs
  await a.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "session-scoped" }));
  await settle();
  assert.deepEqual(a.drafts.data, [{ id: 1, text: "session-scoped" }], "local tables still work (201 baseline)");
  assert.equal(a.layer.role(), "follower", "never self-promotes");
  assert.equal(hub.dbs.size, 0, "nothing persisted");
  assert.equal(hub.channels.size, 0, "nothing replicated");
  assert.deepEqual(a.errors, []);
  a.layer.close();
});

test("defaultEnv flags BroadcastChannel-without-Web-Locks as split-brain territory ([19]/[20])", () => {
  // Stub the globals (Safari 15.1–15.3 / Firefox <96 / older Node: BroadcastChannel present,
  // navigator.locks absent) — the environment where the old solo-mode fallback made every
  // context an announced leader over one database.
  const desc = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  Object.defineProperty(globalThis, "navigator", { value: {}, configurable: true });
  try {
    assert.equal(defaultEnv().leaderElectionUnavailable, true, "no locks + channel ⇒ inert plane");
  } finally {
    if (desc) Object.defineProperty(globalThis, "navigator", desc);
    else delete (globalThis as { navigator?: unknown }).navigator;
  }
  // This runtime (real locks present): the plane stays enabled.
  if ((globalThis as { navigator?: { locks?: unknown } }).navigator?.locks) {
    assert.equal(defaultEnv().leaderElectionUnavailable, false);
  }
});

test("defaultEnv.openDatabase: versionchange closes the connection; a blocked open degrades instead of hanging ([9]/[13])", async () => {
  // A hand-rolled IDBFactory standing in for the browser's — Node has no IndexedDB.
  type Req = {
    result: unknown;
    onupgradeneeded: ((ev: unknown) => void) | null;
    onblocked: ((ev: unknown) => void) | null;
    onsuccess: ((ev: unknown) => void) | null;
    onerror: ((ev: unknown) => void) | null;
  };
  const mkDb = () => ({
    closed: false,
    onversionchange: null as ((ev: unknown) => void) | null,
    transaction: () => {
      throw new Error("unused");
    },
    createObjectStore: () => ({}),
    objectStoreNames: { contains: () => true },
    close(): void {
      this.closed = true;
    },
  });
  const requests: Req[] = [];
  const desc = Object.getOwnPropertyDescriptor(globalThis, "indexedDB");
  Object.defineProperty(globalThis, "indexedDB", {
    configurable: true,
    value: {
      open: () => {
        const req: Req = { result: mkDb(), onupgradeneeded: null, onblocked: null, onsuccess: null, onerror: null };
        requests.push(req);
        return req;
      },
      deleteDatabase: () => ({ onsuccess: null, onerror: null }),
    },
  });
  try {
    const env = defaultEnv();

    // (1) A clean open wires `versionchange` → close, so a sibling tab's deleteDatabase (logout)
    // is never blocked by this connection forever.
    const opened = env.openDatabase("x");
    requests[0].onsuccess?.({});
    const db = await opened;
    assert.ok(db, "opened");
    const conn1 = requests[0].result as ReturnType<typeof mkDb>;
    assert.equal(typeof conn1.onversionchange, "function", "versionchange handler installed");
    conn1.onversionchange?.({});
    assert.equal(conn1.closed, true, "the connection releases so the delete can proceed");

    // (2) A BLOCKED open (parked behind a pending delete) degrades to broadcast-only after the
    // bounded wait — client construction must never hang — and a late success is closed, not leaked.
    const blocked = env.openDatabase("y");
    requests[1].onblocked?.({});
    const t0 = Date.now();
    assert.equal(await blocked, null, "degraded to null instead of hanging");
    assert.ok(Date.now() - t0 >= 1500, "…after the bounded blocked-open wait");
    requests[1].onsuccess?.({}); // the blocker cleared later
    assert.equal((requests[1].result as ReturnType<typeof mkDb>).closed, true, "the late connection was released");
  } finally {
    if (desc) Object.defineProperty(globalThis, "indexedDB", desc);
    else delete (globalThis as { indexedDB?: unknown }).indexedDB;
  }
});

test("non-finite pk cells get distinct keys — ±Infinity rows survive a reload separately ([22])", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await settle();
  await t1.store.writeLocal((tx) => {
    tx.add("draft", { id: Infinity, text: "hi" });
    tx.add("draft", { id: -Infinity, text: "lo" });
  });
  await settle();
  assert.equal(hub.idbRows().size, 2, "two engine-distinct rows → two IDB records (was one '[null]' key)");
  t1.layer.close();

  const t2 = makeClientTab(hub);
  await t2.layer.ready;
  assert.deepEqual(
    t2.drafts.data.map((r) => r.text),
    ["lo", "hi"],
    "both restored",
  );
  t2.layer.close();
});

test("rowsEqual (shared): NaN cells equal themselves; -0 equals 0 ([23]/[27])", () => {
  assert.equal(rowsEqual([NaN], [NaN]), true);
  assert.equal(rowsEqual([-0], [0]), true);
  assert.equal(rowsEqual([1, "a"], [1, "a"]), true);
  assert.equal(rowsEqual([1], [2]), false);
  assert.equal(rowsEqual([1], [1, 2]), false);
});

test("a mid-boot write beats the snapshot (mirror-known pkKeys are skipped, §4.6 step 4)", async () => {
  const hub = new Hub();
  const t1 = makeClientTab(hub);
  await t1.layer.ready;
  await t1.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "stale" }));
  await settle();
  t1.layer.close();

  const t2 = makeClientTab(hub);
  // Written in the attach→restore gap: newer than anything the snapshot holds for this pk.
  await t2.store.writeLocal((tx) => tx.add("draft", { id: 1, text: "fresh" }));
  await t2.layer.ready;
  await settle();
  assert.deepEqual(t2.drafts.data, [{ id: 1, text: "fresh" }], "this session's write won");
  assert.deepEqual(hub.idbRows().get(`draft\0${JSON.stringify([1])}`), [1, "fresh"], "and was persisted");
  t2.layer.close();
});
