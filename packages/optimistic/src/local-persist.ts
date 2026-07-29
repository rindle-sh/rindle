// Local-table persistence (207-LOCAL-TABLE-PERSISTENCE-DESIGN.md): durable, cross-tab
// local-only tables via IndexedDB + a Web-Locks leader.
//
// The whole layer lives OUTSIDE the backend, over exactly two seams 201/207 carved:
//   - `backend.onLocalWrite(observer)` — every committed `writeLocal` batch (the write-through tap);
//   - `backend.applyLocalReplica(muts)` — the application path for restore + commits (no observer,
//     so the echo guard is structural; still funnels the engine's M2 locality guard, P8).
//
// Shape (§4): every tab requests one exclusive Web Lock — the holder is the leader, the browser's
// queue is the whole election (§4.1). The leader is the sole IDB writer and the commit sequencer;
// it persists a batch, THEN broadcasts a `commit` (P1/P2). Followers apply their own writes
// immediately, forward row-state ops to the leader, and hold them unacked until the matching
// commit (P4). All replication is idempotent full-ROW STATE (`put(pk,row)`/`tombstone(pk)`)
// applied through a per-tab MIRROR (a JS Map per local table) that diffs incoming state against
// known state and emits the correct engine add/edit/remove (§4.4, P3) — resend, replay, and
// snapshot/stream overlap are all no-ops by construction.
//
// Browser globals are reached via structural typing (this package has no DOM lib — precedent
// `client.ts:154-158`); the same structural seam (`PersistEnv`) is what the tests inject a fake
// IDB/channel/locks environment through.

import { localSchemaHash, persistedLocalTableNames, rowsEqual, tableSpec } from "@rindle/client";
import type { ColsMap, Mutation, Schema, WireValue } from "@rindle/client";

import type { OptimisticBackend } from "./backend.ts";

// ---------------------------------------------------------------------------------------------
// Public option/handle surface (§5.2)
// ---------------------------------------------------------------------------------------------

export interface PersistLocalOptions {
  /** The storage identity (§3.2): one IDB database per (origin, user). NOT the mutator principal —
   *  this is fixed for the client's lifetime; an anonymous mode passes its own sentinel (`"anon"`). */
  user: string;
  /** Call `navigator.storage.persist()` to resist eviction (§3.2). Default false — it can prompt. */
  requestPersistentStorage?: boolean;
  /** IDB/channel failures surface here (P9 — durability degrades, the write path never throws).
   *  Default `console.error`. */
  onError?: (e: Error) => void;
  /** Test seam: a fake IDB/locks/channel environment. Defaults to the browser globals. */
  env?: PersistEnv;
}

/** The attached layer's handle. `createRindleClient` awaits {@link ready} before returning (§5.2). */
export interface LocalPersistence {
  /** Resolves when the initial restore (§4.6 steps 1–5) has completed and the layer is live. */
  readonly ready: Promise<void>;
  /** Best-effort final persist/forward (the `pagehide` hook, §9): re-posts unacked ops (any
   *  role), then a leader drains its persist queue and retries P9-degraded batches. */
  flush(): Promise<void>;
  /** Release leadership (or abandon the queued lock request), close the channel + IDB (P10).
   *  A leader DRAINS its queued persist steps first (they carry already-committed writes) and
   *  releases the lock only after the last one lands; a follower re-posts its unacked ops. */
  close(): void;
  /** Introspection for tests/devtools: current role. */
  role(): "leader" | "follower";
}

/** Delete a user's local-persistence database — the sanctioned LOGOUT hook (§3.2). Never called
 *  implicitly; the old database otherwise remains on disk (fast re-login, and a privacy decision
 *  the app owns). Close this tab's live client for `user` first; a SIBLING tab's connection is
 *  released automatically (it closes on `versionchange` and degrades to broadcast-only), so a
 *  multi-tab logout completes instead of parking behind the other tab forever. */
export function deleteLocalPersistence(user: string, env?: PersistEnv): Promise<void> {
  return (env ?? defaultEnv()).deleteDatabase(dbName(user));
}

/** Attach the persistence layer to a backend (standalone form — `createRindleClient` calls this
 *  for you). The observer is wired SYNCHRONOUSLY, but the mirror starts empty: attach before the
 *  first `writeLocal` (§5.2, a v1 constraint `createRindleClient` satisfies by construction).
 *  Await `handle.ready` before first render if the app wants restored rows at first paint. */
export function attachLocalPersistence<S extends ColsMap>(
  backend: OptimisticBackend<S>,
  schema: Schema<S>,
  opts: PersistLocalOptions,
): LocalPersistence {
  return new LocalPersistLayer(backend, schema, opts);
}

// ---------------------------------------------------------------------------------------------
// The environment seam — the browser surface, structurally typed (real by default, faked in tests)
// ---------------------------------------------------------------------------------------------

/** The narrow storage surface the layer needs (§3.1): two fixed stores, `rows` + `meta`. Each
 *  method is one atomic IDB transaction; a resolved promise means the txn COMPLETED — the P1
 *  ("persist-then-broadcast") anchor. */
export interface PersistDb {
  getMeta(): Promise<PersistMeta | undefined>;
  putMeta(meta: PersistMeta): Promise<void>;
  getAllRows(): Promise<StoredRow[]>;
  /** Apply one commit batch atomically: `row === null` deletes, else puts. */
  putBatch(batch: RowState[]): Promise<void>;
  /** The P7 gate: clear `rows` and write `meta` in ONE transaction (never a half state). */
  reset(meta: PersistMeta): Promise<void>;
  /** Delete specific records (the leader's stale-table sweep, §3.1). */
  deleteRows(keys: Array<{ table: string; pkKey: string }>): Promise<void>;
  close(): void;
}

export interface PersistChannel {
  post(msg: unknown): void;
  onMessage(handler: (msg: unknown) => void): void;
  close(): void;
}

export interface PersistEnv {
  /** Open (creating if needed) the database. Resolves `null` when storage is unavailable —
   *  the layer degrades to broadcast-only (§3.2). */
  openDatabase(name: string): Promise<PersistDb | null>;
  deleteDatabase(name: string): Promise<void>;
  /** Open the tab-coherence channel; `null` when BroadcastChannel is unavailable (single-tab). */
  createChannel(name: string): PersistChannel | null;
  /** Queue for the exclusive leadership lock (§4.1): `onAcquired`'s promise HOLDS the lock until
   *  it resolves; `signal` aborts a still-queued request. When Web Locks are unavailable, grant
   *  immediately ONLY if the runtime is provably single-context (no channel) — see
   *  {@link leaderElectionUnavailable}. */
  requestLock(name: string, signal: AbortSignal, onAcquired: () => Promise<void>): void;
  /** The runtime has tabs (a channel) but NO exclusive lock (Firefox <96, Safari 15.1–15.3,
   *  Node): every context would self-promote into concurrent leaders over one database — P2
   *  violated, permanent divergence. When set, the layer runs INERT: local tables still work,
   *  session-scoped (the 201 baseline), with no persistence and no cross-tab replication. */
  leaderElectionUnavailable?: boolean;
  /** `navigator.storage.persist()` (§3.2), best-effort. */
  requestPersistentStorage?(): void;
}

export interface PersistMeta {
  schemaHash: string;
  epoch: number;
}

export interface StoredRow {
  table: string;
  pkKey: string;
  row: WireValue[];
}

/** The replication unit everywhere (§4.3): idempotent full-row state. `row: null` = tombstone
 *  (live protocol only — removes DELETE the IDB record; there are no persisted tombstones). */
export interface RowState {
  table: string;
  pkKey: string;
  row: WireValue[] | null;
}

// The three messages (§4.3), one BroadcastChannel per (origin, user, localSchemaHash). `p` on a
// commit = "this batch is durably in IDB" (false ⇒ the P9 degrade fired): every tab tracks the
// un-persisted keys so a later promotion REPAIRS them from the mirror instead of misreading the
// IDB gap as a remove.
type PersistMsg =
  | { t: "op"; origin: string; seq: number; batch: RowState[] }
  | { t: "commit"; epoch: number; lseq: number; origin: string; seq: number; batch: RowState[]; p: boolean }
  | { t: "leader"; epoch: number };

function dbName(user: string): string {
  return `rindle-local:${user}`;
}

/** The channel is partitioned by schema hash (the lock + database stay per-user): a mixed-version
 *  peer mid-deploy must never replicate old-shape rows into a new-schema engine — the width guard
 *  cannot catch a same-width reshape (a column rename or retype), so cross-version traffic is made
 *  structurally impossible instead. The old-version leader keeps the shared lock until it closes;
 *  new-version tabs run local-first meanwhile and adopt their unacked ops at promotion (§4.5). */
function channelName(user: string, schemaHash: string): string {
  return `${dbName(user)}::${schemaHash}`;
}

// ---------------------------------------------------------------------------------------------
// The layer
// ---------------------------------------------------------------------------------------------

interface LocalTableInfo {
  /** pk column indices, in schema pk order. */
  pk: number[];
  /** Full positional row width — the shape guard for replicated rows (a wrong-width row must not
   *  reach the wasm engine, which builds `panic = "abort"`). */
  width: number;
}

class LocalPersistLayer<S extends ColsMap> implements LocalPersistence {
  readonly ready: Promise<void>;

  private readonly backend: OptimisticBackend<S>;
  private readonly onError: (e: Error) => void;
  private readonly env: PersistEnv;
  /** Per PERSISTED local table (`local: true` — a `local: "session"` table stays outside the
   *  plane, §5.4): pk extraction + width (from the same schema meta `tableSpec` reads, §5.3). */
  private readonly tables = new Map<string, LocalTableInfo>();
  private readonly schemaHash: string;

  /** The §4.4 mirror: per PERSISTED local table, pkKey → engine row — the JS twin of the local
   *  source. A `null` value is a PRE-RESTORE TOMBSTONE (§4.6 step 4): a remove issued before the
   *  snapshot landed must not be resurrected by it (`has()` covers both shapes); swept once live. */
  private readonly mirror = new Map<string, Map<string, WireValue[] | null>>();
  /** `local: "session"` table names (§5.4) — inbound states naming one are dropped, never
   *  funneled to the engine (M2 admits local tables, so the isolation guard lives here). */
  private readonly sessionOnly = new Set<string>();

  /** This layer instance's identity on the channel (NOT the clientID — two clients in one tab are
   *  two "tabs" here, §9). */
  private readonly origin: string;
  private channel: PersistChannel | null = null;
  private db: PersistDb | null = null;

  // --- role / election state (§4.1–§4.2) ---
  private leader = false;
  /** Leader-only: announcement broadcast; incoming `op`s are processed. Before this (mid-promotion)
   *  ops are dropped — the sender re-sends on the `leader` announcement (§4.5). */
  private leaderLive = false;
  private releaseLock: (() => void) | null = null;
  private readonly lockAbort = new AbortController();
  private epoch = 0;
  /** Highest `leader`/`commit` epoch seen; commits below it are discarded (P6). */
  private highestEpoch = 0;
  private lseq = 0;
  /** Leader-only: the serialized persist→broadcast chain (P1/P2 — one writer, one order). */
  private persistChain: Promise<void> = Promise.resolve();

  // --- follower state (P4) ---
  private opSeq = 0;
  /** Ops applied locally + forwarded, held until the matching `commit` acks them (§4.5). */
  private readonly unacked = new Map<number, RowState[]>();
  /** Per-pk hold-back (§4.2): `table\0pkKey` → the seq of this tab's LATEST uncommitted write to
   *  that pk. While set, any other state for the pk (our own earlier echo, or a foreign write the
   *  leader sequenced before ours) is stale relative to our tail — applying it would visibly
   *  rewind the row and then snap it forward again. Cleared when the write's own commit applies. */
  private readonly pendingByPk = new Map<string, number>();
  /** P9 bookkeeping: `table\0pkKey`s whose LAST commit was broadcast with `p: false` (the persist
   *  failed or there is no IDB) — IDB does not reflect the coherent state for them. A leader
   *  retries them from the mirror on later chain steps; a promotion REPAIRS them before diffing,
   *  so the IDB gap is never misread as a persisted-but-unannounced remove. */
  private readonly notInIdb = new Set<string>();

  // --- boot state (§4.6) ---
  private live = false;
  private readonly bootBuffer: PersistMsg[] = [];
  /** Boot's meta verdict — the FALLBACK for the P7 decision when promotion's authoritative
   *  re-read (under the lock) fails: anything but a clean boot `match` then fails CLOSED
   *  (reset), because stamping the new hash over unverified rows would legitimize old-shape
   *  data forever. `error` = boot couldn't read meta (nothing was restored). */
  private bootMeta: "match" | "mismatch" | "error" = "error";
  /** Resolves once the §4.6 restore has run. A field initializer (not a `start()` return) so it
   *  exists BEFORE the lock request — an immediately-granted lock (solo mode / a free queue) must
   *  still park its promotion behind the restore. */
  private restoreDone!: () => void;
  private readonly restored = new Promise<void>((r) => {
    this.restoreDone = r;
  });
  private closed = false;
  /** {@link PersistEnv.leaderElectionUnavailable}: the layer is attached but INERT. */
  private readonly disabled: boolean;

  constructor(backend: OptimisticBackend<S>, schema: Schema<S>, opts: PersistLocalOptions) {
    this.backend = backend;
    this.env = opts.env ?? defaultEnv();
    this.onError = opts.onError ?? ((e) => console.error("[rindle] local persistence:", e));
    this.schemaHash = localSchemaHash(schema);
    this.origin = mintOrigin();
    for (const name of persistedLocalTableNames(schema)) {
      const spec = tableSpec(schema.tables[name]);
      this.tables.set(name, { pk: spec.primaryKey, width: spec.columns.length });
      this.mirror.set(name, new Map());
    }
    // `local: "session"` tables (§5.4): local for every 201 rule, but OUTSIDE this plane — their
    // writes are never forwarded/persisted, and an inbound state naming one (a mixed-version
    // peer) must be dropped HERE: M2 would happily admit it (it IS local), which would break the
    // per-tab isolation being ephemeral promises.
    for (const name of Object.keys(schema.tables)) {
      if (schema.tables[name].local === "session") this.sessionOnly.add(name);
    }

    // The write-through tap (§5.1): committed origin writes → row state → mirror, then routed by
    // role. Wired synchronously so no write can slip past the layer (the mirror starts empty).
    backend.onLocalWrite((muts) => this.onOriginWrite(muts));

    if (this.env.leaderElectionUnavailable) {
      // Tabs without an exclusive lock would all self-promote: concurrent leaders over one
      // database, P2 violated, permanent divergence. Run inert instead — local tables keep
      // working, session-scoped (the 201 baseline).
      this.disabled = true;
      console.warn(
        "[rindle] Web Locks unavailable — local-table persistence disabled (local tables are session-scoped).",
      );
      this.ready = Promise.resolve();
      this.restoreDone();
      return;
    }
    this.disabled = false;

    if (opts.requestPersistentStorage) this.env.requestPersistentStorage?.();

    // §4.6 attach order: (1) channel first — start buffering; (2) queue for the lock; (3–5) open
    // IDB, snapshot, replay (in start()).
    this.channel = this.env.createChannel(channelName(opts.user, this.schemaHash));
    this.channel?.onMessage((msg) => this.onMessage(msg as PersistMsg));
    this.env.requestLock(`rindle-local-leader:${opts.user}`, this.lockAbort.signal, () => this.onLockAcquired());

    this.ready = this.start(opts.user);
    void this.ready.then(this.restoreDone);
  }

  role(): "leader" | "follower" {
    return this.leader ? "leader" : "follower";
  }

  // -------------------------------------------------------------------------------------------
  // Boot: subscribe → snapshot → replay (§4.6)
  // -------------------------------------------------------------------------------------------

  private async start(user: string): Promise<void> {
    try {
      this.db = await this.env.openDatabase(dbName(user));
    } catch (e) {
      this.db = null;
      this.onError(asError(e));
    }
    if (this.closed) {
      // close() raced the open (it ran with db still null): release the fresh handle here or it
      // holds the database open forever, blocking every later deleteLocalPersistence.
      this.db?.close();
      this.db = null;
      return;
    }
    if (!this.db) {
      console.warn("[rindle] IndexedDB unavailable — local tables are session-scoped (broadcast-only).");
    }

    let snapshot: StoredRow[] = [];
    if (this.db) {
      try {
        const meta = await this.db.getMeta();
        if (meta?.schemaHash === this.schemaHash) {
          this.bootMeta = "match";
          this.epoch = meta.epoch;
          this.highestEpoch = Math.max(this.highestEpoch, meta.epoch);
          snapshot = await this.db.getAllRows();
        } else {
          // P7: mismatch (or missing meta) ⇒ start empty. The on-disk CLEAR is deferred to
          // promotion (only the lock holder writes IDB — P2); until then we simply load nothing.
          this.bootMeta = "mismatch";
          this.epoch = meta?.epoch ?? 0;
        }
      } catch (e) {
        this.bootMeta = "error"; // nothing restored; promotion must fail CLOSED on this store
        this.onError(asError(e));
        snapshot = [];
      }
    }

    if (this.closed) {
      this.db?.close(); // same close()-race exit as above — never leak the connection
      this.db = null;
      return;
    }
    try {
      this.applySnapshot(snapshot);
    } catch (e) {
      this.onError(asError(e)); // degraded restore, never a broken client construction (P9)
    }

    // Replay the buffered stream (gap-free by P1: anything broadcast pre-subscribe is in the
    // snapshot; anything after is in the buffer; the overlap is a mirror no-op, P3).
    this.live = true;
    for (const m of this.mirror.values()) {
      for (const [k, v] of m) if (v === null) m.delete(k); // sweep the pre-restore tombstones
    }
    const buffered = this.bootBuffer.splice(0);
    for (const msg of buffered) this.dispatch(msg);
  }

  /** §4.6 step 4: apply the IDB snapshot through the mirror, one engine batch per table (§5.3).
   *  Skips any pkKey the mirror already knows — a row written this session is newer than the
   *  snapshot, and a pre-restore remove left a `null` tombstone, so `has()` covers both. Records
   *  whose table is not a current local table are dropped — table-set schema evolution is handled
   *  by data, not DDL (§3.1); the leader's promotion sweep deletes them from disk. */
  private applySnapshot(snapshot: StoredRow[]): void {
    const byTable = new Map<string, RowState[]>();
    for (const rec of snapshot) {
      const info = this.tables.get(rec.table);
      if (!info) continue; // stale table (dropped from the schema) — leader sweeps it at promotion
      if (!validRow(rec.row, info.width)) {
        this.onError(new Error(`local persistence: dropped a wrong-width row for "${rec.table}" (P7 shape guard)`));
        continue;
      }
      if (this.mirror.get(rec.table)!.has(rec.pkKey)) continue; // this session already wrote (or removed) it — newer
      let group = byTable.get(rec.table);
      if (!group) byTable.set(rec.table, (group = []));
      group.push({ table: rec.table, pkKey: rec.pkKey, row: rec.row });
    }
    for (const states of byTable.values()) this.applyStates(states);
  }

  // -------------------------------------------------------------------------------------------
  // Origin writes: the tap (§4.2 roles)
  // -------------------------------------------------------------------------------------------

  /** A committed `writeLocal` batch from THIS tab: derive row state, fold it into the mirror
   *  synchronously (§4.4 — the engine already holds it), then route by role. Never throws (P9). */
  private onOriginWrite(muts: Mutation[]): void {
    if (this.disabled || this.closed) return;
    try {
      const states = this.statesFromMutations(muts);
      if (!states.length) return;
      const seq = ++this.opSeq;
      for (const s of states) {
        const m = this.mirror.get(s.table)!;
        if (s.row === null) {
          // Pre-live, a remove leaves a TOMBSTONE (`null`) so the §4.6 snapshot skip covers it;
          // once live the snapshot can't resurrect anything and plain deletion suffices.
          if (this.live) m.delete(s.pkKey);
          else m.set(s.pkKey, null);
        } else {
          m.set(s.pkKey, s.row);
        }
        this.pendingByPk.set(`${s.table}\0${s.pkKey}`, seq); // §4.2 hold-back until this commits
      }
      if (this.leader && this.leaderLive) {
        this.sequence(this.origin, seq, states);
      } else {
        // Follower (or leaderless / mid-boot): buffer unacked + forward. A leaderless op just
        // sits; the next `leader` announcement triggers the re-send (§4.5).
        this.unacked.set(seq, states);
        this.channel?.post({ t: "op", origin: this.origin, seq, batch: states } satisfies PersistMsg);
      }
    } catch (e) {
      this.onError(asError(e));
    }
  }

  /** Mutation batch → idempotent row state (§4.3): add/edit → put(new) (a pk-changing edit becomes
   *  tombstone(old) + put(new)); remove → tombstone. Later entries for one pk supersede earlier
   *  ones by array order (applied in order everywhere). */
  private statesFromMutations(muts: Mutation[]): RowState[] {
    const out: RowState[] = [];
    for (const m of muts) {
      const info = this.tables.get(m.table);
      if (!info) continue; // a `local: "session"` table (§5.4) — this tab's business only
      if (m.op === "add") {
        out.push({ table: m.table, pkKey: pkKeyOf(m.row, info.pk), row: m.row });
      } else if (m.op === "remove") {
        out.push({ table: m.table, pkKey: pkKeyOf(m.row, info.pk), row: null });
      } else {
        const oldKey = pkKeyOf(m.old, info.pk);
        const newKey = pkKeyOf(m.new, info.pk);
        if (oldKey !== newKey) out.push({ table: m.table, pkKey: oldKey, row: null });
        out.push({ table: m.table, pkKey: newKey, row: m.new });
      }
    }
    return out;
  }

  // -------------------------------------------------------------------------------------------
  // The mirror applier (§4.4, P3) — the ONLY path replicated state takes into the engine
  // -------------------------------------------------------------------------------------------

  /** Apply row states through the mirror: diff against known state, emit the correct engine
   *  delta, commit as ONE `applyLocalReplica` batch, and update the mirror at the engine's
   *  post-commit anchor. Idempotent: matching state is a no-op. `staged` overlays the mirror
   *  WITHIN the batch, so two states for one pk compose (add + edit), never collide.
   *
   *  `commit` names the commit being applied (absent for restore/promotion folds). It drives the
   *  §4.2 per-pk hold-back: while THIS tab has a newer uncommitted write to a pk, every other
   *  state for it — the echo of our own earlier write, or a foreign write the leader sequenced
   *  before ours — is stale relative to our tail; applying it would visibly rewind the row and
   *  snap it forward again (one full persist round-trip per keystroke on the draft-typing path).
   *  Skipped states skip engine AND mirror, which therefore stay in lockstep; the hold clears
   *  when the pending write's own commit arrives, and convergence is untouched (a foreign write
   *  sequenced AFTER ours applies normally once the hold is gone). */
  private applyStates(states: RowState[], commit?: { origin: string; seq: number }): void {
    const muts: Mutation[] = [];
    const mirrorOps: Array<() => void> = [];
    const staged = new Map<string, WireValue[] | null>(); // this batch's writes-so-far, by table\0pkKey
    for (const s of states) {
      const m = this.mirror.get(s.table);
      if (!m) {
        // A `local: "session"` table is OUTSIDE the plane (§5.4): drop silently — same posture
        // as a stale-table snapshot record (a mixed-version peer mid-deploy is normal, §3.1).
        if (this.sessionOnly.has(s.table)) continue;
        if (s.row !== null) {
          // Not a local table. Do NOT pre-filter: funnel it so `WasmBackend.writeLocal`'s M2
          // guard rejects it loudly (P8) — the batch dies whole, the mirror is untouched.
          muts.push({ op: "add", table: s.table, row: s.row });
        }
        continue;
      }
      const info = this.tables.get(s.table)!;
      const key = `${s.table}\0${s.pkKey}`;
      const pendingSeq = this.pendingByPk.get(key);
      if (pendingSeq !== undefined) {
        if (commit !== undefined && commit.origin === this.origin && commit.seq >= pendingSeq) {
          this.pendingByPk.delete(key); // our latest write to this pk is the one committing
        } else {
          continue; // §4.2 hold-back: a newer own write is still uncommitted
        }
      }
      const cur = staged.has(key) ? staged.get(key)! : (m.get(s.pkKey) ?? null);
      if (s.row === null) {
        if (cur) {
          muts.push({ op: "remove", table: s.table, row: cur });
          mirrorOps.push(() => m.delete(s.pkKey));
          staged.set(key, null);
        }
      } else if (!validRow(s.row, info.width)) {
        // Shape guard: a malformed replicated row must not reach the engine (wasm builds
        // panic=abort) — same test as the snapshot/promotion ingress points, deliberately shared.
        this.onError(new Error(`local persistence: dropped a malformed replicated row for "${s.table}"`));
      } else if (!cur) {
        const row = s.row;
        muts.push({ op: "add", table: s.table, row });
        mirrorOps.push(() => m.set(s.pkKey, row));
        staged.set(key, row);
      } else if (!rowsEqual(cur, s.row)) {
        const row = s.row;
        muts.push({ op: "edit", table: s.table, old: cur, new: row });
        mirrorOps.push(() => m.set(s.pkKey, row));
        staged.set(key, row);
      }
    }
    // The mirror updates run at the engine's post-commit anchor — after `tx.commit()`, BEFORE
    // subscriber delivery. A pre-commit rejection (M2) skips them with the engine untouched
    // (loud, P8); a subscriber throwing during delivery re-raises AFTER them — either way
    // mirror == engine. (Running them pre-delivery also means a subscriber that synchronously
    // writeLocal's a row in this batch lands its fresher mirror entry AFTER ours — never under.)
    if (muts.length) {
      this.backend.applyLocalReplica(muts, () => {
        for (const f of mirrorOps) f();
      });
    }
  }

  // -------------------------------------------------------------------------------------------
  // The channel (§4.3)
  // -------------------------------------------------------------------------------------------

  private onMessage(msg: PersistMsg): void {
    if (this.closed || !msg || typeof msg !== "object") return;
    if (!this.live) {
      this.bootBuffer.push(msg); // §4.6 step 1: buffer until the snapshot is in
      return;
    }
    this.dispatch(msg);
  }

  private dispatch(msg: PersistMsg): void {
    try {
      if (msg.t === "op") {
        // Only a LIVE leader sequences ops; mid-promotion (lock held, not announced) they are
        // dropped — the sender re-sends on the `leader` announcement (§4.5).
        if (this.leader && this.leaderLive) this.sequence(msg.origin, msg.seq, msg.batch);
      } else if (msg.t === "commit") {
        if (msg.epoch < this.highestEpoch) return; // stale epoch (P6)
        this.highestEpoch = Math.max(this.highestEpoch, msg.epoch);
        this.noteDurability(msg.batch, msg.p !== false); // P9 bookkeeping (promotion repairs it)
        // Everyone applies, origin included (§4.2/P3) — modulo the per-pk hold-back, which skips
        // states superseded by this tab's own uncommitted tail (see applyStates).
        this.applyStates(msg.batch, { origin: msg.origin, seq: msg.seq });
        if (msg.origin === this.origin) this.unacked.delete(msg.seq); // the ack (P4)
      } else if (msg.t === "leader") {
        this.highestEpoch = Math.max(this.highestEpoch, msg.epoch);
        // P4: a new leader means our unacked ops may never have been persisted — re-send them
        // all, in seq order; mirror application makes any duplicate a no-op (P3).
        this.resendUnacked();
      }
    } catch (e) {
      this.onError(asError(e));
    }
  }

  // -------------------------------------------------------------------------------------------
  // Leadership (§4.1, §4.5)
  // -------------------------------------------------------------------------------------------

  /** The lock callback: the returned promise HOLDS the lock until close() (§4.1). Promotion runs
   *  after our own restore completes; ops arriving meanwhile are dropped and re-sent on the
   *  announcement (§4.5). */
  private onLockAcquired(): Promise<void> {
    if (this.closed) {
      // close() raced the grant (`abort()` no-ops once granted, and `releaseLock` was still
      // null when close ran): resolve immediately, or the exclusive lock is held until the tab
      // dies and every other tab for this user queues leaderless forever.
      return Promise.resolve();
    }
    const held = new Promise<void>((resolve) => {
      this.releaseLock = resolve;
    });
    void this.promote();
    return held;
  }

  private async promote(): Promise<void> {
    try {
      await this.restored;
      if (this.closed) return;
      this.leader = true;

      const pendingReset = await this.resetPending();
      // P6: monotonic epochs — above both the persisted high-water and any announcement heard.
      this.epoch = Math.max(this.epoch, this.highestEpoch) + 1;
      this.highestEpoch = this.epoch;

      if (this.db) {
        if (pendingReset) {
          // P7, deferred from boot (P2 — only the leader writes): clear + new hash, one txn.
          await this.db.reset({ schemaHash: this.schemaHash, epoch: this.epoch });
          if (this.closed) return;
          this.notInIdb.clear(); // the store was just cleared; the seed below re-persists it all
          // Adopt our follower-era ops FIRST (their per-pk holds clear in original order), then
          // persist + announce the mirror itself: IDB was just cleared, so the "diff vs IDB"
          // below is vacuous — this session's live rows are all the data there is.
          this.adoptUnacked();
          const seed = this.mirrorStates();
          if (seed.length) this.sequence(this.origin, ++this.opSeq, seed);
        } else {
          await this.db.putMeta({ schemaHash: this.schemaHash, epoch: this.epoch });
          if (this.closed) return;
          await this.promotionDiff();
        }
      }
      if (this.closed) return;

      // Adopt our OWN unacked ops (we were a follower): sequence + persist them ourselves, in
      // seq order. Anything the old leader did persist re-persists identically (idempotent).
      this.adoptUnacked();

      // Announce LAST (§4.5 step 3): followers now re-send their unacked ops to us.
      this.leaderLive = true;
      this.channel?.post({ t: "leader", epoch: this.epoch } satisfies PersistMsg);
    } catch (e) {
      if (this.closed) return;
      this.onError(asError(e));
      // Storage failed mid-promotion: stay leader (the lock is ours) in broadcast-only degrade —
      // and STILL adopt our own unacked ops: no other tab can ever re-send them for us, and
      // sequence() never throws (its persist failure is the P9 catch), so coherence survives
      // even though this durability attempt didn't.
      this.adoptUnacked();
      this.leaderLive = true;
      this.channel?.post({ t: "leader", epoch: this.epoch } satisfies PersistMsg);
    }
  }

  /** Adopt our follower-era unacked ops as leader: sequence + persist them ourselves, in seq
   *  order (Map insertion order — seq is assigned at insert). Idempotent (the map is cleared). */
  private adoptUnacked(): void {
    for (const [seq, batch] of this.unacked) this.sequence(this.origin, seq, batch);
    this.unacked.clear();
  }

  /** The P7 decision, made HERE — under the lock, where the read is authoritative (single
   *  writer: no one can move meta between this read and our reset/putMeta). Boot's verdict alone
   *  would be stale: an earlier same-version leader may have validly initialized or reset the
   *  store since (its unannounced tail must then be diffed in, not wiped). If this read FAILS,
   *  fall back to boot's verdict and fail CLOSED on anything but a clean boot match — the gate
   *  must never stamp the new hash over unverified rows (that legitimizes old-shape data
   *  forever; a wrongly-kept store self-heals via the promotion diff, a wrongly-stamped one
   *  never does). */
  private async resetPending(): Promise<boolean> {
    if (!this.db) return false;
    try {
      const meta = await this.db.getMeta();
      if (meta?.schemaHash !== this.schemaHash) return true;
      // Matches (boot may have seen `error`/a pre-initialization store): nothing to clear. If
      // boot restored nothing, the promotion diff below folds every persisted row into the live
      // engine (the same P5 path that heals a dead leader's gap). Re-adopt the persisted epoch
      // high-water for P6 monotonicity.
      this.epoch = Math.max(this.epoch, meta.epoch);
      return false;
    } catch (e) {
      this.onError(asError(e));
      return this.bootMeta !== "match";
    }
  }

  /** P5: on promotion, IDB — not any tab's memory — is the durable source of truth. Re-read it and
   *  diff against the mirror; every difference (excluding keys our OWN unacked ops touch, which are
   *  newer by definition and re-sequenced right after) is a persisted-but-unannounced write from
   *  the dead leader's P1 gap: fold it into the mirror/engine and rebroadcast it under the new
   *  epoch. Records for tables no longer in the schema are swept from disk here (§3.1).
   *
   *  The P9 REPAIR runs first: for keys whose last commit never landed in IDB (`p: false`), the
   *  mirror — not IDB — is the coherent truth; without the repair, the IDB gap would read as a
   *  persisted-but-unannounced REMOVE below, and a one-off storage error on the old leader would
   *  escalate into this promotion actively deleting committed, live rows from every tab. */
  private async promotionDiff(): Promise<void> {
    if (!this.db) return;
    await this.repairDurability();
    const persisted = await this.db.getAllRows();
    if (this.closed) return;

    const ownUnacked = new Set<string>();
    for (const batch of this.unacked.values()) {
      for (const s of batch) ownUnacked.add(`${s.table}\0${s.pkKey}`);
    }

    const diff: RowState[] = [];
    const stale: Array<{ table: string; pkKey: string }> = [];
    const seen = new Set<string>();
    for (const rec of persisted) {
      const info = this.tables.get(rec.table);
      if (!info) {
        stale.push({ table: rec.table, pkKey: rec.pkKey });
        continue;
      }
      const key = `${rec.table}\0${rec.pkKey}`;
      seen.add(key);
      if (ownUnacked.has(key)) continue;
      if (!validRow(rec.row, info.width)) {
        this.onError(new Error(`local persistence: dropped a malformed row for "${rec.table}" (promotion diff)`));
        continue;
      }
      const cur = this.mirror.get(rec.table)!.get(rec.pkKey);
      if (!cur || !rowsEqual(cur, rec.row)) diff.push({ table: rec.table, pkKey: rec.pkKey, row: rec.row });
    }
    // Mirror rows ABSENT from IDB (and not ours in flight): a persisted-but-unannounced REMOVE.
    // (Sound because the repair above already re-persisted every key IDB was known to be missing.)
    for (const [table, m] of this.mirror) {
      for (const [pkKey, row] of m) {
        if (row === null) continue; // a pre-restore tombstone (never reaches promotion, but cheap)
        const key = `${table}\0${pkKey}`;
        if (!seen.has(key) && !ownUnacked.has(key)) diff.push({ table, pkKey, row: null });
      }
    }

    if (diff.length) {
      this.applyStates(diff); // fold into our own engine/mirror first (§4.5 step 2)
      // Already persisted (it CAME from IDB) — broadcast straight, no re-persist needed.
      this.channel?.post({
        t: "commit",
        epoch: this.epoch,
        lseq: ++this.lseq,
        origin: this.origin,
        seq: 0,
        batch: diff,
        p: true,
      } satisfies PersistMsg);
    }
    if (stale.length) await this.db.deleteRows(stale);
  }

  /** P9 repair: make IDB match the mirror for every key flagged un-persisted (`notInIdb`). The
   *  mirror holds the coherent truth for them — a present row re-puts, an absent one deletes.
   *  Idempotent; throws propagate to the caller (a failed repair keeps the flags for next time). */
  private async repairDurability(): Promise<void> {
    if (!this.db || this.notInIdb.size === 0) return;
    const repair: RowState[] = [];
    for (const key of this.notInIdb) {
      const i = key.indexOf("\0");
      const table = key.slice(0, i);
      const pkKey = key.slice(i + 1);
      const m = this.mirror.get(table);
      if (!m) {
        this.notInIdb.delete(key); // the table left the plane — its records get swept anyway
        continue;
      }
      repair.push({ table, pkKey, row: m.get(pkKey) ?? null });
    }
    if (repair.length) await this.db.putBatch(repair);
    for (const s of repair) this.notInIdb.delete(`${s.table}\0${s.pkKey}`);
  }

  /** P9 bookkeeping, updated from every commit (our own chain and the channel): `persisted: false`
   *  marks the batch's keys as missing from IDB until a later successful persist covers them. */
  private noteDurability(batch: RowState[], persisted: boolean): void {
    for (const s of batch) {
      const key = `${s.table}\0${s.pkKey}`;
      if (persisted) this.notInIdb.delete(key);
      else this.notInIdb.add(key);
    }
  }

  /** The full mirror as row state (the post-reset seed). Pre-restore tombstones are excluded —
   *  they mark "removed before the snapshot landed", not durable rows. */
  private mirrorStates(): RowState[] {
    const out: RowState[] = [];
    for (const [table, m] of this.mirror) {
      for (const [pkKey, row] of m) if (row !== null) out.push({ table, pkKey, row });
    }
    return out;
  }

  /** The leader's sequencer (§4.2): assign `(epoch, lseq)`, persist (ONE IDB txn), then broadcast
   *  the `commit` (P1) and apply it locally through the mirror (a no-op for our own writes, which
   *  the per-pk hold-back also shields from stale echoes). The chain serializes batches so IDB is
   *  a fold of commits in `(epoch, lseq)` order (P2).
   *
   *  Deliberately NOT gated on `closed`: a queued step carries an already-committed write —
   *  cancelling it on an orderly close would silently drop durable data and its broadcast.
   *  close() drains this chain and only then releases the lock and the handles. */
  private sequence(origin: string, seq: number, batch: RowState[]): void {
    const epoch = this.epoch;
    const lseq = ++this.lseq;
    this.persistChain = this.persistChain.then(async () => {
      let persisted = false;
      try {
        if (this.db) {
          await this.repairDurability(); // P9 backlog first, so IDB folds in commit order
          await this.db.putBatch(batch);
          persisted = true;
        }
      } catch (e) {
        this.onError(asError(e)); // durability degrades; coherence continues (P9)
      }
      this.noteDurability(batch, persisted);
      this.channel?.post({ t: "commit", epoch, lseq, origin, seq, batch, p: persisted } satisfies PersistMsg);
      try {
        this.applyStates(batch, { origin, seq });
        if (origin === this.origin) this.unacked.delete(seq);
      } catch (e) {
        this.onError(asError(e));
      }
    });
  }

  // -------------------------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------------------------

  async flush(): Promise<void> {
    // Best-effort final forward (§9), regardless of role: a mid-promotion leader still holds
    // un-adopted follower-era ops, and for a plain follower this is the last chance for a live
    // leader to persist them. A live leader's buffer is empty — the re-post is then a no-op.
    this.resendUnacked();
    if (this.leader) {
      await this.persistChain;
      try {
        await this.repairDurability(); // last-chance durability for P9-degraded batches
      } catch (e) {
        this.onError(asError(e));
      }
    }
  }

  /** Re-post every unacked op, in seq order (Map insertion order — seq is assigned at insert). */
  private resendUnacked(): void {
    for (const [seq, batch] of this.unacked) {
      this.channel?.post({ t: "op", origin: this.origin, seq, batch } satisfies PersistMsg);
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.lockAbort.abort(); // abandon a still-queued lock request
    // A follower's final forward (the pagehide flush's guarantee, now on the orderly path too):
    // re-post unacked ops while the channel is still open, so a live leader can persist them.
    if (!this.leader) this.resendUnacked();
    const finish = () => {
      this.releaseLock?.(); // release held leadership (P10)
      this.channel?.close();
      this.channel = null;
      this.db?.close();
      this.db = null;
    };
    if (this.leader) {
      // Drain, then release: queued persist steps carry already-COMMITTED writes (engine + every
      // tab's view have them) — cancelling would silently lose them from IDB and from every
      // other tab; and handing the lock over before the last putBatch lands would let the next
      // leader diff against a torn store. The chain never rejects (every step catches).
      void this.persistChain
        .then(() => this.repairDurability())
        .catch((e) => this.onError(asError(e)))
        .then(finish);
    } else {
      finish();
    }
  }
}

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/** `pkKey` (§3.1): JSON of the pk cells in schema pk order — a STRING deliberately (IDB keys
 *  admit neither `null` nor `boolean`, both legal rindle pk cells). Non-finite numbers get a
 *  NUL-prefixed tag: `JSON.stringify` folds NaN/±Infinity into `null`, which would collapse
 *  engine-distinct rows onto one storage/replication key (a remove of either would tombstone
 *  both). The NUL byte keeps a legitimate string cell from ever colliding with the tag. */
function pkKeyOf(row: WireValue[], pk: number[]): string {
  return JSON.stringify(
    pk.map((i) => {
      const v = row[i] ?? null;
      return typeof v === "number" && !Number.isFinite(v) ? `\u0000n:${String(v)}` : v;
    }),
  );
}

/** The replicated-row shape guard, shared by ALL three ingress points (snapshot restore, live
 *  commits, the promotion diff) so they can never drift: a malformed row must not reach the
 *  engine (wasm builds panic=abort). */
function validRow(row: unknown, width: number): row is WireValue[] {
  return Array.isArray(row) && row.length === width;
}

function asError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}

function mintOrigin(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `${Math.random().toString(36).slice(2)}-${Math.random().toString(36).slice(2)}`;
}

// ---------------------------------------------------------------------------------------------
// The default (real-browser) environment — structural typing over the globals (no DOM lib here)
// ---------------------------------------------------------------------------------------------

// The slivers of the IDB/locks/BroadcastChannel surfaces this module touches.
interface IdbRequestLike<T = unknown> {
  result: T;
  error?: unknown;
  onsuccess: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}
interface IdbOpenRequestLike extends IdbRequestLike<IdbDatabaseLike> {
  onupgradeneeded: ((ev: unknown) => void) | null;
  onblocked: ((ev: unknown) => void) | null;
}
interface IdbObjectStoreLike {
  put(value: unknown, key?: unknown): IdbRequestLike;
  delete(key: unknown): IdbRequestLike;
  get(key: unknown): IdbRequestLike;
  getAll(): IdbRequestLike<unknown[]>;
  clear(): IdbRequestLike;
}
interface IdbTransactionLike {
  objectStore(name: string): IdbObjectStoreLike;
  oncomplete: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onabort: ((ev: unknown) => void) | null;
  error?: unknown;
}
interface IdbDatabaseLike {
  transaction(stores: string[], mode: "readonly" | "readwrite"): IdbTransactionLike;
  createObjectStore(name: string, opts?: { keyPath?: string | string[] }): unknown;
  objectStoreNames: { contains(name: string): boolean };
  onversionchange: ((ev: unknown) => void) | null;
  close(): void;
}
interface IdbFactoryLike {
  open(name: string, version: number): IdbOpenRequestLike;
  deleteDatabase(name: string): IdbRequestLike;
}
interface BroadcastChannelLike {
  postMessage(msg: unknown): void;
  onmessage: ((ev: { data: unknown }) => void) | null;
  close(): void;
}
interface LockManagerLike {
  request(name: string, opts: { mode: "exclusive"; signal?: AbortSignal }, cb: () => Promise<void>): Promise<unknown>;
}

const ROWS = "rows";
const META = "meta";
const META_KEY = "meta";

/** Wrap one IDB transaction's completion as a promise (resolution == txn durably committed —
 *  the exact anchor P1 needs). */
function txnDone(txn: IdbTransactionLike): Promise<void> {
  return new Promise((resolve, reject) => {
    txn.oncomplete = () => resolve();
    txn.onerror = () => reject(asError(txn.error ?? new Error("IndexedDB transaction failed")));
    txn.onabort = () => reject(asError(txn.error ?? new Error("IndexedDB transaction aborted")));
  });
}

function reqDone<T>(req: IdbRequestLike<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(asError(req.error ?? new Error("IndexedDB request failed")));
  });
}

class IdbPersistDb implements PersistDb {
  private readonly db: IdbDatabaseLike;

  constructor(db: IdbDatabaseLike) {
    this.db = db;
  }

  async getMeta(): Promise<PersistMeta | undefined> {
    const txn = this.db.transaction([META], "readonly");
    const out = await reqDone(txn.objectStore(META).get(META_KEY));
    return out as PersistMeta | undefined;
  }

  putMeta(meta: PersistMeta): Promise<void> {
    const txn = this.db.transaction([META], "readwrite");
    txn.objectStore(META).put(meta, META_KEY);
    return txnDone(txn);
  }

  async getAllRows(): Promise<StoredRow[]> {
    const txn = this.db.transaction([ROWS], "readonly");
    const out = await reqDone(txn.objectStore(ROWS).getAll());
    return out as StoredRow[];
  }

  putBatch(batch: RowState[]): Promise<void> {
    const txn = this.db.transaction([ROWS], "readwrite");
    const store = txn.objectStore(ROWS);
    for (const s of batch) {
      if (s.row === null) store.delete([s.table, s.pkKey]);
      else store.put({ table: s.table, pkKey: s.pkKey, row: s.row } satisfies StoredRow);
    }
    return txnDone(txn);
  }

  reset(meta: PersistMeta): Promise<void> {
    const txn = this.db.transaction([ROWS, META], "readwrite");
    txn.objectStore(ROWS).clear();
    txn.objectStore(META).put(meta, META_KEY);
    return txnDone(txn);
  }

  deleteRows(keys: Array<{ table: string; pkKey: string }>): Promise<void> {
    const txn = this.db.transaction([ROWS], "readwrite");
    const store = txn.objectStore(ROWS);
    for (const k of keys) store.delete([k.table, k.pkKey]);
    return txnDone(txn);
  }

  close(): void {
    this.db.close();
  }
}

/** How long a BLOCKED IDB open may park before the layer degrades to broadcast-only: a pending
 *  `deleteDatabase` (a sibling tab's logout) queues every later open behind it, and an old-version
 *  connection with no `versionchange` handler can hold that queue indefinitely — client
 *  construction must never hang on it. */
const OPEN_BLOCKED_TIMEOUT_MS = 2000;

/** The real-browser environment: IndexedDB + BroadcastChannel + `navigator.locks`, each reached
 *  structurally and each degrading gracefully when absent (§3.2). */
export function defaultEnv(): PersistEnv {
  const g = globalThis as unknown as {
    indexedDB?: IdbFactoryLike;
    BroadcastChannel?: new (name: string) => BroadcastChannelLike;
    navigator?: { locks?: LockManagerLike; storage?: { persist?: () => Promise<boolean> } };
  };
  const locks = g.navigator?.locks;
  return {
    // Multi-context runtime with no exclusive lock (Firefox <96, Safari 15.1–15.3, Node): the
    // solo-mode fallback below would make EVERY context a leader — the layer must run inert.
    leaderElectionUnavailable: !locks && typeof g.BroadcastChannel === "function",
    openDatabase(name: string): Promise<PersistDb | null> {
      const idb = g.indexedDB;
      if (!idb) return Promise.resolve(null);
      return new Promise((resolve) => {
        let settled = false;
        const settle = (db: PersistDb | null) => {
          if (settled) return;
          settled = true;
          resolve(db);
        };
        let req: IdbOpenRequestLike;
        try {
          req = idb.open(name, 1);
        } catch {
          resolve(null); // some privacy modes throw synchronously — degrade (§3.2)
          return;
        }
        req.onupgradeneeded = () => {
          // Version 1 forever (§3.1): a fixed two-store shape, so a schema's table-set change
          // never needs an IDB version bump.
          const db = req.result;
          if (!db.objectStoreNames.contains(ROWS)) db.createObjectStore(ROWS, { keyPath: ["table", "pkKey"] });
          if (!db.objectStoreNames.contains(META)) db.createObjectStore(META);
        };
        req.onsuccess = () => {
          const db = req.result;
          // A sibling tab's deleteDatabase (logout) fires `versionchange` at every open
          // connection and then WAITS for them all to close: release ours so the delete (and
          // any open queued behind it) can proceed — this session degrades (P9), the logout
          // completes. Without this, one live tab wedges every other tab's logout forever.
          db.onversionchange = () => db.close();
          if (settled) {
            db.close(); // the blocked-open timeout already degraded us — don't hold a handle
            return;
          }
          settle(new IdbPersistDb(db));
        };
        req.onerror = () => settle(null); // unavailable → broadcast-only degrade (§3.2)
        req.onblocked = () => {
          // Parked behind a pending delete (or an old-version connection): degrade after a
          // bounded wait rather than hanging client construction — never forever.
          setTimeout(() => settle(null), OPEN_BLOCKED_TIMEOUT_MS);
        };
      });
    },
    deleteDatabase(name: string): Promise<void> {
      const idb = g.indexedDB;
      if (!idb) return Promise.resolve();
      return reqDone(idb.deleteDatabase(name)).then(() => undefined);
    },
    createChannel(name: string): PersistChannel | null {
      const BC = g.BroadcastChannel;
      if (!BC) return null;
      const ch = new BC(name);
      return {
        post: (msg) => {
          try {
            ch.postMessage(msg);
          } catch {
            /* a closed/failed channel loses coherence, never the write path (P9) */
          }
        },
        onMessage: (handler) => {
          ch.onmessage = (ev) => handler(ev.data);
        },
        close: () => ch.close(),
      };
    },
    requestLock(name: string, signal: AbortSignal, onAcquired: () => Promise<void>): void {
      if (!locks) {
        // No Web Locks AND no BroadcastChannel (`leaderElectionUnavailable` gates the layer off
        // otherwise): a provably single-context runtime — solo mode, safe by construction.
        void onAcquired();
        return;
      }
      void locks.request(name, { mode: "exclusive", signal }, onAcquired).catch(() => {
        /* an aborted queued request (close before acquisition) — expected */
      });
    },
    requestPersistentStorage(): void {
      void g.navigator?.storage?.persist?.()?.catch?.(() => {});
    },
  };
}
