// The OPTIMISTIC ws route (OPTIMISTIC-WRITES-DESIGN.md §8 + CLUSTER-FOLD-IN-DESIGN.md WS6):
// `createOptimisticServer` serves the optimistic protocol — the normalized subscription
// stream with `cv`-stamped frames, plus the named-mutator upstream — from the **cluster-backed**
// @rindle/replica engine (`ClusterDb`). Queries derive in parallel across IVM worker threads
// and per-query batches / per-connection progress frames / faults are pushed **asynchronously**
// through `onEvent`; the server is a thin relay that routes each event to the right ws.
//
//   - **subscription**: `queryNormalized` returns the slim `hello` synchronously (schema-
//     derived); the seq-0 snapshot `nbatch` and its release `progress` arrive on `onEvent`.
//     The reserved `LMID_QUERY_NAME` subscribe resolves to the connection's own one-row
//     system query over `_rindle_client_mutations` (lmid-as-data) — identity comes from
//     the connection's `init`, never the args.
//   - **mutation apply** (§8.1/§8.2): per envelope — dup-skip below the stored lmid
//     (silent: the client already holds its lmid via the system query), a mid GAP throws
//     (impossible over an ordered transport ⇒ a protocol error), the registry mutator runs
//     in ONE transaction with the co-transactional lmid upsert; a throw (or unknown name)
//     rolls effects back and commits lmid-only — processed-as-no-op, NO rejection signal.
//     The lmid row IS the confirmation: it derives through the client's system query and
//     releases with the commit's own data.
//   - **eager delivery** (§8.5): every commit's cv-stamped batches stream to subscribers as
//     the drain folds them — the server keeps no output buffer.
//   - **the poke rule** (§8.4) + **per-worker `cv_min`** (§8): the drain decides which
//     connections get a progress frame and computes `cv_min` from worker positions, so a slow
//     worker holds back only clients with a query on it (the read-side isolation win). The
//     server just relays the frames it is handed — `data frames precede the progress frame`
//     per connection is guaranteed by the drain (KS §6).
//
// One replica is shared by all connections; a connection declares its stable clientID via
// `init`. Re-subscribes (gap recovery) re-register under a NEW epoch and the fresh snapshot's
// progress frame releases the client.

import type { Server as HttpServer } from "node:http";

import { WebSocket, WebSocketServer } from "ws";
import { NativeClusterDb } from "@rindle/replica";
import type { ClusterMutationTxn } from "@rindle/replica";
import type { NormalizedBatch, NormalizedHello } from "@rindle/remote";
import type { ClientMsg, ServerMsg } from "@rindle/remote";
import type { Ast, ColsMap, MutationEnvelope, ProgressFrame, Schema, WireValue } from "@rindle/client";
import { CLIENT_MUTATIONS_TABLE, LMID_QUERY_NAME, tableSpec } from "@rindle/client";

import type { ReplicaServer } from "./index.ts";
import { resolveNamedQuery } from "./queries.ts";
import type { RunQuery, ServerQueries } from "./queries.ts";

/** A server-side (authoritative) mutator: runs SQL against the mutation's open transaction
 *  (`exec`/`queryRows` — reads see its own writes, §4.1). A throw rejects the mutation. */
export type ServerMutator = (txn: ClusterMutationTxn, args: never) => void;

/** The server registry (§4.2) — the authoritative twin of the client's `ClientRegistry`:
 *  shared names (and possibly code), never the wire. */
export type ServerRegistry = Record<string, ServerMutator>;

/** A drain event, as marshaled by the native sink (`onEvent` payload). */
type DrainEvent =
  | { t: "batch"; conn: number; queryId: number; batch: NormalizedBatch }
  | { t: "progress"; conn: number; frame: ProgressFrame }
  | { t: "faulted"; conn: number; queryId: number; reason: string };

// The native `ClusterDb` surface this route uses (kept structural — the generated `.d.ts`
// types the JSON returns as `any`).
interface NativeOptimisticDb {
  registerTable(name: string, columns: string[], primaryKey: number[], columnTypes: string[]): void;
  enableClientMutations(): void;
  clientLmid(clientId: string): number;
  connect(conn: number): void;
  disconnect(conn: number): void;
  queryNormalized(
    conn: number,
    queryId: number,
    astJson: string,
    epoch: number,
  ): { hello: NormalizedHello };
  destroyQuery(queryId: number): void;
  commitNormalized(mutations: unknown): number;
  beginMutation(): ClusterMutationTxn;
  onEvent(cb: (err: unknown, ev: DrainEvent) => void): void;
  close(): void;
}

/** The reserved system query: the connection's own one-row slice of the lmid table.
 *  Identity comes from the connection's `init` — client-supplied args are ignored. */
function lmidQueryAst(clientID: string): Ast {
  return {
    table: CLIENT_MUTATIONS_TABLE,
    where: {
      type: "simple",
      op: "=",
      left: { type: "column", name: "client_id" },
      right: { type: "literal", value: clientID },
    },
    orderBy: [["client_id", "asc"]],
  };
}

/** One connection's state: its drain id, declared client identity, and subscriptions. */
export interface OptimisticConn {
  ws: WebSocket;
  id: number; // the drain-side connection id
  clientID: string;
  queries: Map<number, { serverQid: number; epoch: number }>; // clientQid → ...
}

/** Resolves an opaque query lease (minted by the API server through the private control
 *  plane) to its approved AST. Throw for an unknown/expired token. */
export interface LeaseResolver {
  resolve(leaseToken: string): Ast;
}

// ----- the private control surface (the daemon front's write/control plane) -----

export interface SqlTxnRequest {
  idempotencyKey?: string;
  clientID?: string;
  mid?: number;
  statements: Array<{ sql: string; params?: WireValue[] }>;
}

export interface LmidAdvance {
  clientID: string;
  lmid: number;
}

export interface SqlTxnResult {
  /** False when absorbed as an idempotent replay (`mid ≤ lmid`, or a seen idempotency key). */
  applied: boolean;
  /** The commit version (absent when deduped without a stored cv). */
  cv?: number;
  lmidAdvances?: LmidAdvance[];
}

export interface RowChangeTxnRequest {
  source: string;
  offset: string;
  changes: Array<
    | { table: string; op: "add"; row: WireValue[] }
    | { table: string; op: "remove"; old: WireValue[] }
    | { table: string; op: "edit"; old: WireValue[]; row: WireValue[] }
  >;
}

/** The engine-side twin of `@rindle/daemon-client`: what a daemon HTTP front calls. All
 *  methods are synchronous (the cluster commit returns `cv` synchronously; batches/progress
 *  flow asynchronously through the drain). */
export interface OptimisticControl {
  /** Apply one approved write txn with the client-mutation discipline (dup ⇒ absorbed,
   *  gap ⇒ throw, lmid co-transactional). A daemon-side SQL failure rolls back WITHOUT
   *  advancing lmid and throws — the API server decides retry vs. explicit rejection. */
  executeSqlTxn(txn: SqlTxnRequest): SqlTxnResult;
  /** Advance lmid past `mid` with NO effects (processed-as-no-op): the prediction snaps
   *  back on the lmid release. `reason` is server-side observability only. */
  rejectMutation(input: { clientID: string; mid: number; reason?: string }): SqlTxnResult;
  /** Apply a replication batch as one raw foreign write (no lmid). Replays of the same
   *  `{source, offset}` are absorbed. */
  applyRowChangeTxn(input: RowChangeTxnRequest): SqlTxnResult;
  clientLmid(clientID: string): number;
}

/** `createOptimisticServer`'s handle: the ws server plus the private control surface. */
export interface OptimisticServerHandle extends ReplicaServer {
  control: OptimisticControl;
}

function send(ws: WebSocket, msg: ServerMsg): void {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

/** Start an optimistic-protocol server: a fresh cluster-backed replica with the schema's
 *  tables + client mutations enabled, serving subscriptions, raw (foreign) writes, and
 *  named-mutator envelopes over ws. Resolves once listening (immediately when attaching to
 *  a caller-owned `server` — the caller manages listen/close on it). */
export function createOptimisticServer<S extends ColsMap>(opts: {
  schema: Schema<S>;
  registry: ServerRegistry;
  queries?: ServerQueries<unknown>;
  runQuery?: RunQuery<unknown, OptimisticConn>;
  port?: number;
  /** Attach the ws route to an existing HTTP server (the daemon front) instead of opening
   *  a port. `close()` then tears down the ws layer + engine but NOT the HTTP server. */
  server?: HttpServer;
  /** Accept `{t:"subscribe", leaseToken}` by resolving tokens through this (daemon mode). */
  leases?: LeaseResolver;
}): Promise<OptimisticServerHandle> {
  const db = new NativeClusterDb() as unknown as NativeOptimisticDb;
  for (const name of Object.keys(opts.schema.tables)) {
    const meta = opts.schema.tables[name];
    const { columns, primaryKey } = tableSpec(meta);
    const cols = meta.columns as unknown as Record<string, { type: string }>;
    db.registerTable(name, columns, primaryKey, columns.map((c) => cols[c].type));
  }
  db.enableClientMutations();

  // serverQid → route (for async batch/fault routing); connId → conn (for progress routing).
  const routes = new Map<number, { conn: OptimisticConn; clientQid: number }>();
  const conns = new Map<number, OptimisticConn>();
  let nextServerQid = 1;
  let nextConnId = 1;

  // The single async sink: route each drain event to the connection it belongs to. Data
  // frames (`batch`) precede the releasing `progress` frame per connection (the drain
  // enforces it), so the client's buffer is complete when its release point moves.
  db.onEvent((err, ev) => {
    if (err || !ev) return;
    if (ev.t === "batch") {
      const r = routes.get(ev.queryId);
      if (r) send(r.conn.ws, { t: "nbatch", queryId: r.clientQid, batch: ev.batch });
    } else if (ev.t === "progress") {
      const conn = conns.get(ev.conn);
      if (conn) send(conn.ws, { t: "progress", frame: ev.frame });
    } else if (ev.t === "faulted") {
      // Terminal: the query's pipeline was torn down on its worker. Tell the client to
      // re-subscribe (re-hydrate). Best-effort — valid data never faults.
      const r = routes.get(ev.queryId);
      if (r) {
        routes.delete(ev.queryId);
        send(r.conn.ws, { t: "queryError", queryId: r.clientQid, message: `query faulted (re-subscribe): ${ev.reason}` });
      }
    }
  });

  const applyMutation = (conn: OptimisticConn, env: MutationEnvelope): void => {
    if (!conn.clientID) conn.clientID = env.clientID; // belt-and-braces (init should precede)
    const stored = db.clientLmid(env.clientID);
    if (env.mid <= stored) {
      // Duplicate redelivery: already processed — silently skipped. The client needs no
      // re-confirm: its own lmid system query already delivered (or will deliver, at
      // connect-hydrate) a watermark ≥ this mid, which drops the pending copy.
      return;
    }
    if (env.mid !== stored + 1) {
      // A mid AHEAD of lmid+1 can't happen over an ordered transport — the client sends
      // contiguously. This is a protocol violation (a client bug or two writers on one
      // clientID), not a state to recover: throw (relayed as an error to this connection).
      throw new Error(
        `mutation gap for ${env.clientID}: expected mid ${stored + 1}, got ${env.mid}`,
      );
    }
    let txn = db.beginMutation();
    let failed = false;
    try {
      const mutator = opts.registry[env.name];
      if (!mutator) throw new Error(`unknown mutator: ${env.name}`);
      mutator(txn, env.args as never);
    } catch (err) {
      failed = true;
      // No-reject semantics: the failure is logged server-side only; the protocol
      // carries no signal (the mutation is processed-as-no-op below).
      console.error(`[rindle-server] mutator ${env.name} (mid ${env.mid}) failed:`, err);
    }
    if (failed) {
      // Effects roll back; lmid still advances in an lmid-only commit (§8.2) — the
      // durable record that the mid was processed. Its lmid row derives through the
      // client's own system query, and that release snaps the prediction back.
      txn.rollback();
      txn = db.beginMutation();
      txn.commitWithLmid(env.clientID, env.mid);
    } else {
      txn.commitWithLmid(env.clientID, env.mid);
    }
    // The commit's cv-stamped batches (data + the lmid row) + the touched connections'
    // progress frames flow asynchronously through `onEvent`; the server keeps no buffer.
  };

  // ----- the private control plane (what a daemon HTTP front calls; OptimisticControl) -----
  // This engine is memory-backed, so in-memory dedup records have the same durability as
  // the data they guard. The file-backed Rust daemon persists both.
  const seenIdempotencyKeys = new Map<string, number>();
  const sourceOffsets = new Map<string, string>();

  const runStatements = (txn: ClusterMutationTxn, statements: SqlTxnRequest["statements"]): void => {
    for (const stmt of statements) txn.exec(stmt.sql, stmt.params ?? []);
  };

  const midGap = (clientID: string, expected: number, got: number): Error =>
    new Error(`mutation id gap for ${clientID}: expected ${expected}, got ${got}`);

  const control: OptimisticControl = {
    executeSqlTxn(req) {
      if (req.clientID != null && req.mid != null) {
        const stored = db.clientLmid(req.clientID);
        if (req.mid <= stored) {
          return { applied: false, lmidAdvances: [{ clientID: req.clientID, lmid: stored }] };
        }
        if (req.mid !== stored + 1) throw midGap(req.clientID, stored + 1, req.mid);
        const txn = db.beginMutation();
        try {
          runStatements(txn, req.statements);
        } catch (err) {
          // An APPROVED write failing daemon-side is an ERROR, not processed-as-no-op:
          // no lmid advance, the API server decides retry vs. explicit rejection.
          txn.rollback();
          throw err;
        }
        const cv = txn.commitWithLmid(req.clientID, req.mid);
        return { applied: true, cv, lmidAdvances: [{ clientID: req.clientID, lmid: req.mid }] };
      }
      if (req.idempotencyKey !== undefined) {
        const seen = seenIdempotencyKeys.get(req.idempotencyKey);
        if (seen !== undefined) return { applied: false, cv: seen };
      }
      const txn = db.beginMutation();
      try {
        runStatements(txn, req.statements);
      } catch (err) {
        txn.rollback();
        throw err;
      }
      const cv = txn.commit();
      if (req.idempotencyKey !== undefined) seenIdempotencyKeys.set(req.idempotencyKey, cv);
      return { applied: true, cv };
    },
    rejectMutation({ clientID, mid, reason }) {
      const stored = db.clientLmid(clientID);
      if (mid <= stored) return { applied: false, lmidAdvances: [{ clientID, lmid: stored }] };
      if (mid !== stored + 1) throw midGap(clientID, stored + 1, mid);
      console.error(
        `[rindle-server] mutation ${mid} for ${clientID} rejected (lmid still advances): ${reason ?? "no reason given"}`,
      );
      const txn = db.beginMutation();
      const cv = txn.commitWithLmid(clientID, mid);
      return { applied: true, cv, lmidAdvances: [{ clientID, lmid: mid }] };
    },
    applyRowChangeTxn({ source, offset, changes }) {
      if (sourceOffsets.get(source) === offset) return { applied: false }; // replayed batch
      const mutations = changes.map((c) =>
        c.op === "add"
          ? { op: "add" as const, table: c.table, row: c.row }
          : c.op === "remove"
            ? { op: "remove" as const, table: c.table, row: c.old }
            : { op: "edit" as const, table: c.table, old: c.old, new: c.row },
      );
      const cv = db.commitNormalized(mutations);
      sourceOffsets.set(source, offset);
      return { applied: true, cv };
    },
    clientLmid: (clientID) => db.clientLmid(clientID),
  };

  const wss = opts.server
    ? new WebSocketServer({ server: opts.server })
    : new WebSocketServer({ port: opts.port ?? 0 });

  wss.on("connection", (ws) => {
    const conn: OptimisticConn = { ws, id: nextConnId++, clientID: "", queries: new Map() };
    conns.set(conn.id, conn);

    const dropQuery = (serverQid: number) => {
      db.destroyQuery(serverQid);
      routes.delete(serverQid);
    };

    ws.on("message", (data) => {
      let msg: ClientMsg;
      try {
        msg = JSON.parse(String(data)) as ClientMsg;
      } catch {
        return;
      }
      try {
        handleMessage(msg);
      } catch (err) {
        // A napi/AST/commit throw (unknown table, malformed mutation, capture/derive failure)
        // must NOT crash the process — a synchronous throw in a ws listener is an
        // uncaughtException that takes down every connection. Isolate it to THIS connection (#12).
        const queryId = (msg as { queryId?: number }).queryId;
        send(ws, { t: "error", queryId, message: String((err as Error)?.message ?? err) });
      }
    });

    const handleMessage = (msg: ClientMsg): void => {
      if (msg.t === "init") {
        conn.clientID = msg.clientID;
        // Register the connection's progress bookkeeping. No lmid seed: confirmation
        // state is data, served by the client's own lmid system query.
        db.connect(conn.id);
      } else if (msg.t === "subscribe") {
        const leaseToken = "leaseToken" in msg && typeof msg.leaseToken === "string" ? msg.leaseToken : undefined;
        const name = "name" in msg && typeof msg.name === "string" ? msg.name : undefined;
        const args = "args" in msg ? msg.args : undefined;
        if (leaseToken === undefined && name === undefined) {
          send(ws, { t: "queryError", queryId: msg.queryId, message: "subscribe requires a query name or lease token" });
          return;
        }
        // A re-subscribe (gap recovery) tears down the prior registration and bumps the epoch.
        const prev = conn.queries.get(msg.queryId);
        let epoch = 1;
        if (prev) {
          dropQuery(prev.serverQid);
          epoch = prev.epoch + 1;
        }
        const serverQid = nextServerQid++;
        let ast;
        try {
          if (leaseToken !== undefined) {
            // Daemon mode: the opaque capability the API server minted after auth +
            // named-query resolution. The browser never sends names or ASTs here.
            if (!opts.leases) throw new Error("lease subscriptions are not enabled on this server");
            ast = opts.leases.resolve(leaseToken);
          } else if (name === LMID_QUERY_NAME) {
            // The reserved system query: this connection's own lmid row. Identity is
            // the connection's declared clientID — args are ignored, so a client can
            // never subscribe another client's confirmation stream.
            if (!conn.clientID) throw new Error("subscribe lmid query before init");
            ast = lmidQueryAst(conn.clientID);
          } else {
            ast = resolveNamedQuery(opts, name as string, args, conn);
          }
        } catch (e) {
          conn.queries.delete(msg.queryId);
          send(ws, { t: "queryError", queryId: msg.queryId, message: (e as Error).message });
          return;
        }
        conn.queries.set(msg.queryId, { serverQid, epoch });
        routes.set(serverQid, { conn, clientQid: msg.queryId });
        // hello is synchronous (schema-derived); the seq-0 snapshot `nbatch` + its release
        // `progress` arrive on `onEvent` (after this tick), so nhello precedes them.
        const r = db.queryNormalized(conn.id, serverQid, JSON.stringify(ast), epoch);
        send(ws, { t: "nhello", queryId: msg.queryId, hello: r.hello });
      } else if (msg.t === "unsubscribe") {
        const prev = conn.queries.get(msg.queryId);
        if (prev) {
          dropQuery(prev.serverQid);
          conn.queries.delete(msg.queryId);
        }
      } else if (msg.t === "mutate") {
        // A raw foreign write (no mid: confirms nothing, advances no lmid — §8.2): fold it;
        // the affected queries' batches + touched connections' frames flow via `onEvent`.
        db.commitNormalized(msg.mutations);
      } else if (msg.t === "pushMutation") {
        applyMutation(conn, msg.envelope);
      }
    };

    ws.on("close", () => {
      for (const { serverQid } of conn.queries.values()) dropQuery(serverQid);
      conn.queries.clear();
      db.disconnect(conn.id);
      conns.delete(conn.id);
    });
  });

  const close = () =>
    new Promise<void>((res) =>
      wss.close(() => {
        // Release the native async-event callback so the Node process can exit (the
        // drain thread holds a ThreadsafeFunction ref otherwise).
        db.close();
        res();
      }),
    );

  if (opts.server) {
    // Attached mode: the caller owns the HTTP server's lifecycle (listen/port/close);
    // `close()` tears down the ws layer + engine only.
    return Promise.resolve({ port: 0, close, control });
  }

  return new Promise((resolve) => {
    wss.on("listening", () => {
      const addr = wss.address();
      const port = typeof addr === "object" && addr !== null ? addr.port : (opts.port ?? 0);
      resolve({ port, close, control });
    });
  });
}
