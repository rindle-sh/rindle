// @rindle/server — a Node WebSocket server that serves the @rindle/remote protocol from the
// SQLite-backed @rindle/replica engine. It composes existing pieces (WASM-CLIENT-DESIGN.md §2.5):
//   - a ReplicaBackend (one replica db; its clean hello/snapshot/batch ChangeEvent stream),
//   - a Publisher per subscription (epoch/seq/schema_fp framing), and
//   - ws routing (many queries multiplexed per connection, tagged by the client's queryId).
//
// One replica is shared by all connections, so a mutation from any client updates every
// subscribed query; each derived change is framed and routed to its subscriber. A re-subscribe
// (the client's gap recovery) tears the old query down and re-registers it under a NEW epoch.

import { WebSocket, WebSocketServer } from "ws";
import { NativeReplicaDb, ReplicaBackend } from "@rindle/replica";
import { Publisher } from "@rindle/remote";
import type { ClientMsg, ServerMsg } from "@rindle/remote";
import type { ColsMap, Schema } from "@rindle/client";
import { tableSpec } from "@rindle/client";
import { resolveNamedQuery } from "./queries.ts";
import type { RunQuery, ServerQueries } from "./queries.ts";

// The OPTIMISTIC ws route (OPTIMISTIC-WRITES-DESIGN.md §8): cv-stamped normalized frames,
// the named-mutator upstream, and per-connection progress frames under the poke rule.
export { createOptimisticServer } from "./optimistic.ts";
export type {
  LeaseResolver,
  LmidAdvance,
  OptimisticConn,
  OptimisticControl,
  OptimisticServerHandle,
  RowChangeTxnRequest,
  ServerMutator,
  ServerRegistry,
  SqlTxnRequest,
  SqlTxnResult,
} from "./optimistic.ts";

// The daemon wire contract's single canonical implementation is the Rust `rindled` follower
// (rindle-server's network front). The former in-process JS daemon (`createRindleDaemon`) has been
// retired; the contract lives in `test/daemon-conformance.mjs`, run against `rindled` in
// `test/daemon-rust.e2e.mjs`.
export { defineServerQueries } from "./queries.ts";
export type { RunQuery, RunQueryInput, ServerQueries, ServerQuery, ServerQueryResult } from "./queries.ts";

export interface ReplicaServer {
  /** The bound port (resolved when `port: 0` / omitted). */
  readonly port: number;
  /** Stop accepting connections and close the server. */
  close(): Promise<void>;
}

/** A live subscription's routing state, keyed by the server-assigned query id. */
interface Route {
  ws: WebSocket;
  clientQid: number;
  epoch: number;
  publisher: Publisher | null;
}

function send(ws: WebSocket, msg: ServerMsg): void {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

/** Start a replica-backed server. Registers the schema's tables in a fresh replica, then
 *  serves subscriptions + mutations over ws. Resolves once listening. */
export function createReplicaServer<S extends ColsMap>(opts: {
  schema: Schema<S>;
  queries?: ServerQueries<unknown>;
  runQuery?: RunQuery<unknown, WebSocket>;
  port?: number;
}): Promise<ReplicaServer> {
  const backend = new ReplicaBackend(opts.schema);
  const routes = new Map<number, Route>(); // serverQid → route
  let nextServerQid = 1;

  // The single event sink: frame every query's clean ChangeEvent stream and route it.
  backend.onEvent((serverQid, ev) => {
    const r = routes.get(serverQid);
    if (!r) return;
    if (ev.type === "hello") {
      r.publisher = new Publisher(r.epoch, ev.schema);
      send(r.ws, { t: "hello", queryId: r.clientQid, hello: r.publisher.hello() });
    } else if (ev.type === "snapshot") {
      if (r.publisher) send(r.ws, { t: "batch", queryId: r.clientQid, batch: r.publisher.snapshot(ev.adds) });
    } else {
      const batch = r.publisher?.commit(ev.events);
      if (batch) send(r.ws, { t: "batch", queryId: r.clientQid, batch });
    }
  });

  const wss = new WebSocketServer({ port: opts.port ?? 0 });

  wss.on("connection", (ws) => {
    const conn = new Map<number, { serverQid: number; epoch: number }>(); // clientQid → ...

    const dropQuery = (serverQid: number) => {
      backend.unregisterQuery(serverQid);
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
        // A napi/AST throw (unknown table, malformed mutation) must NOT crash the process — a
        // synchronous throw in a ws listener is an uncaughtException that takes down every
        // connection. Isolate it to THIS connection (#12).
        const queryId = (msg as { queryId?: number }).queryId;
        send(ws, { t: "error", queryId, message: String((err as Error)?.message ?? err) });
      }
    });

    const handleMessage = (msg: ClientMsg): void => {
      if (msg.t === "subscribe") {
        if (!("name" in msg) || typeof msg.name !== "string") {
          send(ws, { t: "queryError", queryId: msg.queryId, message: "subscribe requires a query name" });
          return;
        }
        // A re-subscribe (gap recovery) tears down the prior registration and bumps the epoch
        // so the client rejects any in-flight stale-epoch batch.
        const prev = conn.get(msg.queryId);
        let epoch = 1;
        if (prev) {
          dropQuery(prev.serverQid);
          epoch = prev.epoch + 1;
        }
        const serverQid = nextServerQid++;
        let ast;
        try {
          ast = resolveNamedQuery(opts, msg.name, msg.args, ws);
        } catch (e) {
          conn.delete(msg.queryId);
          send(ws, { t: "queryError", queryId: msg.queryId, message: (e as Error).message });
          return;
        }
        conn.set(msg.queryId, { serverQid, epoch });
        routes.set(serverQid, { ws, clientQid: msg.queryId, epoch, publisher: null });
        backend.registerQuery(serverQid, ast); // → onEvent hello + snapshot synchronously
      } else if (msg.t === "unsubscribe") {
        const prev = conn.get(msg.queryId);
        if (prev) {
          dropQuery(prev.serverQid);
          conn.delete(msg.queryId);
        }
      } else if (msg.t === "mutate") {
        void backend.mutate(msg.mutations); // → onEvent batches for affected queries → routed
      }
    };

    ws.on("close", () => {
      for (const { serverQid } of conn.values()) dropQuery(serverQid);
      conn.clear();
    });
  });

  return new Promise((resolve) => {
    wss.on("listening", () => {
      const addr = wss.address();
      const port = typeof addr === "object" && addr !== null ? addr.port : (opts.port ?? 0);
      resolve({
        port,
        close: () => new Promise<void>((res) => wss.close(() => res())),
      });
    });
  });
}

// The native `Db` surface the normalized server uses (the normalized API added in Slice 5).
interface NativeNormDb {
  registerTable(name: string, columns: string[], primaryKey: number[], columnTypes: string[]): void;
  queryNormalized(
    queryId: number,
    astJson: string,
    epoch: number,
  ): { queryId: number; hello: unknown; snapshot: unknown };
  commitNormalized(mutations: unknown): Array<{ queryId: number; batch: unknown }>;
  destroyQuery(queryId: number): void;
}

/** A NORMALIZED routing state, keyed by the server-assigned query id. */
interface NormRoute {
  ws: WebSocket;
  clientQid: number;
}

/**
 * Start a **normalized** replica-backed server (NORMALIZED-CHANGES-DESIGN.md §6) — the
 * local-first sibling of {@link createReplicaServer}. Each subscription gets the native
 * engine's per-query NORMALIZED footprint stream (`NormalizeFold` + `NormalizedPublisher`,
 * already epoch/seq/fp-stamped), relayed verbatim over ws as `nhello`/`nbatch`. One replica is
 * shared by all connections; a re-subscribe (the client's gap recovery) re-registers the query
 * under a NEW epoch so stale-epoch frames are rejected. (A single server is one mode — the
 * design's construction default; a mixed flat+normalized server would need a unified commit.)
 */
export function createNormalizedServer<S extends ColsMap>(opts: {
  schema: Schema<S>;
  queries?: ServerQueries<unknown>;
  runQuery?: RunQuery<unknown, WebSocket>;
  port?: number;
}): Promise<ReplicaServer> {
  const db = new NativeReplicaDb() as unknown as NativeNormDb;
  for (const name of Object.keys(opts.schema.tables)) {
    const meta = opts.schema.tables[name];
    const { columns, primaryKey } = tableSpec(meta);
    const cols = meta.columns as unknown as Record<string, { type: string }>;
    db.registerTable(name, columns, primaryKey, columns.map((c) => cols[c].type));
  }

  const routes = new Map<number, NormRoute>(); // serverQid → route
  let nextServerQid = 1;

  const wss = new WebSocketServer({ port: opts.port ?? 0 });

  wss.on("connection", (ws) => {
    const conn = new Map<number, { serverQid: number; epoch: number }>(); // clientQid → ...

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
        // A napi/AST throw (unknown table, malformed mutation) must NOT crash the process — a
        // synchronous throw in a ws listener is an uncaughtException that takes down every
        // connection. Isolate it to THIS connection (#12).
        const queryId = (msg as { queryId?: number }).queryId;
        send(ws, { t: "error", queryId, message: String((err as Error)?.message ?? err) });
      }
    });

    const handleMessage = (msg: ClientMsg): void => {
      if (msg.t === "subscribe") {
        if (!("name" in msg) || typeof msg.name !== "string") {
          send(ws, { t: "queryError", queryId: msg.queryId, message: "subscribe requires a query name" });
          return;
        }
        // A re-subscribe (gap recovery) tears down the prior registration and bumps the epoch.
        const prev = conn.get(msg.queryId);
        let epoch = 1;
        if (prev) {
          dropQuery(prev.serverQid);
          epoch = prev.epoch + 1;
        }
        const serverQid = nextServerQid++;
        let ast;
        try {
          ast = resolveNamedQuery(opts, msg.name, msg.args, ws);
        } catch (e) {
          conn.delete(msg.queryId);
          send(ws, { t: "queryError", queryId: msg.queryId, message: (e as Error).message });
          return;
        }
        conn.set(msg.queryId, { serverQid, epoch });
        routes.set(serverQid, { ws, clientQid: msg.queryId });
        const r = db.queryNormalized(serverQid, JSON.stringify(ast), epoch);
        send(ws, { t: "nhello", queryId: msg.queryId, hello: r.hello as never });
        send(ws, { t: "nbatch", queryId: msg.queryId, batch: r.snapshot as never });
      } else if (msg.t === "unsubscribe") {
        const prev = conn.get(msg.queryId);
        if (prev) {
          dropQuery(prev.serverQid);
          conn.delete(msg.queryId);
        }
      } else if (msg.t === "mutate") {
        // One txn fans to every normalized query; route each affected query's batch.
        for (const b of db.commitNormalized(msg.mutations)) {
          const route = routes.get(b.queryId);
          if (route) send(route.ws, { t: "nbatch", queryId: route.clientQid, batch: b.batch as never });
        }
      }
    };

    ws.on("close", () => {
      for (const { serverQid } of conn.values()) dropQuery(serverQid);
      conn.clear();
    });
  });

  return new Promise((resolve) => {
    wss.on("listening", () => {
      const addr = wss.address();
      const port = typeof addr === "object" && addr !== null ? addr.port : (opts.port ?? 0);
      resolve({ port, close: () => new Promise<void>((res) => wss.close(() => res())) });
    });
  });
}
