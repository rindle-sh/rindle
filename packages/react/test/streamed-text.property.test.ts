// Property-based integration test for the CLIENT leg: the real `useStreamedText` hook rendered by
// real React, subscribed through a real `@rindle/api-server` stream plane over a real SQLite store
// (designs-implemented/LM-STREAM-CHECKPOINT-DESIGN.md §10.7). The wire between the tiers is just frames, so the
// two are coupled in-process; the transport emulates `EventSource` semantics faithfully — buffered
// delivery, connection drops that LOSE undelivered frames, and reconnection at the last id the page
// received (which, after a `durable` frame, sits BEHIND the chunk progress — TRAP 5's overlap).
//
// Each fc case generates a checkpoint policy and a schedule over: producer pushes and flushes, a
// mid-run close, frame deliveries, re-renders with a fresh (or deliberately lagging) durable text,
// and connection drops. Properties, checked after every op:
//
//   prefix     the rendered text is ALWAYS a prefix of the produced text — never duplicated
//              (an overlapping resume), never gapped, never another stream's;
//   monotone   the rendered text never gets shorter while the stream runs;
//   no churn   re-renders never reconnect — connections == 1 + effective drops (TRAP 1);
//   terminal   once the stream is sealed and the durable text catches up, the rendered text equals
//              the produced text exactly (contract S, as the component sees it).
//
// Deterministic without mock timers: the store is synchronous, `intervalMs` is set out of reach,
// and checkpoints are driven by the char threshold and explicit flushes — everything settles on the
// microtask queue, which the interpreter drains between ops.
//
// Run count: 40 by default; RINDLE_PBT_RUNS deepens the sweep (nightly runs 5k). On failure
// fast-check prints `{ seed, path }` and the shrunk counterexample; RINDLE_PBT_SEED replays a
// specific seed's sequence.

import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";

import fc from "fast-check";
import { JSDOM } from "jsdom";

const dom = new JSDOM('<!DOCTYPE html><html><body><div id="root"></div></body></html>');
const g = globalThis as Record<string, unknown>;
const win = dom.window as unknown as Record<string, unknown>;
const setGlobal = (k: string, v: unknown) => Object.defineProperty(g, k, { value: v, configurable: true, writable: true });
setGlobal("window", dom.window);
setGlobal("document", dom.window.document);
setGlobal("navigator", win.navigator);
for (const k of ["HTMLElement", "Element", "Node", "Text", "DocumentFragment", "Event", "customElements"]) {
  setGlobal(k, win[k]);
}
g.IS_REACT_ACT_ENVIRONMENT = true;

const { act, createElement: h } = await import("react");
const { createRoot } = await import("react-dom/client");
const { useStreamedText } = await import("../src/index.ts");
import type { StreamFrame, StreamTransport } from "../src/index.ts";

const {
  assembleDurableText,
  createRindleApiServer,
  frameResumePoint,
  sqliteDialect,
  streamChunkTableDdl,
} = await import("@rindle/api-server");
import type { MutationBackend, RindleApiServer, ServerSql, StreamTables } from "@rindle/api-server";

const RUNS = Number(process.env.RINDLE_PBT_RUNS ?? 40);
const STREAM = "m1";
const TABLES: StreamTables = { message: "message", chunks: "message_chunk" };

// --------------------------------------------------------------------------- a minimal real store
// (a fault-free sibling of `@rindle/api-server`'s test/stream-store.ts — the client leg's faults
// are connection drops and IVM lag, not store failures)

class MiniStore {
  readonly db = new DatabaseSync(":memory:");

  constructor() {
    this.db.exec(`CREATE TABLE message (
      id TEXT PRIMARY KEY, chatId TEXT NOT NULL, role TEXT NOT NULL,
      body TEXT NOT NULL DEFAULT '', seq INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending'
    )`);
    for (const ddl of streamChunkTableDdl(TABLES, sqliteDialect)) this.db.exec(ddl);
    this.db.prepare("INSERT INTO message (id, chatId, role) VALUES (?, ?, ?)").run(STREAM, "c1", "assistant");
  }

  /** What the app's IVM query would assemble: `body` plus the un-compacted chunk rows. */
  durable(): string {
    const row = this.db.prepare("SELECT body FROM message WHERE id = ?").get(STREAM) as never as { body: string };
    const chunks = this.db
      .prepare('SELECT seq, text FROM message_chunk WHERE "streamId" = ? ORDER BY seq')
      .all(STREAM) as never as Array<{ seq: number; text: string }>;
    return assembleDurableText(row, chunks);
  }

  get backend(): MutationBackend {
    const db = this.db;
    const sql: ServerSql = {
      async execute(s, params = []) {
        db.prepare(s).run(...(params as never[]));
      },
      async batch(statements) {
        db.exec("BEGIN");
        try {
          for (const s of statements) db.prepare(s.sql).run(...((s.params ?? []) as never[]));
          db.exec("COMMIT");
        } catch (e) {
          db.exec("ROLLBACK");
          throw e;
        }
      },
      async query<Row = Record<string, unknown>>(s: string, params: readonly unknown[] = []): Promise<Row[]> {
        return db.prepare(s).all(...(params as never[])) as Row[];
      },
    };
    return {
      dialect: sqliteDialect,
      outsideSql: sql,
      async runMutation() {
        throw new Error("no mutations here");
      },
      async reject() {
        return {};
      },
    };
  }
}

// --------------------------------------------------------------------------- the EventSource twin

/** A transport with `EventSource`'s exact semantics against the real server: frames buffer in the
 *  "socket" until the test delivers them (under `act`); a drop LOSES the undelivered buffer and
 *  reconnects at the last id the page RECEIVED — which is how a resumed replay comes to overlap
 *  the hook's accumulator (TRAP 5). */
class ServerTransport implements StreamTransport {
  connections = 0;
  private buffered: StreamFrame[] = [];
  private onFrame?: (f: StreamFrame) => void;
  private sub?: { close(): void };
  private lastId: number | undefined;
  private streamId = "";
  private terminalDelivered = false;
  private detached = true;
  private epoch = 0;
  private readonly api: RindleApiServer<string>;

  constructor(api: RindleApiServer<string>) {
    this.api = api;
  }

  subscribe(url: string, onFrame: (frame: StreamFrame) => void): () => void {
    const u = new URL(url, "http://x");
    this.streamId = u.searchParams.get("streamId") ?? "";
    this.onFrame = onFrame;
    this.detached = false;
    void this.attach(Number.parseInt(u.searchParams.get("from") ?? "0", 10));
    return () => {
      this.detached = true;
      this.sub?.close();
    };
  }

  private async attach(from: number): Promise<void> {
    const e = ++this.epoch;
    this.connections++;
    const sub = await this.api.subscribeStream({ user: "u1", streamId: this.streamId, from });
    if (this.detached || e !== this.epoch) {
      sub.close();
      return;
    }
    this.sub = sub;
    for await (const f of sub.frames) {
      if (this.detached || e !== this.epoch) break;
      this.buffered.push(f);
    }
  }

  /** Hand up to `k` buffered frames to the page. Call under `act`. */
  deliver(k: number): void {
    while (k-- > 0 && this.buffered.length > 0) {
      const f = this.buffered.shift()!;
      const id = frameResumePoint(f);
      if (id !== undefined) this.lastId = id;
      if (f.type === "end" || f.type === "stale" || f.type === "absent") this.terminalDelivered = true;
      if (!this.detached) this.onFrame?.(f);
    }
  }

  /** @returns whether a reconnect actually happened (never after terminal delivery or detach). */
  drop(): boolean {
    if (this.detached || this.terminalDelivered) return false;
    this.sub?.close();
    this.sub = undefined;
    this.buffered = [];
    void this.attach(this.lastId ?? 0);
    return true;
  }
}

// --------------------------------------------------------------------------- ops

type Op =
  | { t: "push"; n: number }
  | { t: "flush" }
  | { t: "deliver"; k: number }
  | { t: "renderFresh" }
  | { t: "renderSame" }
  | { t: "drop" }
  | { t: "close" };

const opArb: fc.Arbitrary<Op> = fc.oneof(
  { weight: 6, arbitrary: fc.integer({ min: 1, max: 24 }).map((n): Op => ({ t: "push", n })) },
  { weight: 2, arbitrary: fc.constant<Op>({ t: "flush" }) },
  { weight: 6, arbitrary: fc.integer({ min: 1, max: 30 }).map((k): Op => ({ t: "deliver", k })) },
  { weight: 3, arbitrary: fc.constant<Op>({ t: "renderFresh" }) },
  { weight: 1, arbitrary: fc.constant<Op>({ t: "renderSame" }) },
  { weight: 2, arbitrary: fc.constant<Op>({ t: "drop" }) },
  { weight: 1, arbitrary: fc.constant<Op>({ t: "close" }) },
);

async function drainMicro(): Promise<void> {
  for (let i = 0; i < 25; i++) await new Promise((r) => setImmediate(r));
}

const ALPHABET = "abcdefghijklmnopqrstuvwxyz";

async function runCase(chars: number, ops: Op[]): Promise<void> {
  const store = new MiniStore();
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: {
      checkpoint: { tables: TABLES },
      authorize: () => true,
      policy: { chars, intervalMs: 10_000 },
      onCheckpointError: () => {},
    },
  });
  const transport = new ServerTransport(api);
  const host = dom.window.document.getElementById("root")!;
  const root = createRoot(host);
  let rendered = "";

  function Probe(props: { durable: string; live: boolean }) {
    rendered = useStreamedText({ streamId: STREAM, durable: props.durable, live: props.live }, { transport });
    return h("p", null, rendered);
  }

  const render = async (durable: string, live: boolean): Promise<void> => {
    await act(async () => {
      root.render(h(Probe, { durable, live }));
    });
  };

  let produced = "";
  let closed = false;
  let lastRenderedDurable = "";
  let prevLen = 0;
  let expectedConnections = 1;

  try {
    const s = await api.openStream({ user: "u1", streamId: STREAM });
    await render("", true);
    await drainMicro();

    for (const op of ops) {
      switch (op.t) {
        case "push": {
          if (closed) break;
          const text = Array.from({ length: op.n }, (_, j) => ALPHABET[(produced.length + j) % 26]).join("");
          s.push(text);
          produced += text;
          break;
        }
        case "flush":
          if (!closed) await s.flush();
          break;
        case "deliver":
          await act(async () => {
            transport.deliver(op.k);
          });
          break;
        case "renderFresh":
          lastRenderedDurable = store.durable();
          await render(lastRenderedDurable, true);
          break;
        case "renderSame":
          // IVM lag: the query has not delivered the newest checkpoint; the render changes nothing.
          await render(lastRenderedDurable, true);
          break;
        case "drop":
          if (transport.drop()) expectedConnections++;
          break;
        case "close":
          if (!closed) {
            closed = true;
            await s.close();
          }
          break;
      }
      await drainMicro();

      // The three running properties.
      assert.ok(
        produced.startsWith(rendered),
        `rendered text must be a prefix of produced\n rendered: ${JSON.stringify(rendered)}\n produced: ${JSON.stringify(produced)}`,
      );
      assert.ok(rendered.length >= prevLen, "the rendered text never shrinks while the stream runs");
      prevLen = rendered.length;
      assert.equal(transport.connections, expectedConnections, "re-renders never reconnect (TRAP 1)");
    }

    // ---- quiescence: seal, deliver what remains, and let the durable plane catch up.
    if (!closed) await s.close();
    await drainMicro();
    await act(async () => {
      transport.deliver(Number.POSITIVE_INFINITY);
    });
    await render(store.durable(), false);
    assert.equal(rendered, produced, "S, as the component sees it: the final render IS the response");
    assert.equal(transport.connections, expectedConnections);
  } finally {
    await act(async () => {
      root.unmount();
    });
    api.close();
  }
}

// --------------------------------------------------------------------------- the property

test("useStreamedText: prefix, monotonicity, no reconnect churn, and S over generated schedules", async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.constantFrom(4, 16, 64),
      fc.array(opArb, { minLength: 1, maxLength: 30 }),
      async (chars, ops) => {
        await runCase(chars, ops);
      },
    ),
    {
      numRuns: RUNS,
      ...(process.env.RINDLE_PBT_SEED ? { seed: Number(process.env.RINDLE_PBT_SEED) } : {}),
    },
  );
});
