// LM stream relay (designs-implemented/LM-STREAM-RELAY-DESIGN.md) — the cross-process seam.
//
// Three layers, mirroring the design's test plan:
//   §4 conform  — the pure pass, one case per row of the violation table, no plane around it.
//   the plane   — attach ordering (authorize first, local stream wins), the absent/stale
//                 downgrades, the attach timeout, and producer isolation from a broken publish.
//   integration — an in-memory bus with a configurable floor, shared by TWO api-server instances
//                 over ONE store: a reader on process B sees process A's tokens.
//
// The load-bearing claim everywhere: a relay can be lossy, reordered, truncated, or down, and the
// worst legal outcome for a reader is an early `stale` — the durable plane already carries the
// truth, so nothing here is an error the app must handle.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  StreamRelayConform,
  createRindleApiServer,
  type RindleStreamOptions,
  type StreamFrame,
  type StreamRelay,
  type StreamRelayErrorInfo,
  type StreamSubscription,
} from "../src/index.ts";

import { Store, TABLES } from "./stream-store.ts";

// --------------------------------------------------------------------------- helpers

function relayServer(store: Store, over: Partial<RindleStreamOptions<string>>) {
  return createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: {
      checkpoint: { tables: TABLES },
      authorize: ({ user }) => user === "u1",
      policy: { chars: 16, intervalMs: 10_000, retries: 2 },
      onCheckpointError: () => {},
      ...over,
    },
  });
}

async function collect(sub: StreamSubscription): Promise<StreamFrame[]> {
  const out: StreamFrame[] = [];
  for await (const f of sub.frames) out.push(f);
  return out;
}

/** The client's splice, as `useStreamedText` performs it: cut at the frame's own offset, append. */
function spliced(joinPrefix: string, frames: StreamFrame[]): string {
  let acc = joinPrefix;
  for (const f of frames) {
    if (f.type !== "chunk") continue;
    assert.ok(f.from <= acc.length, `chunk at ${f.from} would leave a gap after ${acc.length}`);
    acc = acc.slice(0, f.from) + f.text;
  }
  return acc;
}

const RESPONSE = "The quick brown fox jumps over the lazy dog, and then keeps going for a while.";
function deltas(text: string): string[] {
  const out: string[] = [];
  let i = 0;
  for (let n = 1; i < text.length; n = (n % 7) + 1) {
    out.push(text.slice(i, i + n));
    i += n;
  }
  return out;
}

const chunk = (from: number, seq: number, text: string): StreamFrame => ({ type: "chunk", from, seq, text });
const open = (from: number, seq = from, durableSeq = from): StreamFrame => ({
  type: "open",
  streamId: "m1",
  from,
  seq,
  durableSeq,
  ended: false,
});
const end = (seq: number): StreamFrame => ({ type: "end", seq, status: "complete" });

function conformer(from = 0): { c: StreamRelayConform; violations: string[] } {
  const violations: string[] = [];
  return { c: new StreamRelayConform("m1", from, (r) => violations.push(r)), violations };
}

// --------------------------------------------------------------------------- §4: the conform pass

test("conform: a missing open is synthesized at the join offset (the broadcast normal case)", () => {
  const { c, violations } = conformer(0);
  const out = c.feed(chunk(0, 3, "abc"));
  assert.deepEqual(out, [
    { type: "open", streamId: "m1", from: 0, seq: 0, durableSeq: 0, ended: false },
    chunk(0, 3, "abc"),
  ]);
  assert.deepEqual(violations, []);
});

test("conform: the adapter's own open passes; a reconnect's second open is absorbed", () => {
  const { c } = conformer(4);
  assert.deepEqual(c.feed(open(4)), [open(4)]);
  assert.deepEqual(c.feed(open(0)), []);
  assert.deepEqual(c.feed(chunk(4, 6, "ef")), [chunk(4, 6, "ef")]);
});

test("conform: an open past the requested offset is the honest floor — stale, not a gap later", () => {
  const { c, violations } = conformer(100);
  const out = c.feed(open(150));
  assert.deepEqual(out, [{ type: "stale", floorSeq: 100, durableSeq: 0 }]);
  assert.equal(violations.length, 1);
  assert.deepEqual(c.feed(chunk(150, 152, "hi")), [], "terminated: nothing follows a terminal");
});

test("conform: an open below the requested offset re-bases the prefix (an over-replaying log)", () => {
  const { c } = conformer(10);
  assert.deepEqual(c.feed(open(6)), [open(6)]);
  assert.deepEqual(c.feed(chunk(6, 12, "abcdef")), [chunk(6, 12, "abcdef")]);
});

test("conform: a chunk that does not span exactly its offsets is stale", () => {
  const { c, violations } = conformer(0);
  assert.deepEqual(c.feed(chunk(0, 5, "ab")), [{ type: "stale", floorSeq: 0, durableSeq: 0 }]);
  assert.equal(violations.length, 1);
});

test("conform: a fractional offset is stale, not NaN math", () => {
  const { c } = conformer(0);
  assert.deepEqual(c.feed({ type: "chunk", from: 0.5, seq: 3.5, text: "abc" }), [
    { type: "stale", floorSeq: 0, durableSeq: 0 },
  ]);
});

test("conform: a chunk ahead of the delivered prefix is a gap — stale", () => {
  const { c, violations } = conformer(0);
  c.feed(chunk(0, 3, "abc"));
  assert.deepEqual(c.feed(chunk(7, 9, "hi")), [{ type: "stale", floorSeq: 3, durableSeq: 0 }]);
  assert.match(violations[1] ?? violations[0]!, /gap/);
});

test("conform: a replayed span already delivered is absorbed, not punished (reconnect-replay)", () => {
  const { c, violations } = conformer(0);
  c.feed(chunk(0, 3, "abc"));
  assert.deepEqual(c.feed(chunk(0, 3, "abc")), [], "exact duplicate: absorbed");
  assert.deepEqual(c.feed(chunk(1, 3, "bc")), [], "covered sub-span: absorbed");
  assert.deepEqual(c.feed(chunk(3, 6, "def")), [chunk(3, 6, "def")], "the stream continues");
  assert.deepEqual(violations, []);
});

test("conform: an overlapping span that extends the prefix passes whole (the client splices)", () => {
  const { c } = conformer(0);
  c.feed(chunk(0, 4, "abcd"));
  assert.deepEqual(c.feed(chunk(2, 6, "cdef")), [chunk(2, 6, "cdef")]);
  assert.deepEqual(c.feed(chunk(6, 7, "g")), [chunk(6, 7, "g")], "the prefix advanced to 6");
});

test("conform: durable claims are monotone and bounded by the delivered prefix; others drop silently", () => {
  const { c, violations } = conformer(0);
  c.feed(chunk(0, 4, "abcd"));
  assert.deepEqual(c.feed({ type: "durable", seq: 2 }), [{ type: "durable", seq: 2 }]);
  assert.deepEqual(c.feed({ type: "durable", seq: 1 }), [], "a rewinding claim: dropped");
  assert.deepEqual(c.feed({ type: "durable", seq: 9 }), [], "a claim past the delivered prefix (P): dropped");
  assert.deepEqual(c.feed({ type: "durable", seq: 4 }), [{ type: "durable", seq: 4 }]);
  assert.deepEqual(violations, []);
});

test("conform: end within the delivered prefix passes and terminates", () => {
  const { c } = conformer(0);
  c.feed(chunk(0, 4, "abcd"));
  assert.deepEqual(c.feed(end(4)), [end(4)]);
  assert.deepEqual(c.feed(chunk(4, 5, "e")), [], "exactly one terminal, then done");
  assert.deepEqual(c.end(), []);
  assert.deepEqual(c.fail(), []);
});

test("conform: an end that outruns the delivered prefix means text was lost — stale", () => {
  const { c, violations } = conformer(0);
  c.feed(chunk(0, 4, "abcd"));
  assert.deepEqual(c.feed(end(10)), [{ type: "stale", floorSeq: 4, durableSeq: 0 }]);
  assert.equal(violations.length, 1);
});

test("conform: bare stale/absent pass through and terminate (the local plane's own floor answers are bare)", () => {
  const a = conformer(0);
  assert.deepEqual(a.c.feed({ type: "stale", floorSeq: 9, durableSeq: 3 }), [
    { type: "stale", floorSeq: 9, durableSeq: 3 },
  ]);
  const b = conformer(0);
  assert.deepEqual(b.c.feed({ type: "absent" }), [{ type: "absent" }]);
  assert.deepEqual(b.c.feed(chunk(0, 1, "a")), []);
});

test("conform: a source that ends without a terminal is a truncation — stale, and reported", () => {
  const { c, violations } = conformer(0);
  c.feed(chunk(0, 3, "abc"));
  assert.deepEqual(c.end(), [{ type: "stale", floorSeq: 3, durableSeq: 0 }]);
  assert.match(violations[0]!, /without a terminal/);
});

test("conform: a synthesized open precedes a bare end too", () => {
  const { c } = conformer(0);
  assert.deepEqual(c.feed(end(0)), [
    { type: "open", streamId: "m1", from: 0, seq: 0, durableSeq: 0, ended: false },
    end(0),
  ]);
});

// --------------------------------------------------------------------------- the plane

test("relay: a local stream always wins — attach is never consulted for a hosted stream", async () => {
  const store = new Store();
  store.seedMessage("m1");
  let attached = 0;
  const api = relayServer(store, {
    relay: {
      attach: async () => {
        attached++;
        return undefined;
      },
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1" }));
  s.push("hello");
  await s.close();
  const frames = await framesP;
  assert.equal(attached, 0);
  assert.equal(spliced("", frames), "hello");
  api.close();
});

test("relay: authorize runs BEFORE attach — a denial never dials the adapter", async () => {
  const store = new Store();
  let attached = 0;
  const api = relayServer(store, {
    relay: {
      attach: async () => {
        attached++;
        return undefined;
      },
    },
  });
  await assert.rejects(api.subscribeStream({ user: "intruder", streamId: "m1" }), /forbidden/);
  assert.equal(attached, 0);
  api.close();
});

test("relay: attach returning undefined, throwing, or hanging is `absent` — never a hung request", async () => {
  const store = new Store();
  const errors: Array<{ err: unknown; info: StreamRelayErrorInfo }> = [];
  const onRelayError = (err: unknown, info: StreamRelayErrorInfo): void => {
    errors.push({ err, info });
  };

  const declined = relayServer(store, { relay: { attach: async () => undefined }, onRelayError });
  assert.deepEqual(await collect(await declined.subscribeStream({ user: "u1", streamId: "m1" })), [
    { type: "absent" },
  ]);
  assert.equal(errors.length, 0, "declining is not an error");
  declined.close();

  const throwing = relayServer(store, {
    relay: {
      attach: () => {
        throw new Error("redis is down");
      },
    },
    onRelayError,
  });
  assert.deepEqual(await collect(await throwing.subscribeStream({ user: "u1", streamId: "m1" })), [
    { type: "absent" },
  ]);
  assert.equal(errors.length, 1);
  assert.equal(errors[0]!.info.phase, "attach");
  throwing.close();

  const hanging = relayServer(store, {
    relay: { attach: () => new Promise(() => {}) },
    relayAttachTimeoutMs: 25,
    onRelayError,
  });
  assert.deepEqual(await collect(await hanging.subscribeStream({ user: "u1", streamId: "m1" })), [
    { type: "absent" },
  ]);
  assert.equal(errors.length, 2);
  assert.match(String((errors[1]!.err as Error).message), /timed out/);
  hanging.close();
});

test("relay: an iterator-construction failure is reported and downgraded to `stale`", async () => {
  const store = new Store();
  const errors: Array<{ err: unknown; info: StreamRelayErrorInfo }> = [];
  const source: AsyncIterable<StreamFrame> = {
    [Symbol.asyncIterator](): AsyncIterator<StreamFrame> {
      throw new Error("iterator construction failed");
    },
  };
  const api = relayServer(store, {
    relay: { attach: async () => source },
    onRelayError: (err, info) => errors.push({ err, info }),
  });

  const sub = await api.subscribeStream({ user: "u1", streamId: "m1", from: 7 });
  assert.deepEqual(await collect(sub), [{ type: "stale", floorSeq: 7, durableSeq: 0 }]);
  assert.equal(errors.length, 1);
  assert.equal(errors[0]!.info.phase, "frames");
  assert.match(String((errors[0]!.err as Error).message), /iterator construction failed/);
  api.close();
});

test("relay: a publish that throws or rejects on EVERY frame leaves the producer bit-identical", async () => {
  const run = async (relay: StreamRelay | undefined): Promise<{ store: Store; frames: StreamFrame[] }> => {
    const store = new Store();
    store.seedMessage("m1");
    const errors: StreamRelayErrorInfo[] = [];
    const api = relayServer(store, relay ? { relay, onRelayError: (_e, i) => errors.push(i) } : {});
    const s = await api.openStream({ user: "u1", streamId: "m1" });
    const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1" }));
    for (const d of deltas(RESPONSE)) s.push(d);
    await s.flush();
    s.push("!");
    await s.close();
    const frames = await framesP;
    if (relay) {
      assert.ok(errors.length > 0, "the broken publish was reported");
      assert.ok(errors.every((i) => i.phase === "publish"));
    }
    api.close();
    return { store, frames };
  };

  const broken = await run({
    publish: () => {
      throw new Error("bus down");
    },
  });
  const rejected = await run({
    publish: async () => {
      throw new Error("async bus down");
    },
  });
  const clean = await run(undefined);
  assert.deepEqual(broken.store.row("m1"), clean.store.row("m1"), "same seal, same body, same status");
  assert.deepEqual(broken.store.batches, clean.store.batches, "same checkpoints, statement for statement");
  assert.deepEqual(broken.frames, clean.frames, "local subscribers are untouched");
  assert.deepEqual(rejected.store.row("m1"), clean.store.row("m1"), "rejected publish: same sealed row");
  assert.deepEqual(rejected.store.batches, clean.store.batches, "rejected publish: same checkpoints");
  assert.deepEqual(rejected.frames, clean.frames, "rejected publish: local subscribers are untouched");
});

test("relay: a relayed reader that stops draining is dropped with `stale` at the same cap as a local one", async () => {
  const store = new Store();
  const api = relayServer(store, {
    maxQueuedFrames: 4,
    relay: {
      attach: async () =>
        (async function* (): AsyncGenerator<StreamFrame> {
          for (let i = 0; i < 100; i++) yield chunk(i, i + 1, "abcdefghijklmnopqrstuvwxyz"[i % 26]!);
        })(),
    },
  });
  const sub = await api.subscribeStream({ user: "u1", streamId: "m1" });
  // Give the driver time to flood the queue BEFORE reading anything.
  await new Promise((r) => setTimeout(r, 10));
  const frames = await collect(sub);
  assert.equal(frames[0]!.type, "open");
  const last = frames[frames.length - 1]!;
  assert.equal(last.type, "stale");
  assert.ok(frames.length <= 4 + 1, `bounded by the cap (+1 for the terminal), got ${frames.length}`);
  api.close();
});

test("relay: misconfiguration is refused loudly at construction", () => {
  const store = new Store();
  assert.throws(() => relayServer(store, { relay: {} }), /neither publish nor attach/);
  assert.throws(() => relayServer(store, { onRelayError: () => {} }), /do nothing without/);
  assert.throws(() => relayServer(store, { relayAttachTimeoutMs: 500 }), /do nothing without/);
});

// --------------------------------------------------------------------------- integration: two processes, one bus

/** An in-memory relay bus: `log` mode retains history (Redis Streams / Kafka) with a trimmable
 *  floor; `pubsub` mode delivers only what arrives after the subscribe (Redis pub/sub). A published
 *  terminal closes the channel, the way a real generation's end reaches every consumer. */
class MemoryBus {
  private readonly retained = new Map<string, StreamFrame[]>();
  private readonly channels = new Map<string, Set<(f: StreamFrame | undefined) => void>>();
  private readonly closedStreams = new Set<string>();
  private readonly mode: "log" | "pubsub";

  constructor(mode: "log" | "pubsub") {
    this.mode = mode;
  }

  /** Drop retained chunks that END at or below `seq` — the log's configurable floor. */
  trimBelow(streamId: string, seq: number): void {
    const kept = (this.retained.get(streamId) ?? []).filter((f) => f.type !== "chunk" || f.seq > seq);
    this.retained.set(streamId, kept);
  }

  readonly relay: StreamRelay = {
    publish: (streamId, frame) => {
      if (this.mode === "log") {
        const list = this.retained.get(streamId) ?? [];
        list.push(frame);
        this.retained.set(streamId, list);
      }
      for (const deliver of this.channels.get(streamId) ?? []) deliver(frame);
      if (frame.type === "end" || frame.type === "stale") {
        this.closedStreams.add(streamId);
        for (const deliver of this.channels.get(streamId) ?? []) deliver(undefined);
      }
    },
    attach: async (streamId) => {
      const queue: Array<StreamFrame | undefined> = [...(this.mode === "log" ? (this.retained.get(streamId) ?? []) : [])];
      if (this.closedStreams.has(streamId)) queue.push(undefined);
      let waiter: (() => void) | undefined;
      const deliver = (f: StreamFrame | undefined): void => {
        queue.push(f);
        waiter?.();
        waiter = undefined;
      };
      const set = this.channels.get(streamId) ?? new Set();
      set.add(deliver);
      this.channels.set(streamId, set);
      const self = this;
      return (async function* (): AsyncGenerator<StreamFrame> {
        try {
          for (;;) {
            while (queue.length === 0) await new Promise<void>((r) => (waiter = r));
            const f = queue.shift();
            if (f === undefined) return;
            yield f;
          }
        } finally {
          set.delete(deliver);
          if (set.size === 0) self.channels.delete(streamId);
        }
      })();
    },
  };
}

/** Producer on A (publish only), reader on B (attach only) — the §3.1 broadcast/log split. */
function twoServers(store: Store, bus: MemoryBus) {
  const apiA = relayServer(store, { relay: { publish: bus.relay.publish! }, onRelayError: () => {} });
  const apiB = relayServer(store, { relay: { attach: bus.relay.attach! }, onRelayError: () => {} });
  return { apiA, apiB };
}

test("relay integration: a reader on process B sees process A's tokens, live", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const bus = new MemoryBus("pubsub");
  const { apiA, apiB } = twoServers(store, bus);

  // B subscribes BEFORE the first token — the join-at-the-live-edge case pub/sub can serve.
  const framesP = collect(await apiB.subscribeStream({ user: "u1", streamId: "m1" }));
  const s = await apiA.openStream({ user: "u1", streamId: "m1" });
  for (const d of deltas(RESPONSE)) s.push(d);
  await s.close();

  const frames = await framesP;
  assert.equal(frames[0]!.type, "open", "synthesized: the bus never carries per-subscriber opens");
  assert.equal(spliced("", frames), RESPONSE);
  const last = frames[frames.length - 1]!;
  assert.equal(last.type === "end" && last.status, "complete");
  apiA.close();
  apiB.close();
});

test("relay integration: pub/sub mid-join gaps to `stale` — the honest floor, not a silent hole", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const bus = new MemoryBus("pubsub");
  const { apiA, apiB } = twoServers(store, bus);

  const s = await apiA.openStream({ user: "u1", streamId: "m1" });
  const ds = deltas(RESPONSE);
  for (const d of ds.slice(0, 5)) s.push(d);

  // Joining mid-generation at 0: pub/sub has no history, so the first live frame gaps.
  const framesP = collect(await apiB.subscribeStream({ user: "u1", streamId: "m1" }));
  for (const d of ds.slice(5)) s.push(d);
  await s.close();

  const frames = await framesP;
  assert.equal(frames.length, 1);
  assert.equal(frames[0]!.type, "stale", "chunky but correct: the durable plane carries this reader");
  apiA.close();
  apiB.close();
});

test("relay integration: a log-backed bus serves any `from` — mid-join and late-join get the whole text", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const bus = new MemoryBus("log");
  const { apiA, apiB } = twoServers(store, bus);

  const s = await apiA.openStream({ user: "u1", streamId: "m1" });
  const ds = deltas(RESPONSE);
  for (const d of ds.slice(0, 5)) s.push(d);

  // Mid-join from 0: the log replays from the beginning, then tails.
  const midP = collect(await apiB.subscribeStream({ user: "u1", streamId: "m1" }));
  // Mid-join from a durable offset: replay below `from` is absorbed by the conform pass, the
  // crossing chunk overlaps, and the client's splice makes it exact.
  const joinAt = 7;
  const midOffsetP = collect(await apiB.subscribeStream({ user: "u1", streamId: "m1", from: joinAt }));
  for (const d of ds.slice(5)) s.push(d);
  await s.close();

  assert.equal(spliced("", await midP), RESPONSE);
  assert.equal(spliced(RESPONSE.slice(0, joinAt), await midOffsetP), RESPONSE);

  // Late join, after the seal: the log still carries everything including `end`.
  const late = await collect(await apiB.subscribeStream({ user: "u1", streamId: "m1" }));
  assert.equal(spliced("", late), RESPONSE);
  assert.equal(late[late.length - 1]!.type, "end");
  apiA.close();
  apiB.close();
});

test("relay integration: a trimmed log floor downgrades an out-of-range join to `stale`", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const bus = new MemoryBus("log");
  const { apiA, apiB } = twoServers(store, bus);

  const s = await apiA.openStream({ user: "u1", streamId: "m1" });
  for (const d of deltas(RESPONSE)) s.push(d);
  await s.close();
  bus.trimBelow("m1", 20);

  const frames = await collect(await apiB.subscribeStream({ user: "u1", streamId: "m1" }));
  assert.equal(frames[frames.length - 1]!.type, "stale", "the floor is visible as `stale`, never a hole");
  assert.ok(!frames.some((f) => f.type === "chunk" && f.from > 20), "no chunk beyond the floor sneaks in early");
  apiA.close();
  apiB.close();
});
