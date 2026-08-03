// Property-based integration tests for the LM stream plane
// (designs-implemented/LM-STREAM-CHECKPOINT-DESIGN.md §0, §9).
//
// The example suite (`streams.test.ts`) checks hand-picked instances of the contract; this suite
// takes the quantifiers seriously. Each fc case generates a POLICY (checkpoint thresholds, store
// latency, queue caps) and a SCHEDULE — an op sequence over one producer, up to four subscribers,
// virtual-clock ticks, and injected store faults — runs it against the real plane over the real
// SQLite store, and checks:
//
//   P  (prefix)   after every op: the store's text is a prefix of the produced text, the row's
//                 length column equals it, chunk rows are contiguous, and `durableSeq` is monotone
//                 and never ahead of `seq`.
//   S  (splice)   for every subscriber, at every generated join offset and join time: the join
//                 prefix plus its `chunk` frames reconstructs the produced text exactly.
//   frames        chunks are contiguous and carry `text.length === seq - from`; `durable` is
//                 monotone and never claims a prefix the store does not hold by delivery time;
//                 exactly one terminal frame ends a subscription.
//   idempotency   injected failures and lost acks never duplicate a chunk row or double-advance
//                 the length CAS (P above would catch either).
//   seal          a successful seal compacts to exactly the produced text with the expected status;
//                 a failed one leaves `streaming` and a durable prefix.
//
// DETERMINISM: `setTimeout` is mocked (node:test mock timers), so the checkpoint cadence, the retry
// backoff, the linger window, AND the store's simulated latency all advance only on generated
// `tick` ops — every counterexample replays exactly, and fc's shrinking works. Time only moves
// when the schedule says so; microtasks are drained through the (unmocked) `setImmediate`.
//
// Produced text is a deterministic alphabet sequence, so a duplicated or dropped span corrupts
// CONTENT, not just lengths — string equality is the oracle.
//
// RELAY LANES (designs-implemented/LM-STREAM-RELAY-DESIGN.md §9): each generated subscriber is
// local or RELAYED — a second api-server instance over the same store, attached through an
// in-memory log bus fed by the producer's `publish`. A relayed subscriber may additionally carry a
// HOSTILE perturbation plan (reorder, duplicate, drop, truncate, gap, oversized chunk, double
// terminal, throw mid-iteration) applied to the frames its adapter yields. The property is §4's
// claim stated as a test: no perturbation may corrupt the spliced text — every relayed accumulator
// is always a true prefix of the produced text, an honest (unperturbed) relay satisfies S exactly
// like a local subscriber, and the worst hostile outcome is an early `stale`.
//
// Run count: 120 by default; RINDLE_PBT_RUNS deepens the sweep (nightly runs 20k). On failure
// fast-check prints `{ seed, path }` and the shrunk counterexample; RINDLE_PBT_SEED replays a
// specific seed's sequence.

import { test } from "node:test";
import assert from "node:assert/strict";

import fc from "fast-check";

import { createRindleApiServer, type StreamFrame, type StreamHandle } from "../src/index.ts";
import { Store, TABLES } from "./stream-store.ts";

const RUNS = Number(process.env.RINDLE_PBT_RUNS ?? 120);
const STREAM = "m1";

// --------------------------------------------------------------------------- ops & config

/** One perturbation applied to one arriving frame on a relayed subscriber's adapter:
 *  `pass` delivers it; `dup` delivers it twice; `drop` loses it; `hold` swaps it behind the next
 *  frame (a reorder); `gap`/`big` inject a structurally-broken chunk ahead of it; `term2` injects a
 *  double terminal; `throw` rejects the iterator; `cut` truncates the source without a terminal. */
type Perturb = "pass" | "dup" | "drop" | "hold" | "gap" | "big" | "term2" | "throw" | "cut";

type Op =
  | { t: "push"; n: number }
  | { t: "tick"; ms: number }
  | { t: "flush"; awaited: boolean }
  | { t: "subscribe"; fromKind: number; relayed: boolean; perturb: Perturb[] }
  | { t: "read"; sub: number; k: number }
  | { t: "closeSub"; sub: number }
  | { t: "failNextBatch" }
  | { t: "failNextQuery" }
  | { t: "loseNextAck" }
  | { t: "cancel" }
  | { t: "seal"; kind: "close" | "fail" | "drain" };

const perturbArb: fc.Arbitrary<Perturb> = fc.constantFrom(
  "pass",
  "dup",
  "drop",
  "hold",
  "gap",
  "big",
  "term2",
  "throw",
  "cut",
);

const opArb: fc.Arbitrary<Op> = fc.oneof(
  { weight: 6, arbitrary: fc.integer({ min: 1, max: 24 }).map((n): Op => ({ t: "push", n })) },
  { weight: 4, arbitrary: fc.constantFrom(5, 20, 60, 400, 900).map((ms): Op => ({ t: "tick", ms })) },
  { weight: 2, arbitrary: fc.boolean().map((awaited): Op => ({ t: "flush", awaited })) },
  {
    weight: 3,
    arbitrary: fc
      .record({
        fromKind: fc.integer({ min: 0, max: 3 }),
        relayed: fc.boolean(),
        // Often empty — an honest relay must satisfy S exactly like a local subscriber.
        perturb: fc.array(perturbArb, { maxLength: 5 }),
      })
      .map(({ fromKind, relayed, perturb }): Op => ({ t: "subscribe", fromKind, relayed, perturb })),
  },
  { weight: 4, arbitrary: fc.tuple(fc.nat({ max: 3 }), fc.integer({ min: 1, max: 40 })).map(([sub, k]): Op => ({ t: "read", sub, k })) },
  { weight: 1, arbitrary: fc.nat({ max: 3 }).map((sub): Op => ({ t: "closeSub", sub })) },
  { weight: 1, arbitrary: fc.constant<Op>({ t: "failNextBatch" }) },
  { weight: 1, arbitrary: fc.constant<Op>({ t: "failNextQuery" }) },
  { weight: 1, arbitrary: fc.constant<Op>({ t: "loseNextAck" }) },
  { weight: 1, arbitrary: fc.constant<Op>({ t: "cancel" }) },
  { weight: 1, arbitrary: fc.constantFrom("close", "fail", "drain").map((kind): Op => ({ t: "seal", kind: kind as "close" | "fail" | "drain" })) },
);

const cfgArb = fc.record({
  chars: fc.constantFrom(4, 16, 64, 1_000_000),
  intervalMs: fc.constantFrom(25, 100, 10_000),
  retries: fc.integer({ min: 0, max: 2 }),
  maxQueuedFrames: fc.constantFrom(4, 16, 1024),
  latencyMs: fc.constantFrom(0, 5, 30),
});
type Cfg = { chars: number; intervalMs: number; retries: number; maxQueuedFrames: number; latencyMs: number };

// --------------------------------------------------------------------------- pumping helpers

/** Drain the microtask/immediate queue so everything runnable without a timer runs. */
async function drainMicro(): Promise<void> {
  for (let i = 0; i < 25; i++) await new Promise((r) => setImmediate(r));
}

/** Check whether `p` has settled after a microtask drain, without blocking on it. */
async function ready<T>(p: Promise<T>): Promise<{ v: T } | undefined> {
  let out: { v: T } | undefined;
  void p.then((v) => (out = { v }));
  await drainMicro();
  return out;
}

type Timers = { tick(ms: number): void };

/** Advance virtual time (checkpoint cadence, retry backoff, store latency) until `p` settles. */
async function settle<T>(timers: Timers, p: Promise<T>): Promise<T> {
  let done = false;
  const tracked = p.then(
    (v) => ((done = true), { ok: true as const, v }),
    (e) => ((done = true), { ok: false as const, e }),
  );
  for (let i = 0; i < 400 && !done; i++) {
    await drainMicro();
    if (done) break;
    timers.tick(250);
  }
  const out = await tracked;
  if (!done) throw new Error("the schedule did not quiesce in 100s of virtual time");
  if (out.ok) return out.v;
  throw out.e;
}

// --------------------------------------------------------------------------- the relay bus

type BusItem = { f: StreamFrame } | { boom: true } | { eos: true };

/** One relayed subscriber's adapter: a queue fed by the bus, with this subscriber's perturbation
 *  plan applied frame by frame as they arrive. Deterministic — delivery is synchronous into the
 *  queue; the driver drains it on microtasks. */
class BusReader {
  private readonly queue: BusItem[] = [];
  private waiter: (() => void) | undefined;
  private held: StreamFrame | undefined;
  private planAt = 0;
  private stopped = false;
  private readonly plan: Perturb[];
  private readonly onDetach: (r: BusReader) => void;

  constructor(plan: Perturb[], onDetach: (r: BusReader) => void) {
    this.plan = plan;
    this.onDetach = onDetach;
  }

  private push(item: BusItem): void {
    this.queue.push(item);
    this.waiter?.();
    this.waiter = undefined;
  }

  deliver(frame: StreamFrame): void {
    if (this.stopped) return;
    const swapped = this.held;
    this.held = undefined;
    switch (this.plan[this.planAt++] ?? "pass") {
      case "pass":
        this.push({ f: frame });
        break;
      case "dup":
        this.push({ f: frame });
        this.push({ f: frame });
        break;
      case "drop":
        break;
      case "hold":
        this.held = frame; // emitted AFTER the next arrival — an adjacent reorder
        break;
      case "gap":
        this.push({ f: { type: "chunk", from: 1_000_000, seq: 1_000_003, text: "zzz" } });
        this.push({ f: frame });
        break;
      case "big":
        this.push({ f: { type: "chunk", from: 0, seq: 2, text: "zzzzz" } });
        this.push({ f: frame });
        break;
      case "term2":
        this.push({ f: { type: "stale", floorSeq: 0, durableSeq: 0 } });
        this.push({ f: { type: "stale", floorSeq: 0, durableSeq: 0 } });
        this.push({ f: frame });
        break;
      case "throw":
        this.stopped = true;
        this.push({ boom: true });
        return;
      case "cut":
        this.stopped = true;
        this.push({ eos: true });
        return;
    }
    if (swapped) this.push({ f: swapped });
  }

  /** The producer sealed (or the bus is closing): flush a held frame, then end the source. */
  close(): void {
    if (this.stopped) return;
    this.stopped = true;
    if (this.held) this.push({ f: this.held });
    this.held = undefined;
    this.push({ eos: true });
  }

  iterable(): AsyncIterable<StreamFrame> {
    const self = this;
    return {
      [Symbol.asyncIterator](): AsyncIterator<StreamFrame> {
        return {
          async next(): Promise<IteratorResult<StreamFrame>> {
            while (self.queue.length === 0) await new Promise<void>((r) => (self.waiter = r));
            const item = self.queue.shift()!;
            if ("f" in item) return { value: item.f, done: false };
            self.onDetach(self);
            if ("boom" in item) throw new Error("relay adapter threw mid-iteration");
            return { value: undefined as never, done: true };
          },
          async return(): Promise<IteratorResult<StreamFrame>> {
            self.onDetach(self);
            return { value: undefined as never, done: true };
          },
        };
      },
    };
  }
}

/** An in-memory LOG bus (Redis Streams-shaped: full retention, so any `from` is servable): the
 *  producer's `publish` appends and fans out; `attach` replays the log then tails. A published
 *  terminal closes every reader's source. One stream only — the suite drives one producer. */
class LogBus {
  private readonly log: StreamFrame[] = [];
  private closed = false;
  private readonly readers = new Set<BusReader>();
  /** The perturbation plan the NEXT attach picks up (set just before its subscribe). */
  nextPlan: Perturb[] = [];

  readonly publish = (_streamId: string, frame: StreamFrame): void => {
    this.log.push(frame);
    const terminal = frame.type === "end" || frame.type === "stale" || frame.type === "absent";
    for (const r of [...this.readers]) r.deliver(frame);
    if (terminal) {
      this.closed = true;
      for (const r of [...this.readers]) r.close();
    }
  };

  readonly attach = async (_streamId: string, _from: number): Promise<AsyncIterable<StreamFrame>> => {
    const reader = new BusReader(this.nextPlan, (r) => this.readers.delete(r));
    this.nextPlan = [];
    for (const f of this.log) reader.deliver(f);
    if (this.closed) reader.close();
    else this.readers.add(reader);
    return reader.iterable();
  };
}

// --------------------------------------------------------------------------- one subscriber's model

/** Checks the frame contract incrementally as frames are drained, and accumulates the tail for the
 *  final S check. `producedRef` is the model's produced text (append-only, so slices are stable).
 *
 *  `relayed` subscribers ride the conform pass, whose contract differs in exactly two ways: chunks
 *  may OVERLAP the delivered prefix (the client splices at the frame's own offset; they must still
 *  extend it and never gap), and under a HOSTILE adapter an `end` guarantees only that everything
 *  the store confirmed was delivered — an honest relay satisfies S exactly like a local subscriber. */
class SubModel {
  frames: StreamFrame[] = [];
  acc = "";
  opened = false;
  terminal: "end" | "stale" | "absent" | undefined;
  private lastDurable = 0;
  private pending: Promise<IteratorResult<StreamFrame>> | undefined;
  private readonly it: AsyncIterator<StreamFrame>;
  private readonly expectedFrom: number;
  private readonly producedRef: () => string;
  private readonly storeSeq: () => number;
  private readonly sealed: () => boolean;
  private readonly mode: "local" | "relayed";
  private readonly hostile: boolean;

  constructor(
    expectedFrom: number,
    producedRef: () => string,
    storeSeq: () => number,
    sealed: () => boolean,
    frames: AsyncIterable<StreamFrame>,
    mode: "local" | "relayed" = "local",
    hostile = false,
  ) {
    this.expectedFrom = expectedFrom;
    this.producedRef = producedRef;
    this.storeSeq = storeSeq;
    this.sealed = sealed;
    this.it = frames[Symbol.asyncIterator]();
    this.mode = mode;
    this.hostile = hostile;
  }

  /** Pull up to `k` frames, giving up (without blocking) when none is ready right now. */
  async read(k: number): Promise<void> {
    for (let i = 0; i < k && this.terminal === undefined; i++) {
      this.pending ??= this.it.next();
      const res = await ready(this.pending);
      if (res === undefined) return; // nothing ready; keep the in-flight next() for later
      this.pending = undefined;
      if (res.v.done) return;
      this.check(res.v.value);
    }
  }

  private check(frame: StreamFrame): void {
    assert.equal(this.terminal, undefined, `frame after terminal: ${frame.type}`);
    this.frames.push(frame);
    const produced = this.producedRef();
    switch (frame.type) {
      case "open":
        assert.equal(this.opened, false, "open must be first and unique");
        this.opened = true;
        assert.equal(frame.from, this.expectedFrom, "open.from is the clamped join offset");
        this.acc = produced.slice(0, frame.from);
        break;
      case "chunk": {
        assert.ok(this.opened, "chunk before open");
        assert.equal(frame.text.length, frame.seq - frame.from, "a chunk spans exactly its offsets");
        if (this.mode === "local") {
          assert.equal(frame.from, this.acc.length, "chunks are contiguous with the join prefix");
        } else {
          // The conform contract: a delivered chunk may overlap the prefix but never gaps it, and
          // always extends it (replays are absorbed before delivery).
          assert.ok(frame.from <= this.acc.length, `a conformed chunk never gaps (${frame.from} > ${this.acc.length})`);
          assert.ok(frame.seq > this.acc.length, "a conformed chunk always extends the delivered prefix");
        }
        const next = this.acc.slice(0, frame.from) + frame.text;
        assert.ok(produced.startsWith(next), "a chunk carries the produced text verbatim");
        this.acc = next;
        break;
      }
      case "durable":
        assert.ok(frame.seq >= this.lastDurable, "durable is monotone");
        assert.ok(frame.seq <= this.storeSeq(), "durable never claims a prefix the store does not hold");
        this.lastDurable = frame.seq;
        break;
      case "end":
        // The relay-side guarantee behind this: conform refuses an `end` whose durable length
        // outruns what was delivered, so `end` never overclaims what this reader saw.
        assert.ok(this.acc.length >= frame.seq, "end only after everything the store confirmed was delivered");
        this.terminal = "end";
        break;
      case "stale":
        this.terminal = "stale";
        break;
      case "absent":
        assert.equal(this.mode, "local", "a relayed subscription never sees absent — the bus always attaches");
        // Only reachable by joining after the linger window evicted a sealed stream.
        assert.ok(this.sealed(), "absent for a live, hosted stream");
        assert.equal(this.opened, false);
        this.terminal = "absent";
        break;
    }
  }

  /** The final word, at quiescence. */
  finish(): void {
    const produced = this.producedRef();
    if (this.terminal === "end" && !this.hostile) {
      // Local subscribers and HONEST relays alike: `end` means the whole produced text arrived.
      assert.equal(this.acc, produced, "S: join prefix ++ chunks == produced, for every join offset and time");
    } else {
      // Dropped (stale), evicted (absent), closed early — or a hostile relay, where a lossy adapter
      // may legally deliver a shortened prefix before its `end` (bounded by the end.seq check
      // above). What was seen must still be a true prefix, and it never shrank.
      assert.ok(produced.startsWith(this.acc), "a partial subscription is always a prefix");
    }
  }
}

// --------------------------------------------------------------------------- the case runner

const ALPHABET = "abcdefghijklmnopqrstuvwxyz";

async function runCase(timers: Timers, cfg: Cfg, ops: Op[]): Promise<void> {
  const store = new Store();
  store.latencyMs = cfg.latencyMs;
  store.seedMessage(STREAM);
  const bus = new LogBus();
  // The PRODUCER instance: every fanout frame is mirrored to the bus (LM-STREAM-RELAY §3).
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: {
      checkpoint: { tables: TABLES },
      authorize: () => true,
      policy: { chars: cfg.chars, intervalMs: cfg.intervalMs, retries: cfg.retries },
      maxQueuedFrames: cfg.maxQueuedFrames,
      onCheckpointError: () => {},
      relay: { publish: bus.publish },
      onRelayError: () => {},
    },
  });
  // The READER instance — a second process over the same store, never hosting the stream: its
  // subscribes miss the live plane and ride `attach` through the conform pass.
  const apiB = createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: {
      checkpoint: { tables: TABLES },
      authorize: () => true,
      maxQueuedFrames: cfg.maxQueuedFrames,
      onCheckpointError: () => {},
      relay: { attach: bus.attach },
      onRelayError: () => {},
    },
  });

  let produced = "";
  const producedRef = (): string => produced;
  const storeSeq = (): number => store.row(STREAM).seq;
  let lastHandleDurable = 0;
  let sealInitiated = false;
  let sealExpected: string | undefined; // the status the FIRST seal should land
  let sealOutcome: Promise<void> | undefined;
  let faultsInjected = false;
  const subs: SubModel[] = [];
  const floating: Array<Promise<unknown>> = [];

  const handle: StreamHandle = await settle(timers, api.openStream({ user: "u1", streamId: STREAM }));

  /** P, checked after every op. */
  const checkStore = (): void => {
    const row = store.row(STREAM);
    const chunks = store.chunks(STREAM);
    const durable = store.durableText(STREAM);
    assert.ok(produced.startsWith(durable), "P: the store is a prefix of the produced text");
    assert.equal(row.seq, durable.length, "the length column always equals the durable text");
    let at = row.body.length;
    for (const c of chunks) {
      assert.equal(c.seq - c.text.length, at, "chunk rows are contiguous and gap-free");
      at = c.seq;
    }
    assert.ok(handle.durableSeq <= handle.seq, "P: durableSeq never exceeds seq");
    assert.ok(handle.durableSeq >= lastHandleDurable, "P: durableSeq is monotone");
    lastHandleDurable = handle.durableSeq;
  };

  const issueSeal = (kind: "close" | "fail" | "drain"): void => {
    if (!sealInitiated) {
      sealInitiated = true;
      sealExpected =
        kind === "fail" ? "error" : kind === "drain" ? "interrupted" : handle.cancelled ? "cancelled" : "complete";
    }
    const p =
      kind === "close" ? handle.close() : kind === "fail" ? handle.fail(new Error("boom")) : api.drainStreams();
    p.catch(() => {});
    sealOutcome ??= p;
  };

  try {
    for (const op of ops) {
      switch (op.t) {
        case "push": {
          const text = Array.from({ length: op.n }, (_, j) => ALPHABET[(produced.length + j) % 26]).join("");
          if (sealInitiated) {
            assert.throws(() => handle.push(text), /closed/, "push after seal must refuse");
          } else {
            handle.push(text);
            produced += text;
            assert.equal(handle.seq, produced.length, "seq counts every pushed code unit");
          }
          break;
        }
        case "tick":
          timers.tick(op.ms);
          break;
        case "flush": {
          const at = handle.seq;
          const p = handle.flush();
          p.catch(() => {});
          if (op.awaited) {
            try {
              const durable = await settle(timers, p);
              assert.ok(durable >= at, "a resolved flush means the store holds everything asked for");
            } catch {
              // A rejected flush is legitimate only when a fault could have caused it, or the
              // stream ended short — never on a healthy schedule.
              assert.ok(faultsInjected || sealInitiated, "flush rejected on a healthy schedule");
            }
          } else {
            floating.push(p);
          }
          break;
        }
        case "subscribe": {
          if (subs.length >= 4) break;
          if (op.relayed) {
            // A relayed `from` is honest by construction (a durable length or a Last-Event-ID, both
            // ≤ produced): the conform pass cannot clamp a beyond-the-edge ask the way the hosting
            // process can, and no real client can make one.
            const requested = [0, storeSeq(), produced.length][op.fromKind % 3]!;
            bus.nextPlan = op.perturb;
            const sub = await apiB.subscribeStream({ user: "u1", streamId: STREAM, from: requested });
            subs.push(
              new SubModel(requested, producedRef, storeSeq, () => sealInitiated, sub.frames, "relayed", op.perturb.length > 0),
            );
          } else {
            const requested = [0, storeSeq(), produced.length, produced.length + 7][op.fromKind]!;
            const expected = Math.min(requested, handle.seq);
            const sub = await api.subscribeStream({ user: "u1", streamId: STREAM, from: requested });
            subs.push(new SubModel(expected, producedRef, storeSeq, () => sealInitiated, sub.frames));
          }
          break;
        }
        case "read": {
          if (subs.length === 0) break;
          await subs[op.sub % subs.length]!.read(op.k);
          break;
        }
        case "closeSub":
          // Detaching early is a client's right at any moment; its partial stays a prefix.
          if (subs.length > 0) subs.splice(op.sub % subs.length, 1);
          break;
        case "failNextBatch":
          faultsInjected = true;
          store.failures.push(new Error("injected store failure"));
          break;
        case "failNextQuery":
          faultsInjected = true;
          store.failNextQueries++;
          break;
        case "loseNextAck":
          faultsInjected = true;
          store.loseNextAck = true;
          break;
        case "cancel":
          store.setCancel(STREAM);
          break;
        case "seal":
          issueSeal(op.kind);
          break;
      }
      await drainMicro();
      checkStore();
    }

    // ---- teardown to quiescence: seal (fault-free), settle everything, drain every subscriber.
    store.failures = [];
    store.loseNextAck = false;
    store.failNextQueries = 0;
    if (!sealInitiated) issueSeal("close");
    let sealRejected = false;
    try {
      await settle(timers, sealOutcome!);
    } catch {
      sealRejected = true;
    }
    for (const p of floating) await settle(timers, p).catch(() => {});
    for (const sub of subs) {
      for (let i = 0; i < 50 && sub.terminal === undefined; i++) {
        await sub.read(1000);
        if (sub.terminal === undefined) timers.tick(250);
      }
      assert.ok(sub.terminal !== undefined, "every subscriber is released at quiescence");
      sub.finish();
    }
    checkStore();

    // The STORE is the judge of whether the seal landed — a rejected seal promise can still have
    // committed (its ack lost), and a drain-carried seal failure resolves (`allSettled`). The
    // caller-facing promise is held to honesty instead: it may only reject when a fault existed.
    const row = store.row(STREAM);
    if (row.status !== "streaming") {
      assert.equal(row.body, produced, "a durable seal compacts to exactly the produced text");
      assert.deepEqual(store.chunks(STREAM), [], "…and drops every chunk row");
      assert.equal(row.status, sealExpected, "the row records why the stream ended");
      if (sealExpected === "error") assert.equal(row.error, "boom");
      if (sealRejected) assert.ok(faultsInjected, "a durable seal's promise rejected on a healthy schedule");
    } else {
      // The seal genuinely did not commit: only reachable through an injected fault, it never
      // claims completion, and the checkpointed prefix stands for the sweeper to find.
      assert.ok(faultsInjected, "a seal failed durably on a healthy schedule");
      assert.ok(produced.startsWith(store.durableText(STREAM)));
    }
  } finally {
    api.close();
    apiB.close();
  }
}

// --------------------------------------------------------------------------- the property

test("stream plane: S, P, frame contract, and seal honesty hold over generated schedules", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const timers = t.mock.timers;
  await fc.assert(
    fc.asyncProperty(cfgArb, fc.array(opArb, { minLength: 1, maxLength: 40 }), async (cfg, ops) => {
      await runCase(timers, cfg, ops);
    }),
    {
      numRuns: RUNS,
      ...(process.env.RINDLE_PBT_SEED ? { seed: Number(process.env.RINDLE_PBT_SEED) } : {}),
    },
  );
});
