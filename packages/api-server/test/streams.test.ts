// LM stream plane (designs-implemented/LM-STREAM-CHECKPOINT-DESIGN.md) — the contract, not the plumbing.
//
// The two properties every test here is ultimately about:
//   S (splice)  durableText(f) ++ concat(frames from f) == producedText, for every join offset and time
//   P (prefix)  durableText is always a PREFIX of producedText, and durableSeq is monotone
//
// The durable half runs against a REAL SQLite (`node:sqlite`), because the interesting claims are
// claims about SQL: that the length CAS refuses a double apply, that a replayed chunk insert is
// absorbed by its deterministic id, and that compaction swaps body-for-chunks atomically. A hand
// mock of those would only prove I can restate my own assumptions.

import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, number, string, table } from "@rindle/client";

import {
  DEFAULT_RINDLE_API_ROUTES,
  RindleApiError,
  assembleDurableText,
  createRindleApiServer,
  spliceStreamText,
  streamFramesToSse,
  streamRequestFromHttp,
  type RindleApiServerOptions,
  type StreamCommitInput,
  type StreamFrame,
  type StreamSubscription,
} from "../src/index.ts";

// The real-SQLite durable half + fault injection, shared with `streams.property.test.ts`.
import { HOST_TABLES, Store, TABLES } from "./stream-store.ts";

function serverWith(store: Store, over?: Partial<RindleApiServerOptions<string>>) {
  return createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: {
      checkpoint: { tables: TABLES },
      authorize: ({ user }) => user === "u1",
      policy: { chars: 16, intervalMs: 10_000, retries: 2 },
      onCheckpointError: () => {},
    },
    ...over,
  });
}

async function collect(sub: StreamSubscription): Promise<StreamFrame[]> {
  const out: StreamFrame[] = [];
  for await (const f of sub.frames) out.push(f);
  return out;
}

/** The client's side of contract S: the join prefix plus every `chunk`. */
function producedFrom(durablePrefix: string, frames: StreamFrame[]): string {
  let text = durablePrefix;
  for (const f of frames) {
    if (f.type !== "chunk") continue;
    assert.equal(f.text.length, f.seq - f.from, "a chunk's text must be exactly its offset span");
    assert.equal(f.from, text.length, "chunks must be contiguous with the join prefix");
    text += f.text;
  }
  return text;
}

const RESPONSE = "The quick brown fox jumps over the lazy dog, and then keeps going for a while.";
/** Deltas of ragged size, the way a real generator emits them. */
function deltas(text: string): string[] {
  const out: string[] = [];
  let i = 0;
  for (let n = 1; i < text.length; n = (n % 7) + 1) {
    out.push(text.slice(i, i + n));
    i += n;
  }
  return out;
}

// --------------------------------------------------------------------------- the contract

test("S/P: chunk rows carry the response, compaction folds them into the body at close", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  assert.equal(store.row("m1").status, "streaming", "the pointer is live before the first token");

  const ds = deltas(RESPONSE);
  for (const d of ds) s.push(d);

  // Mid-flight: the durable text lives in CHUNK rows, each carrying only its own slice — never a
  // growing column, whose `edit` would re-stream the whole message twice per checkpoint.
  await s.flush();
  const mid = store.chunks("m1");
  assert.ok(mid.length > 0 && mid.length < ds.length / 3, `expected coarse chunks, got ${mid.length}`);
  assert.equal(store.durableText("m1"), RESPONSE);
  assert.equal(store.row("m1").body, "", "nothing is compacted while streaming");
  assert.equal(store.row("m1").seq, RESPONSE.length);
  // Contiguous and gap-free (P): each chunk's span abuts the previous one's end.
  let at = 0;
  for (const c of mid) {
    assert.equal(c.seq - c.text.length, at);
    at = c.seq;
  }

  await s.close();
  const row = store.row("m1");
  assert.equal(row.body, RESPONSE, "compaction wrote the whole body...");
  assert.deepEqual(store.chunks("m1"), [], "...and dropped every chunk it absorbed");
  assert.equal(row.seq, RESPONSE.length);
  assert.equal(row.status, "complete");
  assert.equal(store.durableText("m1"), RESPONSE, "the client's assembly is unchanged by compaction");
});

test("S: a first-touch subscriber receives every character, in order", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1" }));

  for (const d of deltas(RESPONSE)) s.push(d);
  await s.close();

  const frames = await framesP;
  assert.equal(frames[0].type, "open");
  assert.equal(producedFrom("", frames), RESPONSE);
  const last = frames[frames.length - 1];
  assert.equal(last.type === "end" && last.status, "complete");
});

test("S: a LATE joiner at any offset splices the durable prefix with the live tail", async () => {
  for (const cut of [0, 5, 20, 43]) {
    const store = new Store();
    store.seedMessage("m1");
    const api = serverWith(store);
    const s = await api.openStream({ user: "u1", streamId: "m1" });

    const ds = deltas(RESPONSE);
    for (const d of ds.slice(0, 12)) s.push(d);
    await s.flush();
    assert.equal(store.durableText("m1"), RESPONSE.slice(0, 43), "the joiner's view has real content to join at");

    // The joiner knows `cut` characters (its IVM view, possibly lagging the checkpoint).
    const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1", from: cut }));
    for (const d of ds.slice(12)) s.push(d);
    await s.close();

    assert.equal(producedFrom(RESPONSE.slice(0, cut), await framesP), RESPONSE, `late join at ${cut}`);
  }
});

test("a joiner ahead of the producer is clamped, never handed a hole", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("abc");
  const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1", from: 999 }));
  s.push("def");
  await s.close();
  const frames = await framesP;
  assert.deepEqual(frames[0], { type: "open", streamId: "m1", from: 3, seq: 3, durableSeq: 0, ended: false });
  assert.equal(producedFrom("abc", frames), "abcdef");
});

// --------------------------------------------------------------------------- checkpoint policy

test("policy: the char threshold, an explicit flush, and close are the only boundaries here", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store); // chars: 16, intervalMs: 10s (so the timer never fires here)

  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("12345678"); // 8 — under the threshold
  assert.equal(s.durableSeq, 0);
  s.push("90123456"); // 16 — at the threshold
  await s.flush();
  assert.equal(s.durableSeq, 16);
  assert.deepEqual(store.chunks("m1"), [{ seq: 16, text: "1234567890123456" }]);

  s.push("tail"); // 4 — under the threshold again
  assert.equal(s.durableSeq, 16);
  await s.close();
  assert.equal(store.row("m1").body, "1234567890123456tail");
});

test("a flush racing an in-flight checkpoint gets its OWN round, not the interval cadence", async () => {
  // THE flush-latency bug the property suite flushed out: `forced` used to be cleared AFTER an
  // in-flight round, so a flush landing mid-round was swallowed and waited out `intervalMs` —
  // 10 seconds here, 750ms in production defaults, on the plane's ordering primitive.
  const store = new Store();
  store.seedMessage("m1");
  store.latencyMs = 20; // keeps the first round in flight while the flush lands
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 4, intervalMs: 10_000 } },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const t0 = Date.now();
  s.push("abcd"); // threshold: a checkpoint round is now in flight (held by latency)
  s.push("e");
  await s.flush(); // lands mid-round; must trigger its own follow-up round
  assert.equal(s.durableSeq, 5);
  assert.ok(Date.now() - t0 < 2_000, "the flush resolved on its own round, not the 10s cadence");
  await s.close();
});

test("flush is the ORDERING primitive: the text is durable before the next discrete write", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("calling a tool");
  assert.equal(await s.flush(), "calling a tool".length);
  assert.equal(store.durableText("m1"), "calling a tool"); // an app's tool-call row would land HERE
  await s.close();
});

test("the interval timer checkpoints a sub-threshold tail on its own", async () => {
  // No flush, no char threshold, no close — only the `intervalMs` cadence can drive this one. It is
  // the trigger that bounds the loss window on a slow generator (§3.1).
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 1_000_000, intervalMs: 20 } },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("tiny");

  const deadline = Date.now() + 2_000;
  while (s.durableSeq < 4 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
  assert.equal(s.durableSeq, 4, "the timer fired a checkpoint without any other trigger");
  assert.deepEqual(store.chunks("m1"), [{ seq: 4, text: "tiny" }]);
  await s.close();
});

test("checkpoints coalesce rather than queue when the store is slow", async () => {
  const store = new Store();
  store.seedMessage("m1");
  store.latencyMs = 5;
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 4 } },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  for (const d of deltas(RESPONSE)) s.push(d);
  await s.close();

  assert.equal(store.row("m1").body, RESPONSE);
  // Every chunk was strictly larger than the 4-char threshold: the slow store forced coalescing
  // instead of accumulating a queue of tiny checkpoints.
  assert.ok(store.batches.length < deltas(RESPONSE).length / 4);
});

// --------------------------------------------------------------------------- durability honesty

test("a committed-but-ack-lost checkpoint is absorbed by the read-back — no retry, no duplicate", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 8, retries: 3 }, onCheckpointError: () => {} },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });

  // The nastiest failure: the checkpoint COMMITS and then the reply is lost. The read-back finds
  // the row already at this slice's end, so the "failure" is recognized as a success on the spot —
  // no mid, no lmid, no dedup ledger, and no wasted retry.
  store.loseNextAck = true;
  s.push("0123456789");
  await s.flush();

  assert.deepEqual(store.chunks("m1"), [{ seq: 10, text: "0123456789" }], "one chunk row, not two");
  assert.equal(store.row("m1").seq, 10);
  assert.equal(s.durableSeq, 10, "the lost ack was absorbed, not mistaken for a stall");
  assert.equal(store.batches.filter((b) => b[0]?.startsWith("INSERT INTO")).length, 1, "and nothing re-ran");

  await s.close();
  assert.equal(store.row("m1").body, "0123456789");
});

test("a checkpoint that truly failed IS retried, and the replayed statements are idempotent", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 8, retries: 3 }, onCheckpointError: () => {} },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });

  store.failures = [new Error("store down")]; // attempt 1 applies nothing; attempt 2 lands
  s.push("0123456789");
  await s.flush();

  assert.deepEqual(store.chunks("m1"), [{ seq: 10, text: "0123456789" }]);
  assert.equal(store.row("m1").seq, 10);
  assert.ok(store.batches.filter((b) => b[0]?.startsWith("INSERT INTO")).length >= 2, "the retry re-ran the batch");
  await s.close();
});

test("a lost ack followed by a growing tail never corrupts the store: the next slice re-covers", async () => {
  // THE P-violation the property suite flushed out: an append commits, its ack is lost, and the
  // NEXT checkpoint spans a longer slice — a different chunk id, so `ON CONFLICT` alone cannot
  // absorb it. Pre-fix this landed an OVERLAPPING chunk row and assembled "abcdabcde".
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 4, intervalMs: 10_000, retries: 0 }, onCheckpointError: () => {} },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  store.loseNextAck = true;
  s.push("abcd"); // the threshold checkpoint commits [0,4); the ack is lost
  s.push("e");
  await s.flush();

  assert.equal(store.durableText("m1"), "abcde", "a prefix of the produced text — never a duplication");
  let at = 0;
  for (const c of store.chunks("m1")) {
    assert.equal(c.seq - c.text.length, at, "chunk rows stay contiguous through the ack loss");
    at = c.seq;
  }
  await s.close();
  assert.equal(store.row("m1").body, "abcde");
});

test("even ack loss PLUS failed read-backs cannot overlap chunks: the insert is guarded", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 4, intervalMs: 10_000, retries: 0 }, onCheckpointError: () => {} },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });

  // The full failure chain: the append commits, the ack is lost, and EVERY read-back fails — the
  // threshold round's confirm + resync, and the flush-earned re-attempt's confirm + resync — so the
  // plane still believes 0. The NEXT slice's `from` is stale; the guard makes its insert a no-op,
  // its read-back resynchronizes, and the round after that lands the real tail. At no point does
  // the store hold text the producer did not emit.
  store.loseNextAck = true;
  store.failNextQueries = 4;
  s.push("abcd");
  await assert.rejects(s.flush());
  assert.equal(store.durableText("m1"), "abcd", "committed, unacknowledged, unclaimed");

  s.push("e");
  await assert.rejects(s.flush(), /confirmed 4 of 5/, "the stale slice no-ops and reports the shortfall");
  assert.equal(s.durableSeq, 4, "…and the read-back resynchronized the plane");

  await s.flush();
  assert.equal(store.durableText("m1"), "abcde");
  let at = 0;
  for (const c of store.chunks("m1")) {
    assert.equal(c.seq - c.text.length, at, "contiguous through the whole chain");
    at = c.seq;
  }
  await s.close();
  assert.equal(store.row("m1").body, "abcde");
});

test("a stalled checkpoint is REPAIRED by the compacting close", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const errors: unknown[] = [];
  const api = serverWith(store, {
    streams: {
      checkpoint: { tables: TABLES },
      authorize: () => true,
      policy: { chars: 8, intervalMs: 10_000, retries: 0 },
      onCheckpointError: (e: unknown) => errors.push(e),
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1" }));

  // TWO failures with `retries: 0`: one for the threshold round, one for the immediate re-attempt
  // the racing flush's force earns. The checkpoint is simply lost either way.
  store.failures = [new Error("store down"), new Error("store down")];
  s.push("0123456789");
  await assert.rejects(s.flush(), /store down/);
  assert.deepEqual(store.chunks("m1"), [], "nothing was checkpointed");

  s.push("more text arrives regardless");
  await s.close(); // the store recovered; the close writes the WHOLE body

  const frames = await framesP;
  // Subscribers saw everything throughout — a durability stall is not a stream stall.
  assert.equal(producedFrom("", frames), "0123456789more text arrives regardless");
  assert.equal(store.row("m1").body, "0123456789more text arrives regardless");
  assert.equal(store.row("m1").status, "complete");
  assert.ok(errors.length > 0, "a durability failure is never silent, even when later repaired");
});

test("a close that cannot commit ends the stream honestly rather than claiming completion", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 8, intervalMs: 10_000, retries: 0 }, onCheckpointError: () => {} },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const framesP = collect(await api.subscribeStream({ user: "u1", streamId: "m1" }));

  s.push("0123456789");
  await s.flush();
  store.failures = [new Error("store down at close")];
  await assert.rejects(s.close(), /store down at close/);

  const frames = await framesP;
  assert.equal(frames[frames.length - 1].type, "end", "subscribers are always released");
  assert.equal(store.row("m1").status, "streaming", "the row is left for the sweeper, not marked complete");
  assert.equal(store.durableText("m1"), "0123456789", "the checkpointed prefix survives");
});

test("flush after a failed close rejects — it neither lies nor quietly re-runs the seal", async () => {
  const seen: string[] = [];
  let failClose = true;
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: {} as never,
    streams: {
      checkpoint: {
        commit: async (i) => {
          seen.push(i.kind);
          if (i.kind === "close" && failClose) {
            failClose = false;
            throw new Error("store down");
          }
        },
      },
      authorize: () => true,
      policy: { chars: 1000, intervalMs: 10_000, retries: 0 },
      onCheckpointError: () => {},
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("hello");
  await assert.rejects(s.close(), /store down/);

  // The stream is over. A late `push` throws; a late `flush` must not resolve (the shortfall is
  // permanent) and must not re-drive the seal either.
  assert.throws(() => s.push("x"), /closed/);
  await assert.rejects(s.flush(), /ended with only 0 of 5 characters durable/);
  assert.deepEqual(seen, ["open", "close"], "no second close was issued");
});

test("drainStreams compacts the outstanding tail, then seals `interrupted`", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("half a sentence");
  await s.flush();
  s.push(" and a tail the generator never got to finish");
  await api.drainStreams();

  const row = store.row("m1");
  // A graceful drain has time to write, so nothing PRODUCED is lost...
  assert.equal(row.body, "half a sentence and a tail the generator never got to finish");
  assert.deepEqual(store.chunks("m1"), []);
  // ...but the response was cut short, and the row says so rather than claiming completion.
  assert.equal(row.status, "interrupted");
});

test("a host killed WITHOUT a drain leaves the checkpointed chunks and a `streaming` row", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("half a sentence");
  await s.flush();
  s.push(" and a tail nobody will ever read");
  api.close(); // the ungraceful path: readers and timers dropped, no durable write

  assert.equal(store.durableText("m1"), "half a sentence", "the tail after the last checkpoint is gone");
  assert.equal(store.row("m1").status, "streaming", "and the row still says streaming — the sweeper's job (§5)");
});

// --------------------------------------------------------------------------- cancellation (§6)

test("cancel rides the checkpoint round-trip: no routing to the hosting instance", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: { checkpoint: { tables: TABLES }, authorize: () => true, policy: { chars: 8, intervalMs: 10_000 } },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });

  // The reader's cancel is an ORDINARY write to the app's own row, from wherever they are.
  store.setCancel("m1");
  assert.equal(s.cancelled, false, "not seen until the next checkpoint");

  let produced = 0;
  await s.pump(
    (async function* () {
      for (let i = 0; i < 100; i++) {
        produced++;
        yield "0123456789";
      }
    })(),
  );
  await s.close();

  assert.ok(produced < 100, `pump stopped early (after ${produced} deltas), aborting the generation`);
  assert.equal(s.cancelled, true);
  assert.equal(store.row("m1").status, "cancelled", "the seal names why it stopped");
  assert.equal(store.row("m1").body.length, s.seq, "everything produced before the stop is durable");
});

test("no cancel column mapped ⇒ no cancel SQL is ever emitted", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: {
      checkpoint: { tables: { message: "message", chunks: "message_chunk" } },
      authorize: () => true,
      policy: { chars: 4, intervalMs: 10_000 },
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  store.setCancel("m1");
  s.push("0123456789");
  await s.flush();
  assert.equal(s.cancelled, false, "cancellation is opt-in BY NAMING the column");
  await s.close();
  assert.equal(store.row("m1").status, "complete");
});

// --------------------------------------------------------------------------- the app's schema

test("openStream refuses a NULL length column — the CAS can never match it", async () => {
  // A nullable `seq` is the nastiest mapping mistake: `WHERE seq = 0` matches nothing, so every
  // chunk insert would land while its length CAS silently did nothing — and `batch` reports no row
  // count, so the plane could not otherwise notice.
  const store = new Store();
  store.db.exec("CREATE TABLE loose (id TEXT PRIMARY KEY, body TEXT, seq INTEGER, status TEXT)");
  store.db.prepare("INSERT INTO loose (id) VALUES (?)").run("m1");
  const api = serverWith(store, {
    streams: {
      checkpoint: { tables: { message: "loose", chunks: "message_chunk" } },
      authorize: () => true,
    },
  });
  await assert.rejects(api.openStream({ user: "u1", streamId: "m1" }), /must be NOT NULL DEFAULT 0/);
});

test("openStream single-flights at the ROW: a second producer is refused", async () => {
  // The kick that starts a generation is at-least-once (a retried envelope re-runs its post-commit
  // code), and the duplicate can land on another instance where the in-memory map is empty — so the
  // guard has to be the row's own status, not this process's bookkeeping.
  const store = new Store();
  store.seedMessage("m1");
  const first = serverWith(store);
  const second = serverWith(store); // a different api-server instance, same store
  const s = await first.openStream({ user: "u1", streamId: "m1" });

  await assert.rejects(
    second.openStream({ user: "u1", streamId: "m1" }),
    /is already "streaming" — another producer holds this stream/,
  );
  s.push("only one producer wrote this");
  await s.close();
  assert.equal(store.row("m1").body, "only one producer wrote this");
});

test("a lost ack on the open write does not make the retry refuse its own success", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  // The precondition is read ONCE, before the write — so the replayed `markStreaming` (which finds
  // the row already `streaming`) is absorbed rather than mistaken for a rival producer.
  store.loseNextAck = true;
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("fine");
  await s.close();
  assert.equal(store.row("m1").body, "fine");
  assert.equal(store.row("m1").status, "complete");
});

test("a mapped host column closes the open race: the loser is refused by read-back", async () => {
  // The probe and the open write are separate round trips, so bare check-then-act lets two
  // SIMULTANEOUS kicks both pass the probe. With `columns.host` the open write is conditional
  // (`… WHERE status <> 'streaming'`) and the read-back names the winner — a true CAS.
  const store = new Store();
  store.seedMessage("m1");
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: { checkpoint: { tables: HOST_TABLES }, authorize: () => true, hostId: "me" },
  });
  // The rival lands in the EXACT window between this open's probe and its write.
  store.nextBatchHook = () => {
    store.db.prepare("UPDATE message SET status = 'streaming', host = 'rival' WHERE id = 'm1'").run();
  };
  await assert.rejects(api.openStream({ user: "u1", streamId: "m1" }), /another producer won the open race/);
  const row = store.db.prepare("SELECT status, host FROM message WHERE id = 'm1'").get() as never as {
    status: string;
    host: string;
  };
  assert.equal(row.host, "rival", "the loser's conditional write matched nothing");
  assert.equal(row.status, "streaming", "the winner's claim stands");
});

test("a lost ack on the host-CAS open still absorbs: the retry reads back its own token", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    streams: { checkpoint: { tables: HOST_TABLES }, authorize: () => true, hostId: "me" },
  });
  store.loseNextAck = true;
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const row = store.db.prepare("SELECT host FROM message WHERE id = 'm1'").get() as never as { host: string };
  assert.equal(row.host, "me", "the row persists the producer's identity, for routing (§4)");
  s.push("fine");
  await s.close();
  assert.equal(store.row("m1").status, "complete");
});

test("tables mode refuses options it cannot honour, at construction", () => {
  const store = new Store();
  assert.throws(
    () =>
      createRindleApiServer<string>({
        daemon: {} as never,
        backend: store.backend,
        streams: { checkpoint: { tables: TABLES }, authorize: () => true, retainChars: 1024 },
      }),
    /retainChars is `commit`-mode only/,
  );
  assert.throws(
    () =>
      createRindleApiServer<string>({
        daemon: {} as never,
        backend: store.backend,
        // TABLES maps no host column, so a hostId would be silently dead — refused instead.
        streams: { checkpoint: { tables: TABLES }, authorize: () => true, hostId: "me" },
      }),
    /columns\.host/,
  );
});

test("openStream refuses a message row that is missing or already advanced", async () => {
  const store = new Store();
  const api = serverWith(store);
  await assert.rejects(api.openStream({ user: "u1", streamId: "ghost" }), /no row in "message"/);

  store.seedMessage("m1");
  store.db.prepare("UPDATE message SET seq = 5, body = 'hello' WHERE id = ?").run("m1");
  await assert.rejects(api.openStream({ user: "u1", streamId: "m1" }), /already holds 5 characters/);
});

test("a bad table mapping fails the deploy, not a 3am generation", () => {
  const store = new Store();
  // A REAL schema, so the validation is checked against the same shape the SQL renderer reads.
  const schema = createSchema({
    tables: [
      table("message")
        .columns({ id: string(), chatId: string(), body: string(), status: string(), seq: number() })
        .primaryKey("id"),
      table("message_chunk")
        .columns({ id: string(), streamId: string(), seq: number(), text: string() })
        .primaryKey("id"),
    ],
  });
  // A column the app never added.
  assert.throws(
    () =>
      createRindleApiServer<string>({
        daemon: {} as never,
        backend: store.backend,
        schema,
        streams: {
          checkpoint: { tables: { message: "message", chunks: "message_chunk", columns: { cancel: "cancelRequested" } } },
          authorize: () => true,
        },
      }),
    /has no column "cancelRequested" \(the cancel column\)/,
  );
  // A table that is not in the schema at all.
  assert.throws(
    () =>
      createRindleApiServer<string>({
        daemon: {} as never,
        backend: store.backend,
        schema,
        streams: { checkpoint: { tables: { message: "message", chunks: "chunkz" } }, authorize: () => true },
      }),
    /names "chunkz", which is not in the configured schema/,
  );
  // An opt-in host column the app never added.
  assert.throws(
    () =>
      createRindleApiServer<string>({
        daemon: {} as never,
        backend: store.backend,
        schema,
        streams: {
          checkpoint: { tables: { message: "message", chunks: "message_chunk", columns: { host: "host" } } },
          authorize: () => true,
        },
      }),
    /has no column "host" \(the host column\)/,
  );
  // The correct mapping constructs cleanly.
  createRindleApiServer<string>({
    daemon: {} as never,
    backend: store.backend,
    schema,
    streams: { checkpoint: { tables: { message: "message", chunks: "message_chunk" } }, authorize: () => true },
  });
});

// --------------------------------------------------------------------------- the fallback answers

test("an unknown stream answers `absent` — the store is the whole truth", async () => {
  const store = new Store();
  const api = serverWith(store);
  assert.deepEqual(await collect(await api.subscribeStream({ user: "u1", streamId: "nope" })), [{ type: "absent" }]);
});

test("a `from` below the retained floor answers `stale`, naming where the store has got to", async () => {
  // Trimming only exists in `commit`-callback mode — mapped tables retain the whole text for
  // compaction, so their floor never rises and `stale` is unreachable by construction.
  const committed: StreamCommitInput[] = [];
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: {} as never,
    streams: {
      checkpoint: { commit: async (i) => void committed.push(i) },
      authorize: () => true,
      policy: { chars: 8, intervalMs: 10_000 },
      retainChars: 0,
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("0123456789abcdef");
  await s.flush();

  assert.deepEqual(await collect(await api.subscribeStream({ user: "u1", streamId: "m1", from: 2 })), [
    { type: "stale", floorSeq: 16, durableSeq: 16 },
  ]);
  // The app was handed everything below that floor, so the client can resolve the `stale`.
  assert.equal(committed.filter((c) => c.kind === "append").map((c) => (c.kind === "append" ? c.text : "")).join(""), "0123456789abcdef");
  await s.close();
});

test("the commit escape hatch gets the whole body at close, and can answer the cancel probe", async () => {
  const seen: StreamCommitInput[] = [];
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: {} as never,
    streams: {
      checkpoint: {
        commit: async (input) => {
          seen.push(input);
          return { cancelRequested: input.kind === "append" && input.seq >= 20 };
        },
      },
      authorize: () => true,
      policy: { chars: 10, intervalMs: 10_000 },
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  await s.pump(
    (async function* () {
      for (let i = 0; i < 10; i++) yield "0123456789";
    })(),
  );
  await s.close();

  const close = seen.at(-1)!;
  assert.equal(close.kind, "close");
  assert.equal(close.kind === "close" && close.status, "cancelled");
  assert.equal(close.kind === "close" && close.body.length, s.seq, "the whole produced text, for compaction");
  assert.equal(close.kind === "close" && close.seq, s.seq);
  assert.equal(close.kind === "close" && close.bodyFrom, 0, "untrimmed: the body starts at offset 0");
});

test("close under trimming: `bodyFrom` names the retained suffix, and the recipe keeps the tail", async () => {
  // With `retainChars` trimming (commit mode only), the producer no longer holds the whole text at
  // close. The close input says so honestly: `body` is the RETAINED suffix, `bodyFrom` its absolute
  // start, and `seq === bodyFrom + body.length` — so the non-compacting recipe
  // `body.slice(from - bodyFrom)` still appends exactly the outstanding tail.
  const seen: StreamCommitInput[] = [];
  const api = createRindleApiServer<string>({
    daemon: {} as never,
    backend: {} as never,
    streams: {
      checkpoint: { commit: async (i) => void seen.push(i) },
      authorize: () => true,
      policy: { chars: 8, intervalMs: 10_000 },
      retainChars: 0,
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("0123456789abcdef");
  await s.flush(); // durable 16; retainChars 0 trims the buffer to the floor
  s.push("XYZ");
  await s.close();

  const close = seen.at(-1)!;
  assert.equal(close.kind, "close");
  if (close.kind !== "close") return;
  assert.equal(close.seq, 19);
  assert.equal(close.bodyFrom + close.body.length, close.seq, "seq === bodyFrom + body.length, always");
  assert.equal(close.body.slice(close.from - close.bodyFrom), "XYZ", "the recipe appends exactly the tail");
});

test("a sealed stream stays joinable through the linger window", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("done");
  await s.close();

  const frames = await collect(await api.subscribeStream({ user: "u1", streamId: "m1", from: 0 }));
  assert.equal(frames[0].type === "open" && frames[0].ended, true);
  assert.equal(producedFrom("", frames), "done");
  assert.equal(frames[frames.length - 1].type, "end");
});

test("a subscriber that stops reading is dropped with `stale`, never pins the producer", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: {
      checkpoint: { tables: TABLES },
      authorize: () => true,
      policy: { chars: 1_000_000, intervalMs: 10_000 },
      maxQueuedFrames: 4,
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const sub = await api.subscribeStream({ user: "u1", streamId: "m1" });
  for (let i = 0; i < 50; i++) s.push(`delta-${i} `);
  const frames = await collect(sub); // only drained NOW, long after the cap was blown
  assert.equal(frames[frames.length - 1].type, "stale");
  await s.close();
});

test("SSE honours consumer backpressure: an unread response backs up in the BOUNDED subscriber queue", async () => {
  // The SSE encoder is pull-based, so a consumer that never reads leaves the frames in the
  // subscriber's capped queue — where `maxQueuedFrames` drops it with `stale` — rather than
  // accumulating them unboundedly inside the ReadableStream.
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store, {
    streams: {
      checkpoint: { tables: TABLES },
      authorize: () => true,
      policy: { chars: 1_000_000, intervalMs: 10_000 },
      maxQueuedFrames: 4,
    },
  });
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const body = streamFramesToSse(await api.subscribeStream({ user: "u1", streamId: "m1" }), { keepAliveMs: 60_000 });

  for (let i = 0; i < 50; i++) s.push(`delta-${i} `); // nobody is reading `body` yet
  let text = "";
  const decoder = new TextDecoder();
  for await (const bytes of body as unknown as AsyncIterable<Uint8Array>) text += decoder.decode(bytes);
  assert.ok(text.includes('"type":"stale"'), "the slow reader was dropped with `stale`, not buffered forever");
  await s.close();
});

// --------------------------------------------------------------------------- authorization + wire

test("a denied subscribe is a 403 that reveals nothing about existence", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });

  for (const streamId of ["m1", "does-not-exist"]) {
    await assert.rejects(
      api.subscribeStream({ user: "intruder", streamId }),
      (e: unknown) => e instanceof RindleApiError && e.status === 403,
      `subscribing to ${streamId} as a stranger`,
    );
  }
  await s.close();
});

test("streams are 403 until configured", async () => {
  const api = createRindleApiServer<string>({ daemon: {} as never, backend: {} as never });
  await assert.rejects(api.openStream({ user: "u1", streamId: "m1" }), /streams not configured/);
  await assert.rejects(api.subscribeStream({ user: "u1", streamId: "m1" }), /streams not configured/);
  await api.drainStreams(); // a no-op, never a throw — shutdown must not care
});

test("handleStreamJson parses the default body", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("hello");
  const framesP = collect(await api.handleStreamJson({ streamId: "m1", from: 2 }, { user: "u1" }));
  await s.close();
  assert.equal(producedFrom("he", await framesP), "hello");
  await assert.rejects(api.handleStreamJson({}, { user: "u1" }), /invalid streamId/);
  // `from` must be a non-negative INTEGER — a float would yield a replay chunk violating
  // `text.length === seq - from`, the frame invariant the client splices by.
  await assert.rejects(api.handleStreamJson({ streamId: "m1", from: 2.5 }, { user: "u1" }), /invalid from/);
  await assert.rejects(api.handleStreamJson({ streamId: "m1", from: -1 }, { user: "u1" }), /invalid from/);
});

test("streamResponse: the subscribe route in one call", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  s.push("hello");

  const headers = { get: () => null };
  const res = await api.streamResponse({ url: "https://x/api/rindle/stream?streamId=m1&from=0", headers }, { user: "u1" });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("content-type"), "text/event-stream; charset=utf-8");
  const closeP = s.close();
  const text = await res.text();
  await closeP;
  assert.ok(text.includes('"type":"chunk"') && text.includes('"type":"end"'));

  // A refusal is a RESPONSE, not a throw the route forgot to catch — and the same generic body
  // whether or not the stream exists.
  for (const streamId of ["m1", "does-not-exist"]) {
    const denied = await api.streamResponse({ url: `https://x/api/rindle/stream?streamId=${streamId}`, headers }, { user: "intruder" });
    assert.equal(denied.status, 403);
    assert.equal(await denied.text(), '{"error":"rindle request forbidden"}');
  }

  // Unconfigured streams answer the same way.
  const bare = createRindleApiServer<string>({ daemon: {} as never, backend: {} as never });
  const off = await bare.streamResponse({ url: "https://x/api/rindle/stream?streamId=m1", headers }, { user: "u1" });
  assert.equal(off.status, 403);
});

test("the stream route constant pins the path the react hook mirrors by hand", () => {
  // `@rindle/react` hard-codes DEFAULT_STREAM_ENDPOINT (the browser must never pull this package);
  // each side pins the literal so either drifting fails its own suite.
  assert.equal(DEFAULT_RINDLE_API_ROUTES.stream, "/api/rindle/stream");
});

test("SSE: every positional frame carries `id: <seq>` — the browser's own resume offset", async () => {
  const store = new Store();
  store.seedMessage("m1");
  const api = serverWith(store);
  const s = await api.openStream({ user: "u1", streamId: "m1" });
  const body = streamFramesToSse(await api.subscribeStream({ user: "u1", streamId: "m1" }), { keepAliveMs: 60_000 });

  s.push("hello ");
  s.push("world");
  await s.close();

  let text = "";
  const decoder = new TextDecoder();
  for await (const bytes of body as unknown as AsyncIterable<Uint8Array>) text += decoder.decode(bytes);

  const events = text
    .split("\n\n")
    .filter((e) => e.startsWith("id:") || e.startsWith("data:"))
    .map((e) => {
      const [head, data] = e.split("\n");
      return {
        id: head.startsWith("id: ") ? Number(head.slice(4)) : undefined,
        frame: JSON.parse((data ?? head).slice(6)) as StreamFrame,
      };
    });

  assert.deepEqual(
    events.map((e) => [e.frame.type, e.id]),
    [
      ["open", 0],
      ["chunk", 6],
      ["chunk", 11],
      ["end", 11],
    ],
  );
  // Reconnecting at the last id it saw is a plain re-subscribe — no application state involved.
  const headers = { get: (n: string) => (n.toLowerCase() === "last-event-id" ? "6" : null) };
  assert.deepEqual(streamRequestFromHttp({ url: "https://x/s?streamId=m1", headers }), { streamId: "m1", from: 6 });
});

test("streamRequestFromHttp: Last-Event-ID wins over the URL's original join point", () => {
  const headers = (v: string | null) => ({ get: (n: string) => (n.toLowerCase() === "last-event-id" ? v : null) });
  assert.deepEqual(
    streamRequestFromHttp({ url: "https://x/api/rindle/stream?streamId=m1&from=10", headers: headers(null) }),
    { streamId: "m1", from: 10 },
  );
  assert.deepEqual(
    streamRequestFromHttp({ url: "https://x/api/rindle/stream?streamId=m1&from=10", headers: headers("42") }),
    { streamId: "m1", from: 42 },
  );
  assert.deepEqual(streamRequestFromHttp({ url: "https://x/api/rindle/stream", headers: headers(null) }), {
    streamId: "",
    from: 0,
  });
});

test("assembleDurableText: body first, then the chunks not yet folded into it", () => {
  assert.equal(assembleDurableText({ body: "" }, [{ seq: 3, text: "abc" }, { seq: 6, text: "def" }]), "abcdef");
  // Order is by `seq`, never by arrival.
  assert.equal(assembleDurableText({ body: "" }, [{ seq: 6, text: "def" }, { seq: 3, text: "abc" }]), "abcdef");
  // Post-compaction: body holds everything, no chunks remain.
  assert.equal(assembleDurableText({ body: "abcdef" }, []), "abcdef");
  assert.equal(assembleDurableText(null), "");
});

test("spliceStreamText merges the two planes by taking the longer prefix", () => {
  // The IVM view is behind the stream — the stream's tail wins.
  assert.equal(spliceStreamText("The quick", "The quick brown fox"), "The quick brown fox");
  // The IVM view caught up past what this subscription saw (a checkpoint the client saw first, or a
  // reconnect gap) — the store wins. Never a duplication either way.
  assert.equal(spliceStreamText("The quick brown fox", "The quick"), "The quick brown fox");
  assert.equal(spliceStreamText("", ""), "");
});
