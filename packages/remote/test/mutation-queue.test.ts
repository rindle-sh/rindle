import { test } from "node:test";
import assert from "node:assert/strict";

import type { MutationEnvelope } from "@rindle/client";

import { createQueuedMutationSender } from "../src/mutation-queue.ts";
import type { PushOutcome } from "../src/mutation-queue.ts";

const env = (mid: number, name = "m"): MutationEnvelope => ({ clientID: "c1", mid, name, args: null });

test("envelopes flush as in-order batches, never overlapping", async () => {
  const batches: number[][] = [];
  let inFlight = 0;
  const sender = createQueuedMutationSender({
    send: async (envelopes) => {
      assert.equal(inFlight, 0, "a batch left before the prior one was confirmed");
      inFlight++;
      batches.push(envelopes.map((e) => e.mid));
      await new Promise((r) => setTimeout(r, 5));
      inFlight--;
    },
  });

  const sends = [sender(env(1)), sender(env(2)), sender(env(3))];
  await Promise.all(sends);
  assert.deepEqual(batches.flat(), [1, 2, 3]);
});

test("a failed flush retries the SAME batch (mid dedup makes the replay safe)", async () => {
  const attempts: number[][] = [];
  let failures = 2;
  const sender = createQueuedMutationSender({
    retryDelayMs: () => 1,
    send: async (envelopes) => {
      attempts.push(envelopes.map((e) => e.mid));
      if (failures-- > 0) throw new Error("network down");
    },
  });

  await sender(env(1));
  assert.equal(attempts.length, 3);
  assert.deepEqual(attempts[0], attempts[1]);
  assert.deepEqual(attempts[1], attempts[2]);
});

test("rejections surface through onRejected and are not retried", async () => {
  const rejected: Array<{ name: string; reason: string }> = [];
  let calls = 0;
  const sender = createQueuedMutationSender({
    send: async (envelopes): Promise<PushOutcome[]> => {
      calls++;
      return envelopes.map((e) => (e.name === "bad" ? { accepted: false, reason: "nope" } : { accepted: true }));
    },
    onRejected: (envelope, reason) => rejected.push({ name: envelope.name, reason }),
  });

  await sender(env(1, "ok"));
  await sender(env(2, "bad"));
  await sender(env(3, "ok"));
  assert.deepEqual(rejected, [{ name: "bad", reason: "nope" }]);
  assert.equal(calls <= 3, true, "rejections must not trigger retries");
});

test("maxBatch caps a flush; later envelopes ride the next batch", async () => {
  const batches: number[][] = [];
  let release: () => void = () => {};
  const gate = new Promise<void>((r) => (release = r));
  const sender = createQueuedMutationSender({
    maxBatch: 2,
    send: async (envelopes) => {
      batches.push(envelopes.map((e) => e.mid));
      if (batches.length === 1) await gate; // hold the first flush so the rest queue up
    },
  });

  const first = sender(env(1));
  const rest = [sender(env(2)), sender(env(3)), sender(env(4))];
  release();
  await Promise.all([first, ...rest]);
  assert.deepEqual(batches.flat(), [1, 2, 3, 4]);
  for (const batch of batches) assert.equal(batch.length <= 2, true);
});
