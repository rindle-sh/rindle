// Retryable-vs-terminal `queryError` (101-QUERY-ERRORS §5, the FOLLOWER-LAG-SHED §6.3 client
// contract): a retryable error keeps the subscription state AND the already-delivered rows,
// then re-subscribes on a backoff timer honoring `retryAfterMs`; a terminal (or unclassified)
// error stays terminal — today's behavior, pinned.

import { mock, test } from "node:test";
import assert from "node:assert/strict";

import { COMPARATOR_VERSION } from "@rindle/client";

import type { ClientMsg, ServerMsg } from "../src/protocol.ts";
import { normalizedFp } from "../src/normalized.ts";
import { RemoteNormalizedSource } from "../src/remote-source.ts";
import { RemoteOptimisticSource } from "../src/optimistic-source.ts";
import { retryDelayMs } from "../src/query-error.ts";
import type { Transport } from "../src/transport.ts";

const tables = [{ name: "issue", columns: ["id", "title"], primaryKey: [0] }];
const fp = normalizedFp(tables);

class TestTransport implements Transport {
  sent: ClientMsg[] = [];
  private handler: (msg: ServerMsg) => void = () => {};

  send(msg: ClientMsg): void {
    this.sent.push(msg);
  }

  onMessage(handler: (msg: ServerMsg) => void): void {
    this.handler = handler;
  }

  emit(msg: ServerMsg): void {
    this.handler(msg);
  }

  close(): void {}

  subscribes(): ClientMsg[] {
    return this.sent.filter((m) => m.t === "subscribe");
  }
}

function hello(queryId: number, epoch: number): ServerMsg {
  return { t: "nhello", queryId, hello: { epoch, comparatorVersion: COMPARATOR_VERSION, tables, normalizedFp: fp } };
}

test("a retryable queryError re-subscribes after the backoff and keeps delivered rows", () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const transport = new TestTransport();
    const source = new RemoteNormalizedSource(transport);
    source.expectClientSchema(tables);
    const events: unknown[] = [];
    source.onNormalized((_qid, ev) => events.push(ev));

    source.registerQuery(7, { name: "issues", args: {} });
    transport.emit(hello(7, 1));
    transport.emit({
      t: "nbatch",
      queryId: 7,
      batch: { epoch: 1, seq: 0, normalizedFp: fp, ops: [] },
    });
    const eventsBeforeError = events.length;
    assert.equal(transport.subscribes().length, 1);

    // The follower sheds (FOLLOWER-LAG-SHED §6.2): a structured retryable error.
    transport.emit({
      t: "queryError",
      queryId: 7,
      message: "follower shedding load",
      code: "shed",
      retryable: true,
      retryAfterMs: 1_000,
    });

    // No clearing event reached the app — the rows stay rendered (101 §6)…
    assert.equal(events.length, eventsBeforeError, "no event may clear the frozen view");
    // …and nothing is sent until the backoff elapses (the server's floor is honored).
    assert.equal(transport.subscribes().length, 1);
    mock.timers.tick(999);
    assert.equal(transport.subscribes().length, 1, "must not retry before retryAfterMs");
    // The jitter adds up to 25 % on top of the floor.
    mock.timers.tick(300);
    assert.equal(transport.subscribes().length, 2, "the retry re-subscribed after the backoff");

    // Recovery: a fresh hello + seq-0 snapshot under a new epoch heals the stream and resets
    // the backoff (the next retryable error starts from the floor again).
    transport.emit(hello(7, 2));
    transport.emit({
      t: "nbatch",
      queryId: 7,
      batch: { epoch: 2, seq: 0, normalizedFp: fp, ops: [] },
    });
    assert.ok(events.length > eventsBeforeError, "the recovery snapshot flowed through");
  } finally {
    mock.timers.reset();
  }
});

test("consecutive retryable errors back off exponentially until a hello resets it", () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const transport = new TestTransport();
    const source = new RemoteNormalizedSource(transport);
    source.expectClientSchema(tables);
    source.onNormalized(() => {});
    source.registerQuery(7, { name: "issues", args: {} });

    const shed = (): void =>
      transport.emit({
        t: "queryError",
        queryId: 7,
        message: "still shedding",
        code: "shed",
        retryable: true,
        retryAfterMs: 1_000,
      });

    // attempt 0: fires within [1000, 1250]ms
    shed();
    mock.timers.tick(1_250);
    assert.equal(transport.subscribes().length, 2);
    // attempt 1: doubled — nothing before 2000ms.
    shed();
    mock.timers.tick(1_999);
    assert.equal(transport.subscribes().length, 2, "second retry must wait ≥ 2× the floor");
    mock.timers.tick(501);
    assert.equal(transport.subscribes().length, 3);

    // A successful hello resets the exponent: the next error waits the floor again.
    transport.emit(hello(7, 2));
    shed();
    mock.timers.tick(1_250);
    assert.equal(transport.subscribes().length, 4, "backoff reset after recovery");
  } finally {
    mock.timers.reset();
  }
});

test("a terminal (or unclassified) queryError stays terminal", () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const transport = new TestTransport();
    const source = new RemoteNormalizedSource(transport);
    source.expectClientSchema(tables);
    source.onNormalized(() => {});
    source.registerQuery(7, { name: "issues", args: {} });

    // Unclassified (an older server / a genuine rejection): terminal — pinned behavior.
    transport.emit({ t: "queryError", queryId: 7, message: "no such named query" });
    mock.timers.tick(60_000);
    assert.equal(transport.subscribes().length, 1, "no retry for a terminal error");

    // Explicitly non-retryable is likewise terminal.
    const t2 = new TestTransport();
    const s2 = new RemoteNormalizedSource(t2);
    s2.expectClientSchema(tables);
    s2.onNormalized(() => {});
    s2.registerQuery(9, { name: "issues", args: {} });
    t2.emit({ t: "queryError", queryId: 9, message: "unauthorized", code: "unauthorized", retryable: false });
    mock.timers.tick(60_000);
    assert.equal(t2.subscribes().length, 1);
  } finally {
    mock.timers.reset();
  }
});

test("the optimistic source heals a retryable error through the same path", () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const transport = new TestTransport();
    const source = new RemoteOptimisticSource(transport, "client-1");
    source.expectClientSchema(tables);
    source.onNormalized(() => {});
    source.registerQuery(3, { name: "issues", args: {} });
    assert.equal(transport.subscribes().length, 1);

    // The existing worker-fault frame, now classified retryable by the server.
    transport.emit({
      t: "queryError",
      queryId: 3,
      message: "query faulted (re-subscribe): worker 0 restarted",
      code: "faulted",
      retryable: true,
      retryAfterMs: 250,
    });
    mock.timers.tick(400);
    assert.equal(transport.subscribes().length, 2, "the optimistic source re-subscribed");

    // Unsubscribing cancels a pending retry: no stray subscribe after removal.
    transport.emit({
      t: "queryError",
      queryId: 3,
      message: "query faulted (re-subscribe): worker 0 restarted",
      code: "faulted",
      retryable: true,
      retryAfterMs: 250,
    });
    source.unregisterQuery(3);
    mock.timers.tick(60_000);
    assert.equal(transport.subscribes().length, 2, "no retry may outlive the subscription");
  } finally {
    mock.timers.reset();
  }
});

test("retryDelayMs honors the server floor, backs off, jitters upward only, and caps", () => {
  for (let attempt = 0; attempt < 12; attempt++) {
    const d = retryDelayMs(attempt, 1_000);
    const floor = Math.min(1_000 * 2 ** attempt, 30_000);
    assert.ok(d >= floor, `attempt ${attempt}: ${d} < floor ${floor}`);
    assert.ok(d <= floor * 1.25, `attempt ${attempt}: ${d} > floor+jitter ${floor * 1.25}`);
  }
  // No server suggestion: a sane default floor.
  const d0 = retryDelayMs(0, undefined);
  assert.ok(d0 >= 500 && d0 <= 625, `default floor: ${d0}`);
});

test("a retryable lease_expired re-RESOLVES the lease — the retry carries a fresh token", () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const transport = new TestTransport();
    let resolutions = 0;
    const source = new RemoteNormalizedSource(transport, {
      resolveSubscribe: () => ({ leaseToken: `lease-${++resolutions}` }),
    });
    source.expectClientSchema(tables);
    source.onNormalized(() => {});
    source.registerQuery(7, { name: "issues", args: {} });
    assert.equal(resolutions, 1);
    assert.deepStrictEqual(transport.subscribes().at(-1), {
      t: "subscribe",
      queryId: 7,
      leaseToken: "lease-1",
      mode: "normalized",
    });

    // The daemon reclaimed the lease (a shed/bounce sweep) between resolution and subscribe:
    // 101 §5 classifies it retryable, and the client's retry must re-run the resolver — the
    // dead token would just fail again.
    transport.emit({
      t: "queryError",
      queryId: 7,
      message: "unknown lease",
      code: "lease_expired",
      retryable: true,
      retryAfterMs: 250,
    });
    mock.timers.tick(400);
    assert.equal(resolutions, 2, "the retry re-resolved the lease");
    assert.deepStrictEqual(transport.subscribes().at(-1), {
      t: "subscribe",
      queryId: 7,
      leaseToken: "lease-2",
      mode: "normalized",
    });
  } finally {
    mock.timers.reset();
  }
});
