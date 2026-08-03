import assert from "node:assert/strict";
import test from "node:test";

import { DaemonHttpError } from "@rindle/daemon-client";

import {
  MAX_EVENTS_PER_TRANSACTION,
  MAX_SQL_STATEMENTS_PER_EVENT,
  MAX_SQL_STATEMENTS_PER_TRANSACTION,
  createSyntheticSource,
  createWikimediaSource,
} from "../src/sources.ts";
import type { WikiEvent } from "../src/schema.ts";

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
}

function deferred(): Deferred {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function waitFor(check: () => boolean, label: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!check()) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

function sseRecord(n: number): string {
  const change = {
    id: n,
    type: "edit",
    namespace: 0,
    title: `Page ${n}`,
    timestamp: n,
    user: "Editor",
    bot: false,
    server_name: process.env.WIKI_DOMAIN ?? "en.wikipedia.org",
    wiki: "enwiki",
    revision: { new: n },
    length: { old: 10, new: 11 },
  };
  return `id: offset-${n}\ndata: ${JSON.stringify(change)}\n\n`;
}

interface ControlledConnection {
  push: (...ids: number[]) => void;
  close: () => void;
}

function controlledFetch(): {
  fetch: typeof fetch;
  calls: { url: string; lastEventId: string | null }[];
  connections: ControlledConnection[];
} {
  const calls: { url: string; lastEventId: string | null }[] = [];
  const connections: ControlledConnection[] = [];
  const encoder = new TextEncoder();

  const fetchImpl = (async (input: string | URL | Request, init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    calls.push({ url: String(input), lastEventId: headers.get("Last-Event-ID") });
    let streamController!: ReadableStreamDefaultController<Uint8Array>;
    let ended = false;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        streamController = controller;
      },
      cancel() {
        ended = true;
      },
    });
    const connection: ControlledConnection = {
      push(...ids) {
        if (ended) throw new Error("cannot push to a closed SSE connection");
        streamController.enqueue(encoder.encode(ids.map(sseRecord).join("")));
      },
      close() {
        if (ended) return;
        ended = true;
        streamController.close();
      },
    };
    connections.push(connection);
    init?.signal?.addEventListener(
      "abort",
      () => {
        if (ended) return;
        ended = true;
        streamController.error(new DOMException("aborted", "AbortError"));
      },
      { once: true },
    );
    return new Response(stream, { status: 200 });
  }) as typeof fetch;

  return { fetch: fetchImpl, calls, connections };
}

function ids(events: readonly WikiEvent[]): string[] {
  return events.map((event) => event.edit.id);
}

test("production source transactions stay below the HCTree change ceiling", () => {
  assert.equal(MAX_EVENTS_PER_TRANSACTION, 1_000);
  assert.equal(MAX_SQL_STATEMENTS_PER_EVENT, 3);
  assert.equal(
    MAX_SQL_STATEMENTS_PER_TRANSACTION,
    MAX_EVENTS_PER_TRANSACTION * MAX_SQL_STATEMENTS_PER_EVENT,
  );
  assert.ok(MAX_SQL_STATEMENTS_PER_TRANSACTION < 8_192);
});

test("wikimedia batches are bounded, ordered, and strictly single-flight", async () => {
  const sse = controlledFetch();
  const pending: Deferred[] = [];
  const emitted: string[][] = [];
  const offsets: string[] = [];
  let active = 0;
  let maxActive = 0;
  const source = createWikimediaSource({
    fetch: sse.fetch,
    flushMs: 60_000,
    maxEventsPerTransaction: 2,
    maxBufferedEvents: 4,
  });
  const stop = source.start(
    async (events) => {
      active++;
      maxActive = Math.max(maxActive, active);
      emitted.push(ids(events));
      const gate = deferred();
      pending.push(gate);
      try {
        await gate.promise;
      } finally {
        active--;
      }
    },
    { onOffset: (offset) => offsets.push(offset) },
  );

  await waitFor(() => sse.connections.length === 1, "the SSE connection");
  sse.connections[0].push(1, 2, 3, 4, 5);
  await waitFor(() => emitted.length === 1, "the first batch");
  assert.deepEqual(emitted[0], ["1", "2"]);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(emitted.length, 1, "a second emit must not overlap the blocked first emit");

  pending[0].resolve();
  await waitFor(() => emitted.length === 2 && offsets.length === 1, "the second batch");
  assert.deepEqual(emitted[1], ["3", "4"]);
  assert.deepEqual(offsets, ["offset-2"]);

  pending[1].resolve();
  await waitFor(() => emitted.length === 3 && offsets.length === 2, "the final batch");
  assert.deepEqual(emitted[2], ["5"]);
  assert.deepEqual(offsets, ["offset-2", "offset-4"]);
  pending[2].resolve();
  await waitFor(() => offsets.length === 3, "the final offset");
  await stop();

  assert.equal(maxActive, 1);
  assert.deepEqual(emitted.flat(), ["1", "2", "3", "4", "5"]);
  assert.deepEqual(offsets, ["offset-2", "offset-4", "offset-5"]);
  assert.ok(emitted.every((batch) => batch.length <= 2));
});

test("wikimedia reconnects only from the latest successfully applied offset", async () => {
  const sse = controlledFetch();
  const firstEmit = deferred();
  const offsets: string[] = [];
  let emits = 0;
  const source = createWikimediaSource({
    fetch: sse.fetch,
    flushMs: 60_000,
    maxEventsPerTransaction: 2,
    initialReconnectBackoffMs: 0,
    maxReconnectBackoffMs: 0,
  });
  const stop = source.start(
    async () => {
      emits++;
      await firstEmit.promise;
    },
    { onOffset: (offset) => offsets.push(offset) },
  );

  await waitFor(() => sse.connections.length === 1, "the first SSE connection");
  sse.connections[0].push(1, 2);
  sse.connections[0].close();
  await waitFor(() => emits === 1, "the blocked transaction");
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(sse.calls.length, 1, "must not reconnect past an in-flight transaction");

  firstEmit.resolve();
  await waitFor(() => sse.calls.length === 2, "the reconnect");
  assert.deepEqual(offsets, ["offset-2"]);
  assert.equal(sse.calls[1].lastEventId, "offset-2");
  await stop();
});

test("retryable failures retain and retry the same head batch", async () => {
  const sse = controlledFetch();
  const attempts: string[][] = [];
  const offsets: string[] = [];
  const fatals: Error[] = [];
  const source = createWikimediaSource({
    fetch: sse.fetch,
    maxEventsPerTransaction: 1,
    maxEmitAttempts: 3,
    initialEmitRetryMs: 1,
    maxEmitRetryMs: 1,
  });
  const stop = source.start(
    async (events) => {
      attempts.push(ids(events));
      if (attempts.length < 3) throw new Error("master temporarily unavailable");
    },
    { onOffset: (offset) => offsets.push(offset), onFatal: (error) => fatals.push(error) },
  );

  await waitFor(() => sse.connections.length === 1, "the SSE connection");
  sse.connections[0].push(1);
  await waitFor(() => offsets.length === 1, "the retried transaction");
  assert.deepEqual(attempts, [["1"], ["1"], ["1"]]);
  assert.deepEqual(offsets, ["offset-1"]);
  assert.deepEqual(fatals, []);
  await stop();
});

test("exhausted retries become one visible terminal failure", async () => {
  const sse = controlledFetch();
  const fatals: Error[] = [];
  let attempts = 0;
  const source = createWikimediaSource({
    fetch: sse.fetch,
    maxEventsPerTransaction: 1,
    maxEmitAttempts: 3,
    initialEmitRetryMs: 1,
    maxEmitRetryMs: 1,
  });
  const stop = source.start(
    async () => {
      attempts++;
      throw new Error("master remains unavailable");
    },
    { onFatal: (error) => fatals.push(error) },
  );

  await waitFor(() => sse.connections.length === 1, "the SSE connection");
  sse.connections[0].push(1);
  await waitFor(() => fatals.length === 1, "the terminal callback");
  await assert.rejects(stop(), /retry budget exhausted after 3 attempts/);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(attempts, 3, "a terminal source must not keep retrying in the background");
  assert.equal(fatals.length, 1, "the terminal callback fires once");
});

test("definite daemon 4xx errors fail immediately without retry", async () => {
  const sse = controlledFetch();
  const fatals: Error[] = [];
  let attempts = 0;
  const source = createWikimediaSource({
    fetch: sse.fetch,
    maxEventsPerTransaction: 1,
    maxEmitAttempts: 8,
    initialEmitRetryMs: 1,
  });
  const stop = source.start(
    async () => {
      attempts++;
      throw new DaemonHttpError(400, "Bad Request", "invalid SQL");
    },
    { onFatal: (error) => fatals.push(error) },
  );

  await waitFor(() => sse.connections.length === 1, "the SSE connection");
  sse.connections[0].push(1);
  await waitFor(() => fatals.length === 1, "the terminal callback");
  await assert.rejects(stop(), /non-retryable emit failure/);
  assert.equal(attempts, 1);
});

test("stop waits for the in-flight transaction and its offset", async () => {
  const sse = controlledFetch();
  const emitGate = deferred();
  const offsets: string[] = [];
  let emits = 0;
  const source = createWikimediaSource({ fetch: sse.fetch, maxEventsPerTransaction: 1 });
  const stop = source.start(
    async () => {
      emits++;
      await emitGate.promise;
    },
    { onOffset: (offset) => offsets.push(offset) },
  );

  await waitFor(() => sse.connections.length === 1, "the SSE connection");
  sse.connections[0].push(1);
  await waitFor(() => emits === 1, "the in-flight transaction");
  let stopped = false;
  const stopping = stop().then(() => {
    stopped = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(stopped, false, "stop must not resolve while an accepted event is in flight");

  emitGate.resolve();
  await stopping;
  assert.deepEqual(offsets, ["offset-1"]);
});

test("synthetic generation also waits for its one in-flight transaction", async () => {
  const firstEmit = deferred();
  let emits = 0;
  const source = createSyntheticSource({ intervalMs: 1 });
  const stop = source.start(async () => {
    emits++;
    await firstEmit.promise;
  });

  await waitFor(() => emits === 1, "the first synthetic transaction");
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(emits, 1, "synthetic generation must pause behind its in-flight transaction");

  const stopping = stop();
  firstEmit.resolve();
  await stopping;
  assert.equal(emits, 1, "stop prevents the next synthetic group before draining");
});
