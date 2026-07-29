import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { Publisher, ProtocolError, schemaFp, Subscriber } from "../src/protocol.ts";
import type { Batch } from "../src/protocol.ts";
import { COMPARATOR_VERSION } from "@rindle/client";
import type { ChangeEvent, FlatChange, WireSchema } from "@rindle/client";

const schema: WireSchema = {
  columns: ["id", "val"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [],
};

const add = (id: number, val: number): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, val], rels: [] } },
});

/** Open a Subscriber against a Publisher's hello; collect the ChangeEvents it emits. */
function open(pub: Publisher): { sub: Subscriber; events: ChangeEvent[] } {
  const events: ChangeEvent[] = [];
  const sub = new Subscriber(pub.hello(), (ev) => events.push(ev));
  return { sub, events };
}

test("schemaFp matches the engine fingerprint over Rust-dumped vectors", () => {
  const vectors = JSON.parse(
    readFileSync(new URL("./fixtures/schema-fp-vectors.json", import.meta.url), "utf8"),
  ) as Array<{ schema: WireSchema; fp: string }>;
  assert.ok(vectors.length >= 4);
  for (const v of vectors) {
    assert.strictEqual(schemaFp(v.schema), v.fp, `fp mismatch for ${JSON.stringify(v.schema.columns)}`);
  }
});

test("open emits the hello ChangeEvent; snapshot + in-order commits emit snapshot/batch", () => {
  const pub = new Publisher(1, schema);
  const { sub, events } = open(pub);
  assert.deepStrictEqual(events[0], { type: "hello", schema, comparatorVersion: COMPARATOR_VERSION });

  assert.strictEqual(sub.apply(pub.snapshot([add(1, 10), add(2, 20)])), "applied");
  assert.deepStrictEqual(events[1], { type: "snapshot", adds: [add(1, 10), add(2, 20)], last: true });

  const b1 = pub.commit([add(3, 30)])!;
  assert.strictEqual(b1.seq, 1);
  assert.strictEqual(sub.apply(b1), "applied");
  assert.deepStrictEqual(events[2], { type: "batch", events: [add(3, 30)] });
});

test("open hard-rejects a comparator-version mismatch", () => {
  const pub = new Publisher(1, schema);
  assert.throws(
    () => new Subscriber({ ...pub.hello(), comparatorVersion: 99 }, () => {}),
    (e: unknown) => e instanceof ProtocolError && e.kind === "comparator-mismatch",
  );
});

test("open rejects a forged schema fingerprint", () => {
  const pub = new Publisher(1, schema);
  assert.throws(
    () => new Subscriber({ ...pub.hello(), schemaFp: "deadbeefdeadbeef" }, () => {}),
    (e: unknown) => e instanceof ProtocolError && e.kind === "schema-mismatch",
  );
});

test("a gap (skipped seq) throws", () => {
  const pub = new Publisher(1, schema);
  const { sub } = open(pub);
  sub.apply(pub.snapshot([add(1, 10)])); // seq 0, expected next = 1
  const b2: Batch = { ...pub.commit([add(2, 20)])!, seq: 2 }; // fabricate seq 2 (1 was lost)
  assert.throws(
    () => sub.apply(b2),
    (e: unknown) => e instanceof ProtocolError && e.kind === "gap",
  );
});

test("a duplicate (re-delivered seq) is discarded, emitting nothing", () => {
  const pub = new Publisher(1, schema);
  const { sub, events } = open(pub);
  sub.apply(pub.snapshot([add(1, 10)])); // seq 0
  const b1 = pub.commit([add(2, 20)])!; // seq 1
  assert.strictEqual(sub.apply(b1), "applied");
  const before = events.length;
  assert.strictEqual(sub.apply(b1), "duplicate"); // re-deliver seq 1
  assert.strictEqual(events.length, before, "a duplicate emits no ChangeEvent");
});

test("a stale-epoch batch throws epoch-mismatch", () => {
  const pub = new Publisher(2, schema);
  const { sub } = open(pub);
  sub.apply(pub.snapshot([add(1, 10)]));
  const stale: Batch = { epoch: 1, seq: 1, schemaFp: schemaFp(schema), events: [add(2, 20)] };
  assert.throws(
    () => sub.apply(stale),
    (e: unknown) => e instanceof ProtocolError && e.kind === "epoch-mismatch",
  );
});

test("an incremental batch before the snapshot is a gap", () => {
  const pub = new Publisher(1, schema);
  const { sub } = open(pub);
  const early = pub.snapshot([]); // seq 0
  // Pretend seq 0 was lost: deliver seq 1 first.
  const b1: Batch = { ...pub.commit([add(9, 90)])!, seq: 1 };
  void early;
  assert.throws(
    () => sub.apply(b1),
    (e: unknown) => e instanceof ProtocolError && e.kind === "gap",
  );
});
