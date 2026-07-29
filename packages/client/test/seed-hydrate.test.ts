// Level A of the seed→hydrate harness (SSR-DESIGN.md §6): the Store's seed-retirement logic across
// every backend ordering/shape, driven hermetically by a scriptable backend. The retirement logic
// lives in the Store (shared by every backend), so a fake that emits the primitive event sequences
// covers the whole matrix without a real engine. Level B (real NormalizedBackend / OptimisticBackend)
// proves each backend actually EMITS these sequences — see those packages' seed tests.
//
// The invariants asserted at every step:
//   • no empty flash — `data` never goes seed → [] → live (a flash shows up as a step mismatch);
//   • no freeze — once retired, a plain incremental batch is visible in `data`;
//   • the handoff NOTIFIES — a subscriber sees the seed→tree switch (the whole point of the 0-net-muts
//     fix: retire a seed whose accompanying fold folded nothing, and force the notify);
//   • retire-once — a later mount is not re-seeded (here the fake yields empty on register, so a
//     remount reading [] rather than the seed proves the seed left the map; "remount shows LIVE data"
//     is a stateful-engine property, hence Level B);
//   • resultType tracks (unknown → complete, or complete throughout for a lifecycle-less backend).

import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, number, string, table } from "../src/schema.ts";
import { Store } from "../src/store.ts";
import { defineQuery, newQueryBuilder } from "../src/query.ts";
import { stableKey } from "../src/key.ts";
import type { Backend, ChangeEvent, FlatChange, QueryId, RemoteQuery, ResultType, WireSchema } from "../src/types.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const decksQuery = defineQuery("decks", (_a: { limit: number }) => qb.issue);

const issueWire: WireSchema = { columns: ["id", "title"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
type Row = { id: number; title: string };
const row = (id: number, title: string): Row => ({ id, title });
const add = (id: number, title: string): FlatChange => ({ path: [], op: { tag: "add", node: { row: [id, title], rels: [] } } });

/** A scriptable LIFECYCLE backend (models optimistic/normalized): `registerQuery` fires hello + an
 *  empty PRE-SYNC snapshot + `resultType=unknown` synchronously (the local engine's not-yet-synced
 *  state); the test then drives `complete`/`catchUp`/`plainBatch`/`snapshot` by hand. */
class LifecycleBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private rt: (qid: QueryId, rt: ResultType) => void = () => {};
  readonly registered: Array<{ qid: QueryId; remote?: RemoteQuery }> = [];
  registerQuery(qid: QueryId, _ast: unknown, remote?: RemoteQuery): void {
    this.registered.push({ qid, remote });
    this.handler(qid, { type: "hello", schema: issueWire, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: [], last: true }); // pre-sync LOCAL snapshot (empty)
    this.rt(qid, "unknown");
  }
  unregisterQuery(): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(h: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = h;
  }
  onResultType(h: (qid: QueryId, rt: ResultType) => void): void {
    this.rt = h;
  }
  complete(qid: QueryId): void {
    this.rt(qid, "complete");
  }
  catchUp(qid: QueryId, events: FlatChange[]): void {
    this.handler(qid, { type: "batch", events, catchUp: true });
  }
  plainBatch(qid: QueryId, events: FlatChange[]): void {
    this.handler(qid, { type: "batch", events });
  }
  snapshot(qid: QueryId, adds: FlatChange[]): void {
    this.handler(qid, { type: "snapshot", adds, last: true });
  }
}

/** A scriptable LIFECYCLE-LESS backend (models the plain ws remote): no `onResultType`, so every view
 *  stays `complete`; `registerQuery` fires hello only (the authoritative snapshot lands async). */
class PlainBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  readonly registered: Array<{ qid: QueryId; remote?: RemoteQuery }> = [];
  registerQuery(qid: QueryId, _ast: unknown, remote?: RemoteQuery): void {
    this.registered.push({ qid, remote });
    this.handler(qid, { type: "hello", schema: issueWire, comparatorVersion: 1 });
  }
  unregisterQuery(): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(h: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = h;
  }
  snapshot(qid: QueryId, adds: FlatChange[]): void {
    this.handler(qid, { type: "snapshot", adds, last: true });
  }
}

type Ctl = LifecycleBackend | PlainBackend;
interface Step {
  label: string;
  do: (be: Ctl, qid: QueryId) => void;
  data: Row[]; // expected view.data after this step (a direct read)
  rt: ResultType; // expected view.resultType after this step
  lastNotified?: Row[]; // if set, the most-recent DATA notification must equal this (the subscriber SAW it)
}
interface Case {
  name: string;
  backend: () => Ctl;
  seed?: Row[];
  steps: Step[];
  /** After the steps, a fresh mount must NOT re-flash the seed. The fake yields empty on register, so
   *  we assert `[]` (not the seed) — the retire-once invariant at the Store level. */
  noReseed?: boolean;
}

const cases: Case[] = [
  {
    // The common path: hydration flips complete, then folds the real rows as a catch-up delta.
    name: "lifecycle · non-empty catchUp hydration, then an incremental update stays visible",
    backend: () => new LifecycleBackend(),
    seed: [row(1, "seeded")],
    steps: [
      { label: "complete before data", do: (be, q) => (be as LifecycleBackend).complete(q), data: [row(1, "seeded")], rt: "complete" },
      {
        label: "catchUp folds the live rows",
        do: (be, q) => (be as LifecycleBackend).catchUp(q, [add(2, "a"), add(3, "b")]),
        data: [row(2, "a"), row(3, "b")],
        rt: "complete",
        lastNotified: [row(2, "a"), row(3, "b")],
      },
      {
        label: "a post-hydration batch is visible (NOT frozen on the seed)",
        do: (be, q) => (be as LifecycleBackend).plainBatch(q, [add(4, "c")]),
        data: [row(2, "a"), row(3, "b"), row(4, "c")],
        rt: "complete",
        lastNotified: [row(2, "a"), row(3, "b"), row(4, "c")],
      },
    ],
    noReseed: true,
  },
  {
    // Case 1: the authoritative answer is 0 rows. The empty catchUp must still retire the seed AND
    // notify — else the view freezes showing the stale seeded rows.
    name: "lifecycle · 0-row authoritative result (empty catchUp) retires + notifies",
    backend: () => new LifecycleBackend(),
    seed: [row(1, "seeded")],
    steps: [
      { label: "complete", do: (be, q) => (be as LifecycleBackend).complete(q), data: [row(1, "seeded")], rt: "complete" },
      { label: "empty catchUp", do: (be, q) => (be as LifecycleBackend).catchUp(q, []), data: [], rt: "complete", lastNotified: [] },
    ],
    noReseed: true,
  },
  {
    // Case 2: the query's whole result is already in `top` (folded via a sibling while unknown), so
    // its own hydration yields ZERO net muts — an empty catchUp. Retiring must reveal `top` and notify.
    name: "lifecycle · 0-net-muts (rows already in top) retires + reveals the tree",
    backend: () => new LifecycleBackend(),
    seed: [row(1, "seeded")],
    steps: [
      {
        label: "a sibling-driven batch populates top while still unknown (seed masks it)",
        do: (be, q) => (be as LifecycleBackend).plainBatch(q, [add(9, "shared")]),
        data: [row(1, "seeded")],
        rt: "unknown",
        lastNotified: [row(1, "seeded")],
      },
      { label: "complete", do: (be, q) => (be as LifecycleBackend).complete(q), data: [row(1, "seeded")], rt: "complete" },
      {
        label: "empty catchUp retires → top is revealed",
        do: (be, q) => (be as LifecycleBackend).catchUp(q, []),
        data: [row(9, "shared")],
        rt: "complete",
        lastNotified: [row(9, "shared")],
      },
    ],
    noReseed: true,
  },
  {
    // The gate is load-bearing: a data event that arrives BEFORE `complete` must NOT retire the seed
    // early (the backend contract is complete-first). A later hydration event while complete recovers.
    name: "lifecycle · data-before-complete does not retire early; a later snapshot recovers",
    backend: () => new LifecycleBackend(),
    seed: [row(1, "seeded")],
    steps: [
      {
        label: "catchUp while still unknown — folds into top but seed is KEPT (gate skips)",
        do: (be, q) => (be as LifecycleBackend).catchUp(q, [add(2, "a")]),
        data: [row(1, "seeded")],
        rt: "unknown",
      },
      { label: "complete — no retiring event yet, so the seed still shows", do: (be, q) => (be as LifecycleBackend).complete(q), data: [row(1, "seeded")], rt: "complete" },
      {
        label: "a snapshot while complete retires (its adds are already in top → forced notify)",
        do: (be, q) => (be as LifecycleBackend).snapshot(q, [add(2, "a")]),
        data: [row(2, "a")],
        rt: "complete",
        lastNotified: [row(2, "a")],
      },
    ],
    noReseed: true,
  },
  {
    // Lifecycle-less (plain ws): the view is `complete` from creation and retires on the FIRST
    // snapshot — the pre-fix behavior, unchanged.
    name: "lifecycle-less · seed retires on the first snapshot",
    backend: () => new PlainBackend(),
    seed: [row(1, "seeded")],
    steps: [
      {
        label: "the authoritative snapshot lands",
        do: (be, q) => (be as PlainBackend).snapshot(q, [add(2, "a")]),
        data: [row(2, "a")],
        rt: "complete",
        lastNotified: [row(2, "a")],
      },
    ],
    noReseed: true,
  },
  {
    // Control: a lifecycle query with NO seed must be undisturbed by the retirement path.
    name: "lifecycle · no seed — normal hydration, nothing to retire",
    backend: () => new LifecycleBackend(),
    steps: [
      { label: "complete", do: (be, q) => (be as LifecycleBackend).complete(q), data: [], rt: "complete" },
      {
        label: "catchUp folds rows",
        do: (be, q) => (be as LifecycleBackend).catchUp(q, [add(2, "a")]),
        data: [row(2, "a")],
        rt: "complete",
        lastNotified: [row(2, "a")],
      },
    ],
  },
];

for (const c of cases) {
  test(`seed→hydrate: ${c.name}`, () => {
    const be = c.backend();
    const store = new Store(schema, be);
    const q = decksQuery({ limit: 10 });
    if (c.seed) store.hydrate({ [stableKey(q.ast())]: { rows: c.seed, cvMin: 0 } });

    const view = store.materialize(q);
    const qid = be.registered[0].qid;

    // Record every DATA notification, then zero out the immediate replay `subscribe` fires on attach —
    // so `lastNotified` reflects only notifications caused by the driven steps.
    const notified: Row[][] = [];
    view.subscribe((d) => notified.push([...(d as readonly Row[])]));
    notified.length = 0;

    // Baseline after materialize: a seeded query shows its seed (lifecycle → PENDING, lifecycle-less →
    // complete); an unseeded one is empty.
    assert.deepStrictEqual(view.data, c.seed ?? [], `${c.name}: initial data`);

    for (const s of c.steps) {
      s.do(be, qid);
      assert.deepStrictEqual(view.data as readonly Row[], s.data, `${c.name} › ${s.label}: data`);
      assert.equal(view.resultType, s.rt, `${c.name} › ${s.label}: resultType`);
      if (s.lastNotified !== undefined) {
        assert.deepStrictEqual(notified.at(-1), s.lastNotified, `${c.name} › ${s.label}: subscriber saw the handoff`);
      }
    }

    if (c.noReseed) {
      assert.deepStrictEqual(store.materialize(q).data, [], `${c.name}: a later mount is not re-seeded`);
    }
  });
}
