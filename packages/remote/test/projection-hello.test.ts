// Projection (PROJECTION-SUPPORT-DESIGN.md §5.2 / §7): a projected query advertises a SUBSET
// of a table's columns in its normalized hello. The subscriber must accept that subset (a
// subsequence of the client's full column list, by name) while still rejecting a genuine
// skew — a renamed column, a reordered column, or a PK-by-name change.
//
// The reverse skew is ALSO accepted: a server WIDER than the client (a table expanded mid an
// `expand-then-contract` migration). The client's columns are a subsequence of the server's;
// the client drops the columns it doesn't yet have. Without this, an old client running a `'*'`
// query against a newer server is wrongly rejected, and `expand-then-contract` is impossible.

import { test } from "node:test";
import assert from "node:assert/strict";

import { COMPARATOR_VERSION } from "@rindle/client";
import type { NormalizedEvent, NormalizedTableSchema } from "@rindle/client";
import { NormalizedSubscriber, normalizedFp, type NormalizedHello } from "../src/normalized.ts";

// Client's full typed schema: issue(id, title, priority), pk = id.
const CLIENT_TABLES: NormalizedTableSchema[] = [
  { name: "issue", columns: ["id", "title", "priority"], primaryKey: [0] },
];

function helloFor(tables: NormalizedTableSchema[]): NormalizedHello {
  return { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables, normalizedFp: normalizedFp(tables) };
}

test("a projected-subset hello is accepted (id, priority — title dropped)", () => {
  // The server projects issue to [id, priority] (ascending base order); PK remaps to 0.
  const projected: NormalizedTableSchema[] = [{ name: "issue", columns: ["id", "priority"], primaryKey: [0] }];
  const events: NormalizedEvent[] = [];
  assert.doesNotThrow(
    () => new NormalizedSubscriber(helloFor(projected), (ev) => events.push(ev), CLIENT_TABLES),
    "a narrower projection of the client's columns must be accepted",
  );
});

test("a single projected column (just the PK) is accepted", () => {
  const projected: NormalizedTableSchema[] = [{ name: "issue", columns: ["id"], primaryKey: [0] }];
  assert.doesNotThrow(() => new NormalizedSubscriber(helloFor(projected), () => {}, CLIENT_TABLES));
});

test("a renamed column in a projection is still rejected", () => {
  const renamed: NormalizedTableSchema[] = [{ name: "issue", columns: ["id", "prio"], primaryKey: [0] }];
  assert.throws(
    () => new NormalizedSubscriber(helloFor(renamed), () => {}, CLIENT_TABLES),
    /schema|mismatch|drift/i,
  );
});

test("a reordered projection (not a subsequence) is still rejected", () => {
  // [priority, id] is not a subsequence of [id, title, priority] → a column-order skew.
  const reordered: NormalizedTableSchema[] = [{ name: "issue", columns: ["priority", "id"], primaryKey: [1] }];
  assert.throws(
    () => new NormalizedSubscriber(helloFor(reordered), () => {}, CLIENT_TABLES),
    /schema|mismatch|drift/i,
  );
});

test("a PK-by-name change in a projection is still rejected", () => {
  // Same columns, but the server claims `priority` is the PK.
  const pkSkew: NormalizedTableSchema[] = [{ name: "issue", columns: ["id", "priority"], primaryKey: [1] }];
  assert.throws(() => new NormalizedSubscriber(helloFor(pkSkew), () => {}, CLIENT_TABLES), /primary-key|drift/i);
});

// --- expand-then-contract: a server WIDER than the client is accepted (the client drops the extra) ---

test("an expanded hello with an extra TRAILING column is accepted (the client drops it)", () => {
  // The server's `issue` gained `assignee`; an old client doing `'*'` sees all four columns. Its
  // own columns [id, title, priority] are a subsequence of the server's → accept, drop `assignee`.
  const expanded: NormalizedTableSchema[] = [
    { name: "issue", columns: ["id", "title", "priority", "assignee"], primaryKey: [0] },
  ];
  assert.doesNotThrow(
    () => new NormalizedSubscriber(helloFor(expanded), () => {}, CLIENT_TABLES),
    "a server with an extra trailing column must be accepted (expand-then-contract)",
  );
});

test("an expanded hello with an extra column in the MIDDLE is accepted", () => {
  // The new column can land anywhere; the client maps by NAME, so [id, title, priority] is still
  // a subsequence of [id, assignee, title, priority].
  const expanded: NormalizedTableSchema[] = [
    { name: "issue", columns: ["id", "assignee", "title", "priority"], primaryKey: [0] },
  ];
  assert.doesNotThrow(() => new NormalizedSubscriber(helloFor(expanded), () => {}, CLIENT_TABLES));
});

test("a genuine skew (a renamed column) on a wider hello is still rejected", () => {
  // `priority` renamed to `prio` AND a new `assignee` added: neither list subsequences the other
  // (client has `priority`, server doesn't; server has `prio`/`assignee`, client doesn't) → reject.
  const skewed: NormalizedTableSchema[] = [
    { name: "issue", columns: ["id", "title", "prio", "assignee"], primaryKey: [0] },
  ];
  assert.throws(
    () => new NormalizedSubscriber(helloFor(skewed), () => {}, CLIENT_TABLES),
    /schema|mismatch|drift/i,
  );
});
