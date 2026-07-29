// stableClientID: a per-origin base (localStorage) + a per-tab suffix (sessionStorage) + a per-load
// instance suffix, so neither two tabs NOR two clients in one tab share a clientID (and thus never
// collide on the server's per-clientID mid sequence) while a reload within a tab keeps its identity.

import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  resetStableClientID,
  resetTabInstanceForTests,
  sessionTicketPersistence,
  stableClientID,
} from "../src/client-id.ts";

/** A minimal in-memory Storage, enough for the localStorage/sessionStorage reads stableClientID does. */
function fakeStorage(): Storage {
  const map = new Map<string, string>();
  return {
    get length() {
      return map.size;
    },
    clear: () => map.clear(),
    getItem: (k: string) => (map.has(k) ? (map.get(k) as string) : null),
    key: (i: number) => [...map.keys()][i] ?? null,
    removeItem: (k: string) => void map.delete(k),
    setItem: (k: string, v: string) => void map.set(k, String(v)),
  } satisfies Storage;
}

type Globals = { localStorage?: Storage; sessionStorage?: Storage };

/** Run `fn` with the given storage areas installed on globalThis, restoring the originals after. */
function withStorage<T>(areas: Globals, fn: () => T): T {
  const g = globalThis as Globals;
  const prev = { localStorage: g.localStorage, sessionStorage: g.sessionStorage };
  g.localStorage = areas.localStorage;
  g.sessionStorage = areas.sessionStorage;
  try {
    return fn();
  } finally {
    g.localStorage = prev.localStorage;
    g.sessionStorage = prev.sessionStorage;
  }
}

/** The per-origin base = everything before the final `-` (the base is a hyphenated UUID; the tab and
 *  instance suffixes never contain a `-`). */
const baseOf = (id: string) => id.slice(0, id.lastIndexOf("-"));

// Each test starts as a fresh page load (instance counter at 0).
beforeEach(() => resetTabInstanceForTests());

test("a reload within one tab keeps the same clientID", () => {
  const local = fakeStorage();
  const session = fakeStorage();
  const first = withStorage({ localStorage: local, sessionStorage: session }, stableClientID);
  resetTabInstanceForTests(); // a reload resets the in-memory instance counter
  const second = withStorage({ localStorage: local, sessionStorage: session }, stableClientID);
  assert.equal(first, second, "same tab + same origin, across a reload => stable id");
  assert.match(first, /-/, "id is base-suffix");
});

test("two clients in ONE tab (one load) get distinct clientIDs sharing base+tab", () => {
  const local = fakeStorage();
  const session = fakeStorage();
  const a = withStorage({ localStorage: local, sessionStorage: session }, stableClientID);
  const b = withStorage({ localStorage: local, sessionStorage: session }, stableClientID); // no reset: same load
  assert.notEqual(a, b, "two client instances in one tab must not share a mid stream");
  assert.equal(baseOf(a), baseOf(b), "but they share the per-origin base");
  assert.ok(b.startsWith(a), "the extra instance is the bare id plus an instance suffix");
});

test("two tabs on one origin get distinct clientIDs sharing a base", () => {
  const local = fakeStorage(); // shared across tabs (same origin)
  const tabA = withStorage({ localStorage: local, sessionStorage: fakeStorage() }, stableClientID);
  resetTabInstanceForTests(); // each tab is its own page load
  const tabB = withStorage({ localStorage: local, sessionStorage: fakeStorage() }, stableClientID);
  assert.notEqual(tabA, tabB, "distinct tabs => distinct ids (no mid-stream collision)");
  assert.equal(baseOf(tabA), baseOf(tabB), "but they share the per-origin base");
});

test("without localStorage every call is a fresh random id", () => {
  withStorage({ localStorage: undefined, sessionStorage: undefined }, () => {
    assert.notEqual(stableClientID(), stableClientID());
  });
});

test("localStorage but no sessionStorage still yields distinct per-load ids on a shared base", () => {
  const local = fakeStorage();
  withStorage({ localStorage: local, sessionStorage: undefined }, () => {
    const a = stableClientID();
    const b = stableClientID();
    assert.notEqual(a, b, "no sessionStorage => fresh suffix each load");
    assert.equal(baseOf(a), baseOf(b), "base persists in localStorage");
  });
});

test("resetStableClientID clears persisted identity before a hard reload", () => {
  const local = fakeStorage();
  const session = fakeStorage();
  withStorage({ localStorage: local, sessionStorage: session }, () => {
    const first = stableClientID();
    resetStableClientID();
    const second = stableClientID();
    assert.notEqual(first, second, "reset forces the next load/client to use a fresh id");
    assert.notEqual(baseOf(first), baseOf(second), "the origin-level base was cleared");
  });
});

// --- sessionTicketPersistence (the affinity ticket, per-tab, survives a reload) ---------------

test("sessionTicketPersistence round-trips the ticket through sessionStorage", () => {
  const session = fakeStorage();
  withStorage({ localStorage: fakeStorage(), sessionStorage: session }, () => {
    const p = sessionTicketPersistence();
    assert.equal(p.load(), undefined, "no ticket initially");
    p.save("aff.tkt.1");
    // A fresh instance (a reload within the tab) reads the same persisted ticket back.
    assert.equal(sessionTicketPersistence().load(), "aff.tkt.1", "survives a reload within the tab");
    p.clear();
    assert.equal(sessionTicketPersistence().load(), undefined, "cleared after a dead-follower drop");
  });
});

test("sessionTicketPersistence uses sessionStorage (per-tab), NOT localStorage (per-origin)", () => {
  const local = fakeStorage();
  const session = fakeStorage();
  withStorage({ localStorage: local, sessionStorage: session }, () => {
    sessionTicketPersistence().save("aff.tab.9");
    assert.equal(session.getItem("rindle-affinity-ticket"), "aff.tab.9", "stored in sessionStorage");
    assert.equal(local.getItem("rindle-affinity-ticket"), null, "never in localStorage (two tabs, two regions)");
  });
});

test("sessionTicketPersistence tolerates absent web storage (SSR / private mode)", () => {
  withStorage({ localStorage: undefined, sessionStorage: undefined }, () => {
    const p = sessionTicketPersistence();
    assert.equal(p.load(), undefined);
    // Best-effort: save/clear must not throw when there is no storage area.
    p.save("aff.x.1");
    p.clear();
    assert.equal(p.load(), undefined);
  });
});
