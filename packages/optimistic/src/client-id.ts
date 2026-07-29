import type { TicketPersistence } from "@rindle/remote";

// The connection's stable clientID — kept in its own (wasm-free) module so it can be unit-tested
// without pulling the engine in. A per-ORIGIN base (localStorage) shared by every tab and kept
// across reloads, a per-TAB suffix (sessionStorage) that survives a reload within a tab but is
// distinct per tab, and a per-LOAD instance suffix that keeps two clients in one tab distinct.
// Without these two tabs (or two clients in one tab) would share a clientID and collide on the
// server's per-clientID mid sequence (a confirmed-mid-ahead-of-issued throw, OPTIMISTIC-WRITES §8.2).

/** The per-ORIGIN base id (localStorage): one logical identity shared by every tab and kept
 *  across reloads. */
const CLIENT_ID_KEY = "rindle-client-id";
/** The per-TAB suffix (sessionStorage): survives a reload within a tab but is distinct per tab. */
const TAB_ID_KEY = "rindle-tab-id";
/** The per-TAB follower-affinity ticket (sessionStorage): kept across a reload WITHIN a tab (so a
 *  reload lands back on the same follower — monotone reads survive it, FOLLOWER-AFFINITY-DESIGN.md
 *  §8), but DISTINCT per tab so two tabs can pin two regions (design §13; a shared localStorage
 *  ticket would force both onto one follower). The ticket is opaque and short-lived — the follower
 *  re-mints one on every connect — so losing it (no sessionStorage) merely re-anycasts. */
const AFFINITY_TICKET_KEY = "rindle-affinity-ticket";

/** Read-or-mint a value in a web Storage area, tolerating its absence (SSR, privacy mode). Returns
 *  `undefined` when the area is unavailable so the caller can pick a fallback. */
function persistedId(area: "localStorage" | "sessionStorage", key: string, mint: () => string): string | undefined {
  try {
    const storage = (globalThis as unknown as Record<string, Storage | undefined>)[area];
    if (!storage) return undefined;
    const existing = storage.getItem(key);
    if (existing) return existing;
    const fresh = mint();
    storage.setItem(key, fresh);
    return fresh;
  } catch {
    return undefined;
  }
}

/** Per-PAGE-LOAD instance counter. The FIRST client created in a tab keeps the bare tab suffix (so a
 *  reload resumes its mid sequence and adds no row); each ADDITIONAL client constructed in the same
 *  load gets a distinct instance suffix, so two clients in one tab never share a mid stream. Held in
 *  memory so it resets on reload — the sole client then reclaims the bare, reload-stable suffix. */
let tabInstance = 0;

/** Test-only: reset the per-load instance counter to simulate a fresh page load. Deliberately NOT
 *  re-exported from the package barrel. */
export function resetTabInstanceForTests(): void {
  tabInstance = 0;
}

/** Clear the persisted client identity so the next page load starts a fresh mid/lmid stream.
 *  Intended for dev recovery after out-of-band server state loss, not for normal operation. */
export function resetStableClientID(): void {
  try {
    globalThis.localStorage?.removeItem(CLIENT_ID_KEY);
  } catch {
    // Storage can throw in private modes or test shims; best-effort reset.
  }
  try {
    globalThis.sessionStorage?.removeItem(TAB_ID_KEY);
  } catch {
    // Storage can throw in private modes or test shims; best-effort reset.
  }
  tabInstance = 0;
}

/** A sessionStorage-backed {@link TicketPersistence} for the affinity ticket — per-tab, survives a
 *  reload (see {@link AFFINITY_TICKET_KEY}). Every access tolerates web storage being absent (SSR,
 *  private mode): the store then behaves as pure in-memory, re-anycasting on each fresh load. */
export function sessionTicketPersistence(): TicketPersistence {
  const area = () => {
    try {
      return (globalThis as unknown as Record<string, Storage | undefined>).sessionStorage;
    } catch {
      return undefined;
    }
  };
  return {
    load: () => {
      try {
        return area()?.getItem(AFFINITY_TICKET_KEY) ?? undefined;
      } catch {
        return undefined;
      }
    },
    save: (ticket) => {
      try {
        area()?.setItem(AFFINITY_TICKET_KEY, ticket);
      } catch {
        // Best-effort persistence; a full/blocked store just means we re-anycast on the next load.
      }
    },
    clear: () => {
      try {
        area()?.removeItem(AFFINITY_TICKET_KEY);
      } catch {
        // Best-effort.
      }
    },
  };
}

/** The connection's stable clientID. A per-origin base (localStorage) keeps one logical identity
 *  across reloads; a per-tab suffix (sessionStorage) gives each tab its OWN mid/lmid stream; a
 *  per-load instance suffix keeps two clients constructed in ONE tab from sharing that stream. Falls
 *  back to a fresh random id when web storage is unavailable.
 *
 *  Note: an explicit "duplicate tab" copies sessionStorage, so the clone briefly shares its source's
 *  suffix until reloaded — the one residual case of the shared-clientID collision. */
export function stableClientID(): string {
  const base = persistedId("localStorage", CLIENT_ID_KEY, () => crypto.randomUUID());
  if (!base) return crypto.randomUUID(); // no localStorage: session-scoped, unique per load
  // With localStorage but no sessionStorage, mint a fresh suffix each load so tabs still differ.
  const tab =
    persistedId("sessionStorage", TAB_ID_KEY, () => crypto.randomUUID().slice(0, 8)) ??
    crypto.randomUUID().slice(0, 8);
  // Instance 0 keeps the bare tab suffix (reload-stable, no extra row); extra concurrent clients in
  // one tab get `.N` so they can't collide on the server's per-clientID mid sequence.
  const instance = tabInstance++;
  const suffix = instance === 0 ? tab : `${tab}.${instance}`;
  return `${base}-${suffix}`;
}
