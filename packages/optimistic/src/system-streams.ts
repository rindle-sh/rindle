// The §4 lifecycle SYSTEM-STREAM plane, client half (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md
// Slice I-iii): the four daemon-registered system tables the api-server mints subscriptions over
// when its realtime `lifecycle` config is present — the doorbell (`_rindle_scope_sessions`, §4.1)
// on every labeled lease, and the fence bundle (`_rindle_room_watermark` §4.2 +
// `_rindle_room_client_mutations` §7.1 + `_rindle_room_mutation_outcomes` §3.3) on every
// room-served lease.
//
// This module holds the leaf vocabulary the backend and the client share:
//   - the table NAMES + wire schemas (the daemon DDL is the source of truth —
//     `rust/rindle-replica/src/mutations.rs` `realtime_lifecycle_ddl()` / the room-ledger DDL in
//     `enable_client_mutations` — mirrored here exactly like `CLIENT_MUTATIONS_SCHEMA` mirrors the
//     lmid table, so the system subscriptions' hellos pass CRIT#4 validation);
//   - the retain SPEC (`SystemStreamSpec`) a system subscription carries so the backend knows,
//     at fold time, which table a system qid serves and which scope/doc it was minted for;
//   - the reserved client-side query NAME system retains subscribe under. Like
//     `LMID_QUERY_NAME`, it is part of the client wire bookkeeping, NOT an app query: the
//     api-server never resolves it by name — the subscribe presents the minted lease's
//     `leaseToken`, and a RE-resolution re-leases the PARENT labeled query and picks the matching
//     entry out of the fresh `lifecycle` block (client.ts).
//
// The schemas live HERE (not `@rindle/client`) deliberately: the system plane is an
// optimistic-store concern — no other backend composition consumes these tables.

import type { NormalizedTableSchema, WireValue } from "@rindle/client";

/** §4.1 occupancy: `(scope, client_id, expires_at)`, PK `(scope, client_id)`. The row delta IS
 *  the upgrade doorbell (Slice I-iv reacts; I-iii only folds the map). */
export const SCOPE_SESSIONS_TABLE = "_rindle_scope_sessions";
/** §4.2 downgrade fence: `(doc, flush_seq)`, PK `doc` — monotone, co-committed per room flush. */
export const ROOM_WATERMARK_TABLE = "_rindle_room_watermark";
/** §7.1 domain-scoped room ledger: `(doc, client_id, last_mutation_id)`, PK `(doc, client_id)` —
 *  the FIRST daemon-carried room-lmid path (the named invariant's enforcement point). */
export const ROOM_CLIENT_MUTATIONS_TABLE = "_rindle_room_client_mutations";
/** §3.3 durable outcome rows: `(doc, client_id, mid, kind, reason, name, args)`, PK
 *  `(doc, client_id, mid)` — the H-iv-b `mutationOutcome` frame's durable twin (Slice I-ii
 *  co-commits one per NON-applied mid; an absent row under a covering lmid means `applied`). */
export const ROOM_MUTATION_OUTCOMES_TABLE = "_rindle_room_mutation_outcomes";

/** The four wire schemas, mirroring the daemon DDL column-for-column (order and PK by name are
 *  what `validateAgainstClientSchema` checks at each system hello — CRIT#4). Appended to the
 *  backend's expected-schema base unconditionally: extra CLIENT-side entries are inert (validation
 *  only checks tables a server actually advertises), so a client that never receives a lifecycle
 *  block behaves byte-identically. */
export const SCOPE_SESSIONS_SCHEMA: NormalizedTableSchema = {
  name: SCOPE_SESSIONS_TABLE,
  columns: ["scope", "client_id", "expires_at"],
  primaryKey: [0, 1],
};
export const ROOM_WATERMARK_SCHEMA: NormalizedTableSchema = {
  name: ROOM_WATERMARK_TABLE,
  columns: ["doc", "flush_seq"],
  primaryKey: [0],
};
export const ROOM_CLIENT_MUTATIONS_SCHEMA: NormalizedTableSchema = {
  name: ROOM_CLIENT_MUTATIONS_TABLE,
  columns: ["doc", "client_id", "last_mutation_id"],
  primaryKey: [0, 1],
};
export const ROOM_MUTATION_OUTCOMES_SCHEMA: NormalizedTableSchema = {
  name: ROOM_MUTATION_OUTCOMES_TABLE,
  columns: ["doc", "client_id", "mid", "kind", "reason", "name", "args"],
  primaryKey: [0, 1, 2],
};

export const LIFECYCLE_TABLE_SCHEMAS: readonly NormalizedTableSchema[] = [
  SCOPE_SESSIONS_SCHEMA,
  ROOM_WATERMARK_SCHEMA,
  ROOM_CLIENT_MUTATIONS_SCHEMA,
  ROOM_MUTATION_OUTCOMES_SCHEMA,
];

/** The tables a system retain may serve — the fold category discriminant. */
export type SystemStreamTable =
  | typeof SCOPE_SESSIONS_TABLE
  | typeof ROOM_WATERMARK_TABLE
  | typeof ROOM_CLIENT_MUTATIONS_TABLE
  | typeof ROOM_MUTATION_OUTCOMES_TABLE;

/** What a system retain declares about itself (`OptimisticBackend.retainSystemQuery`): which
 *  system table its frames carry, and the scope/doc its minted predicate was scoped to. The fold
 *  filters rows against this spec (AND against the client's own `clientID`) as DEFENSE IN DEPTH —
 *  the server predicate is the tight path, but a server that couldn't scope (no `clientId` on the
 *  lease request) legitimately delivers other clients' rows, and a confused server must never
 *  invent verdicts for us. */
export interface SystemStreamSpec {
  table: SystemStreamTable;
  /** The §4.1 occupancy scope the doorbell was minted for (`_rindle_scope_sessions` only). */
  scope?: string;
  /** The room doc the fence entry was minted for (the three room tables). */
  doc?: string;
}

/** The reserved name system retains subscribe under (never an app query — see the module doc).
 *  Its `args` carry the {@link SystemStreamSpec} identity fields PLUS the parent labeled query's
 *  `(name, args)`, so a RE-resolution (reconnect / gate overflow) can re-lease the parent and pick
 *  the matching lifecycle entry — renewal-as-reauthorization, the room-token precedent. */
export const LIFECYCLE_QUERY_NAME = "_rindle/lifecycle";

/** The domain/gate key a room doc's daemon-carried confirms fold into — the SAME key the lease
 *  wire mints for the room source (`api-server: sourceKey = "room:" + doc`), so a daemon-carried
 *  lmid and a live room socket's lmid stream land on ONE watermark entry. */
export function roomDomainKey(doc: string): string {
  return `room:${doc}`;
}

/** One outcome row, decoded (positional per {@link ROOM_MUTATION_OUTCOMES_SCHEMA}). */
export interface OutcomeRow {
  doc: string;
  clientId: string;
  mid: number;
  kind: "deopt" | "rejected";
  reason?: string;
  name?: string;
  args?: unknown;
}

/** Decode one `_rindle_room_mutation_outcomes` wire row; `undefined` for a malformed one (fail
 *  CLOSED into "not a verdict" — a garbled row must not invent a deopt/rejection; the absent-row
 *  default of the covering lmid then treats the mid as applied, which is I-ii's sound default). */
export function decodeOutcomeRow(row: readonly WireValue[]): OutcomeRow | undefined {
  const [doc, clientId, mid, kind, reason, name, args] = row;
  if (typeof doc !== "string" || typeof clientId !== "string") return undefined;
  const midNum = Number(mid);
  if (!Number.isFinite(midNum)) return undefined;
  if (kind !== "deopt" && kind !== "rejected") return undefined;
  const out: OutcomeRow = { doc, clientId, mid: midNum, kind };
  if (typeof reason === "string") out.reason = reason;
  if (typeof name === "string") out.name = name;
  if (typeof args === "string") {
    // The row echoes `args` as JSON TEXT (I-ii). A frame synthesized without parseable args still
    // resolves a PENDING entry (the flip re-uses the entry's own args); only the already-retired
    // re-invoke arm needs them — it drops a frame that is not self-contained, the H-v contract.
    try {
      out.args = JSON.parse(args);
    } catch {
      /* not self-contained — see above */
    }
  }
  return out;
}
