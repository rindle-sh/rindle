// The NORMALIZED subscription protocol — the wire contract shared by the server
// (`@rindle/server`, which relays the native engine's already-stamped frames) and the client
// (`RemoteNormalizedSource`, which validates via {@link NormalizedSubscriber}). The path-free
// twin of `./protocol.ts`: same epoch/seq/gap envelope, normalized payloads (table-tagged
// `NormalizedOp`s), and a slim per-table-schema hello (NORMALIZED-CHANGES-DESIGN.md §3).
//
// As with the flat path, the Subscriber does ONLY validation/sequencing and hands clean
// `NormalizedEvent`s up — the `NormalizedSync` layer (in `@rindle/normalized`) folds them.

import { COMPARATOR_VERSION } from "@rindle/client";
import type { NormalizedEvent, NormalizedOp, NormalizedTableSchema } from "@rindle/client";

import { Fnv } from "./protocol.ts";
import { ProtocolError } from "./protocol.ts";

// ----------------------------- normalized fingerprint -----------------------------

/** A `tables` set's content fingerprint — FNV-1a 64 over the canonical, length-prefixed byte
 *  stream of `rindle-replica::normalize_protocol::normalized_fp` (PK resolved to column NAMES),
 *  as 16-char lowercase hex (=== the Rust hex). `tables` MUST be sorted by name (the server's
 *  `NormalizedPublisher` guarantees it) so the fingerprint is order-stable. */
export function normalizedFp(tables: NormalizedTableSchema[]): string {
  const f = new Fnv();
  f.u32(tables.length);
  for (const t of tables) {
    f.s(t.name);
    f.u32(t.columns.length);
    for (const c of t.columns) f.s(c);
    f.u32(t.primaryKey.length);
    for (const pk of t.primaryKey) f.s(t.columns[pk]); // PK by NAME
  }
  return f.h.toString(16).padStart(16, "0");
}

/** Is `sub` a subsequence of `sup` by NAME — every entry of `sub` present in `sup`, in order?
 *  Accepts a projection (server ⊆ client) or an expansion (client ⊆ server); a rename/reorder
 *  satisfies neither direction. */
function isSubsequenceByName(sub: readonly string[], sup: readonly string[]): boolean {
  let i = 0;
  for (const name of sup) if (i < sub.length && sub[i] === name) i++;
  return i === sub.length;
}

/** Validate the tables a `hello` advertises against the CLIENT's own typed schema, by name
 *  (column order + PK indices). `hello.tables` is the query's table subtree, so each one must
 *  be column-compatible with a client table. Throws {@link ProtocolError} `"schema-mismatch"`
 *  on the first unknown table / column-order skew / PK skew — the §3 "drift ⇒ re-subscribe"
 *  guard (CRIT#4). */
function validateAgainstClientSchema(
  serverTables: NormalizedTableSchema[],
  clientTables: NormalizedTableSchema[],
): void {
  const byName = new Map(clientTables.map((t) => [t.name, t]));
  const eq = (a: readonly (string | number)[], b: readonly (string | number)[]) =>
    a.length === b.length && a.every((x, i) => x === b[i]);
  for (const st of serverTables) {
    const ct = byName.get(st.name);
    if (!ct) {
      throw new ProtocolError("schema-mismatch", `server advertises table "${st.name}" not in the client schema`);
    }
    // The server may advertise FEWER columns than the client (a projected query —
    // PROJECTION-SUPPORT-DESIGN.md §5.2/§7) OR MORE (an EXPANDED server table mid an
    // `expand-then-contract` migration — the client drops the columns it doesn't yet have).
    // Both are safe: the client maps every advertised column to a base position BY NAME, so
    // the relative order is preserved either way. Accept when one column list is a SUBSEQUENCE
    // of the other by name — a narrowing (projection) or a widening (expand) — while still
    // rejecting a genuine skew, where NEITHER is a subsequence of the other: a renamed column
    // or a reordered one (the CRIT#4 guard). A server with more columns than the client is no
    // longer drift; without this, `expand-then-contract` is impossible.
    if (!isSubsequenceByName(st.columns, ct.columns) && !isSubsequenceByName(ct.columns, st.columns)) {
      throw new ProtocolError(
        "schema-mismatch",
        `column drift on "${st.name}": server [${st.columns}] is neither a projection nor an expansion of client [${ct.columns}] (column order skew)`,
      );
    }
    // PK compared by NAME (the server forces the PK into every projection, so it is always
    // present): resolve each side's PK indices to names and require equality.
    const serverPk = st.primaryKey.map((i) => st.columns[i]);
    const clientPk = ct.primaryKey.map((i) => ct.columns[i]);
    if (!eq(serverPk, clientPk)) {
      throw new ProtocolError(
        "schema-mismatch",
        `primary-key drift on "${st.name}": client [${clientPk}] != server [${serverPk}]`,
      );
    }
  }
}

// ----------------------------- the wire frames -----------------------------

/** The slim normalized handshake (§3), sent once before any {@link NormalizedBatch}. */
export interface NormalizedHello {
  epoch: number;
  comparatorVersion: number;
  tables: NormalizedTableSchema[];
  normalizedFp: string;
}

/** One committed transaction's normalized ops (or the seq-0 hydrate snapshot). `cv` (the
 *  global commit version the frame reflects) is stamped by optimistic-protocol servers and
 *  rides through to the `NormalizedEvent` (OPTIMISTIC-WRITES-DESIGN.md §8.3). */
export interface NormalizedBatch {
  epoch: number;
  seq: number;
  normalizedFp: string;
  ops: NormalizedOp[];
  cv?: number;
}

// ----------------------------- Subscriber (client side) -----------------------------

/** Receiver side: validates a normalized frame stream (comparator at `hello`; per batch —
 *  epoch match, fingerprint match, strict in-order seq) and emits clean `NormalizedEvent`s.
 *  It does NOT fold (the `NormalizedSync` layer does). Mirrors the flat {@link Subscriber}. */
export class NormalizedSubscriber {
  readonly epoch: number;
  readonly normalizedFp: string;
  private readonly emit: (ev: NormalizedEvent) => void;
  private phase: "snapshot" | "live" = "snapshot";
  private lastSeq = 0;

  /**
   * @param hello         the server's normalized handshake.
   * @param emit          clean-event sink (the `NormalizedSync` fold).
   * @param clientTables  the CLIENT's own typed per-table schemas (all tables). When given,
   *   each table the hello advertises is validated by NAME against it (column order + PK
   *   indices). The hello's `tables` is a per-query SUBSET (the query's table tree), so this
   *   checks each advertised table rather than one global fingerprint. A mismatch (routine
   *   deployment / schema skew) is rejected here — without it, positional rows aligned to the
   *   SERVER's column order are stored verbatim under the CLIENT's order, silently swapping
   *   cells and mis-keying the refcount/GC (CRIT#4 / §3 "drift ⇒ re-subscribe").
   */
  constructor(
    hello: NormalizedHello,
    emit: (ev: NormalizedEvent) => void,
    clientTables?: NormalizedTableSchema[],
  ) {
    this.emit = emit;
    if (hello.comparatorVersion !== COMPARATOR_VERSION) {
      throw new ProtocolError(
        "comparator-mismatch",
        `comparator version ${hello.comparatorVersion} != ${COMPARATOR_VERSION}`,
      );
    }
    const computed = normalizedFp(hello.tables);
    if (computed !== hello.normalizedFp) {
      throw new ProtocolError("schema-mismatch", `advertised fp ${hello.normalizedFp} != computed ${computed}`);
    }
    if (clientTables) validateAgainstClientSchema(hello.tables, clientTables);
    this.epoch = hello.epoch;
    this.normalizedFp = hello.normalizedFp;
    emit({
      type: "hello",
      tables: hello.tables,
      comparatorVersion: hello.comparatorVersion,
      normalizedFp: hello.normalizedFp,
    });
  }

  /** Apply one normalized batch (or the seq-0 snapshot). Returns `"duplicate"` for an
   *  already-applied seq; throws {@link ProtocolError} on a gap / epoch / fp mismatch (the
   *  caller re-hydrates under a new epoch). */
  apply(batch: NormalizedBatch): "applied" | "duplicate" {
    if (batch.epoch !== this.epoch) {
      throw new ProtocolError("epoch-mismatch", `expected epoch ${this.epoch}, got ${batch.epoch}`);
    }
    if (batch.normalizedFp !== this.normalizedFp) {
      throw new ProtocolError("schema-mismatch", `expected fp ${this.normalizedFp}, got ${batch.normalizedFp}`);
    }
    if (this.phase === "snapshot") {
      if (batch.seq === 0) {
        this.phase = "live";
        this.lastSeq = 0;
        this.emit({ type: "snapshot", ops: batch.ops, cv: batch.cv });
        return "applied";
      }
      throw new ProtocolError("gap", `expected the seq-0 snapshot, got seq ${batch.seq}`);
    }
    const expected = this.lastSeq + 1;
    if (batch.seq < expected) return "duplicate";
    if (batch.seq > expected) {
      throw new ProtocolError("gap", `expected seq ${expected}, got ${batch.seq}`);
    }
    this.lastSeq = batch.seq;
    this.emit({ type: "batch", ops: batch.ops, cv: batch.cv });
    return "applied";
  }
}
