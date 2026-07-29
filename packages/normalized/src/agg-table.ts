// Synthetic aggregate tables (AGGREGATE-SYNC-DESIGN.md §3.1/§3.3) — the client twin of
// `rindle-replica/src/normalize.rs`. A relationship `count` is never recomputed locally
// (the client lacks the child rows); the server ships the reduce's `(group_key, count)`
// output as a SYNTHETIC base table, and the client:
//   1. registers that table on its local engine + the `NormalizedSync` refcount layer,
//   2. rewrites the local AST so the relationship reads the table with a plain singular
//      join + the same scalar projection (`aggregatePrecomputed`) instead of a reduce.
//
// The table NAME is a content hash of the aggregate's definition, so client and server
// agree without coordinating. `aggTableName` is the **byte-exact** twin of Rust
// `agg_table_name` — the FNV-1a-64 byte protocol documented there; cross-language vectors
// pin it (Rust `agg_table_name_cross_language_vector` ↔ this module's test).

import type {
  Aggregate,
  Ast,
  Condition,
  CorrelatedSubquery,
  Correlation,
  LitValue,
  NormalizedTableSchema,
  ValuePosition,
} from "@rindle/client";

// FNV-1a-64, the same constants as `protocol.ts`'s `Fnv` (a local copy so `@rindle/normalized`
// keeps no dependency on `@rindle/remote`). The byte methods mirror Rust `normalize::Fnv`.
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x00000100000001b3n;
const MASK = 0xffffffffffffffffn;
const enc = new TextEncoder();

class Fnv {
  h = FNV_OFFSET;
  byte(b: number): void {
    this.h = ((this.h ^ BigInt(b & 0xff)) * FNV_PRIME) & MASK;
  }
  /** A `u32` little-endian (Rust `v.to_le_bytes()`). */
  u32(v: number): void {
    this.byte(v & 0xff);
    this.byte((v >>> 8) & 0xff);
    this.byte((v >>> 16) & 0xff);
    this.byte((v >>> 24) & 0xff);
  }
  /** A length-prefixed string: `u32(byteLen)` then UTF-8 bytes. */
  s(str: string): void {
    const bytes = enc.encode(str);
    this.u32(bytes.length);
    for (const b of bytes) this.byte(b);
  }
  /** An `f64` little-endian (Rust `n.to_le_bytes()` — IEEE-754 binary64). */
  f64(v: number): void {
    const buf = new ArrayBuffer(8);
    new DataView(buf).setFloat64(0, v, true);
    for (const b of new Uint8Array(buf)) this.byte(b);
  }
  /** An `i64` little-endian (Rust `i.to_le_bytes()` — two's complement). */
  i64(v: bigint): void {
    const u = BigInt.asUintN(64, v);
    for (let shift = 0n; shift < 64n; shift += 8n) this.byte(Number((u >> shift) & 0xffn));
  }
  hex(): string {
    return this.h.toString(16).padStart(16, "0");
  }
}

/** The synthetic base-table name for a relationship `count` aggregate — the byte-exact twin
 *  of Rust `agg_table_name` (`normalize.rs` §3.1). A content hash of the aggregate definition
 *  (child table, kind, group key = correlation **child** fields, child `where`); the parent
 *  correlation field is excluded. Same definition ⇒ same name (cross-query sharing); a
 *  different filter ⇒ a different name (no `(table, pk)` collision). */
export function aggTableName(csq: CorrelatedSubquery): string {
  const f = new Fnv();
  f.u32(csq.correlation.childField.length);
  for (const cf of csq.correlation.childField) f.s(cf);
  hashAggSubquery(f, csq.subquery);
  return `__agg_${f.hex()}`;
}

function hashAggSubquery(f: Fnv, ast: Ast): void {
  f.s(ast.table);
  f.byte(ast.aggregate === "count" ? 1 : 0); // Some(Count)=1, None=0
  if (ast.where === undefined) f.byte(0);
  else {
    f.byte(1);
    hashCondition(f, ast.where);
  }
  const related = ast.related ?? [];
  f.u32(related.length);
  for (const r of related) {
    hashCorrelation(f, r.correlation);
    hashAggSubquery(f, r.subquery);
  }
}

function hashCorrelation(f: Fnv, c: Correlation): void {
  f.u32(c.parentField.length);
  for (const p of c.parentField) f.s(p);
  f.u32(c.childField.length);
  for (const x of c.childField) f.s(x);
}

function hashCondition(f: Fnv, cond: Condition): void {
  switch (cond.type) {
    case "simple":
      f.byte(0);
      f.s(cond.op); // SimpleOp is the wire string ("=", "!=", …) === Rust `op_str`
      hashValuePosition(f, cond.left);
      hashValuePosition(f, cond.right);
      break;
    case "and":
      f.byte(1);
      f.u32(cond.conditions.length);
      for (const c of cond.conditions) hashCondition(f, c);
      break;
    case "or":
      f.byte(2);
      f.u32(cond.conditions.length);
      for (const c of cond.conditions) hashCondition(f, c);
      break;
    case "correlatedSubquery":
      f.byte(3);
      f.byte(cond.op === "EXISTS" ? 1 : 0); // Exists=1, NotExists=0
      f.byte(systemByte(cond.related.system));
      hashCorrelation(f, cond.related.correlation);
      hashAggSubquery(f, cond.related.subquery);
      break;
  }
}

function systemByte(sys: CorrelatedSubquery["system"]): number {
  switch (sys) {
    case "permissions":
      return 1; // System::Permissions
    case "client":
      return 2; // System::Client
    case "test":
      return 3; // System::Test
    default:
      return 0; // None
  }
}

function hashValuePosition(f: Fnv, vp: ValuePosition): void {
  if (vp.type === "column") {
    f.byte(0);
    f.s(vp.name);
  } else {
    f.byte(1);
    hashLit(f, vp.value);
  }
}

// The serde boundary for a number literal (Rust `Lit`, untagged): an INTEGER-form JSON
// token in i64 range deserializes as `Lit::Int`; every other number falls through to
// `Number(f64)`.
const INTEGER_TOKEN = /^-?[0-9]+$/;
const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;

function hashLit(f: Fnv, lit: LitValue): void {
  if (lit === null) f.byte(0);
  else if (typeof lit === "boolean") {
    f.byte(1);
    f.byte(lit ? 1 : 0);
  } else if (typeof lit === "number") {
    // The exact-int plane, mirroring serde's untagged rule for Rust `Lit` (design 226
    // Stage B): what serde sees is the WIRE TOKEN, so hash the token, not the binary
    // value. `String(lit)` is exactly the token `JSON.stringify` emits, and above 2^53
    // the two diverge: JS serializes the SHORTEST decimal that round-trips (`2 ** 60` →
    // "1152921504606847000", not ...846976), and serde parses that token as the i64.
    // An integer-form token in i64 range → `Lit::Int` → tag 5 + exact i64 LE bytes;
    // everything else (non-integral, exponent-form ≥ 1e21, out of i64 range) → tag 2 +
    // f64 — where serde's parse also lands back on `lit`'s exact bits. Hashing either
    // side differently would name a different synthetic table from the server's, and
    // the aggregate rows would never route.
    if (!Number.isFinite(lit)) {
      // NaN/±Infinity have NO wire token — `JSON.stringify` emits `null`, so the
      // server parses `Lit::Null` and hashes the null tag. Mirror that, not the
      // binary f64 bits String() would suggest ("NaN" is not a JSON token).
      f.byte(0);
      return;
    }
    const token = String(lit);
    let int: bigint | null = null;
    if (INTEGER_TOKEN.test(token)) {
      const parsed = BigInt(token);
      if (parsed >= I64_MIN && parsed <= I64_MAX) int = parsed;
    }
    if (int !== null) {
      f.byte(5);
      f.i64(int);
    } else {
      f.byte(2);
      f.f64(lit);
    }
  } else if (typeof lit === "string") {
    f.byte(3);
    f.s(lit);
  } else if (typeof lit === "bigint") {
    // A bigint literal cannot reach the server at all — `JSON.stringify` throws on
    // bigint, so no wire token exists to mirror. Refuse informatively here rather
    // than fall into the array branch's `lit.length`/iteration TypeError.
    throw new Error(
      "bigint literals are not supported on the live-query wire until the browser " +
        "bigint lane ships (design 226) — pass a number, or read exact int64 cells " +
        "through the SQL plane",
    );
  } else {
    f.byte(4);
    f.u32(lit.length);
    for (const x of lit) hashLit(f, x);
  }
}

/** Every synthetic aggregate table `ast` surfaces (recursively, a nested aggregate under a
 *  materialized relationship included), as flat table schemas — the twin of Rust
 *  `agg_table_schemas`. Columns `[childField…, "count"]`; PK = the leading group columns. */
export function aggTableSchemas(ast: Ast, isLocal?: (table: string) => boolean): NormalizedTableSchema[] {
  const out: NormalizedTableSchema[] = [];
  collectAggTables(ast, out, isLocal);
  return out;
}

function collectAggTables(ast: Ast, out: NormalizedTableSchema[], isLocal?: (table: string) => boolean): void {
  for (const r of ast.related ?? []) {
    if (r.subquery.aggregate) {
      // L1 (`201-LOCAL-ONLY-TABLES-DESIGN.md` §5.2): a count over a LOCAL child is a native IVM
      // reduce — it has no server-authoritative `__agg_*` base, so emit no synthetic schema for it.
      if (isLocal?.(r.subquery.table)) continue;
      const k = r.correlation.childField.length;
      out.push({
        name: aggTableName(r),
        columns: [...r.correlation.childField, "count"],
        primaryKey: Array.from({ length: k }, (_, i) => i),
      });
    } else {
      collectAggTables(r.subquery, out, isLocal);
    }
  }
}

/** Rewrite an AST for the LOCAL engine: each relationship `count` becomes a precomputed,
 *  source-backed singular relationship over its synthetic table — read the server's count
 *  with a plain join + the same scalar projection (`aggregatePrecomputed`), never a `reduce`
 *  (which would recount the already-aggregated rows). Non-aggregate relationships recurse
 *  (a nested aggregate is rewritten too); the parent's frame is otherwise untouched, so the
 *  view-schema slot order is preserved. Returns a new AST; the input is not mutated. */
export function rewriteAggregates(ast: Ast, isLocal?: (table: string) => boolean): Ast {
  if (!ast.related?.length) return ast;
  return { ...ast, related: ast.related.map((r) => rewriteRelationship(r, isLocal)) };
}

function rewriteRelationship(csq: CorrelatedSubquery, isLocal?: (table: string) => boolean): CorrelatedSubquery {
  // L1 (`201-LOCAL-ONLY-TABLES-DESIGN.md` §5.2): a count over a LOCAL child is left as the native
  // IVM reduce (the pre-agg-sync path). The server never ships a count for it, so the precomputed
  // rewrite would point the relationship at an `__agg_*` table that is fed nothing → empty forever.
  if (csq.subquery.aggregate && !isLocal?.(csq.subquery.table)) {
    const subquery: Ast = {
      table: aggTableName(csq),
      aggregate: csq.subquery.aggregate as Aggregate,
      aggregatePrecomputed: true,
    };
    // The correlation is unchanged: the synthetic table's group columns are named after the
    // child correlation fields, so `childField` still resolves against `[childField…, count]`.
    if (csq.subquery.alias !== undefined) subquery.alias = csq.subquery.alias;
    const out: CorrelatedSubquery = { correlation: csq.correlation, subquery };
    if (csq.system !== undefined) out.system = csq.system;
    return out;
  }
  return { ...csq, subquery: rewriteAggregates(csq.subquery, isLocal) };
}
