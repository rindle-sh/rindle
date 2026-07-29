// Postgres type classification — the predicates that decide which `::text::<type>` cast a
// value gets (§6.2). Ported verbatim from mono's `zero-cache/src/types/pg-data-type.ts` (the
// maps + `formatTypeForLookup`), plus the temporal helpers the projection reconciliation needs.
// Keeping these faithful to z2s is what keeps the filter-side casts bug-for-bug aligned.

// The map *values* are irrelevant to the predicates (they test key membership); kept as in
// mono for fidelity and in case the value-type mapping is needed later.
const pgToZqlNumericTypeMap: Record<string, string> = Object.freeze({
  smallint: "number",
  integer: "number",
  int: "number",
  int2: "number",
  int4: "number",
  int8: "number",
  bigint: "number",
  smallserial: "number",
  serial: "number",
  serial2: "number",
  serial4: "number",
  serial8: "number",
  bigserial: "number",
  decimal: "number",
  numeric: "number",
  real: "number",
  "double precision": "number",
  float: "number",
  float4: "number",
  float8: "number",
});

const pgToZqlNativeStringTypeMap: Record<string, string> = Object.freeze({
  bpchar: "string",
  character: "string",
  "character varying": "string",
  text: "string",
  varchar: "string",
});

const pgToZqlTextRepresentedTypeMap: Record<string, string> = Object.freeze({
  cidr: "string",
  ean13: "string",
  inet: "string",
  isbn: "string",
  isbn13: "string",
  ismn: "string",
  ismn13: "string",
  issn: "string",
  issn13: "string",
  macaddr: "string",
  macaddr8: "string",
  pg_lsn: "string",
  upc: "string",
  uuid: "string",
});

const pgToZqlStringTypeMap: Record<string, string> = Object.freeze({
  ...pgToZqlNativeStringTypeMap,
  ...pgToZqlTextRepresentedTypeMap,
});

/** Strips args (the `(32)` in `char(32)`) and lowercases — mono's `formatTypeForLookup`.
 *  Every predicate normalizes through this, so `VARCHAR(255)` / `UUID` classify correctly. */
export function formatTypeForLookup(pgType: string): string {
  const startOfArgs = pgType.indexOf("(");
  if (startOfArgs === -1) return pgType.toLowerCase();
  return pgType.toLowerCase().substring(0, startOfArgs);
}

export function isPgNumberType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlNumericTypeMap, formatTypeForLookup(pgType));
}

export function isPgNativeStringType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlNativeStringTypeMap, formatTypeForLookup(pgType));
}

export function isPgTextRepresentedType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlTextRepresentedTypeMap, formatTypeForLookup(pgType));
}

export function isPgStringType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlStringTypeMap, formatTypeForLookup(pgType));
}

// ── temporal helpers (projection reconciliation, §6.2) ──────────────────────────────────────

const TEMPORAL_TYPES = new Set([
  "date",
  "time",
  "time without time zone",
  "time with time zone",
  "timetz",
  "timestamp",
  "timestamp without time zone",
  "timestamp with time zone",
  "timestamptz",
]);

/** A temporal type projected as epoch-ms (outbound) and reconciled via `to_timestamp`/interval
 *  (inbound). Everything else projects raw. */
export function isTemporalType(pgType: string): boolean {
  return TEMPORAL_TYPES.has(formatTypeForLookup(pgType));
}

// Offset-bearing time-of-day types: `EXTRACT(EPOCH FROM …)` can go negative (e.g. 01:00+02 =
// 23:00 UTC prev day = -3600s), so the outbound projection normalizes into 0..86_400_000 ms.
const TIME_OF_DAY_TZ_TYPES = new Set(["timetz", "time with time zone"]);

/**
 * Whether an outbound temporal projection needs the `mod 1 day` normalization. **Deliberately
 * narrower than z2s's `selectIdent`**, which (via a switch fall-through) also normalizes
 * `timestamptz` — that would collapse a full timestamp to time-of-day. The design (§6.2) is
 * explicit that `timestamptz` projects as plain epoch-ms and only the offset-bearing
 * *time-of-day* types are modular-normalized; the §9 parity harness against the engine's value
 * model is the check that keeps this honest.
 */
export function needsTimeOfDayNormalization(pgType: string): boolean {
  return TIME_OF_DAY_TZ_TYPES.has(formatTypeForLookup(pgType));
}
