// The Postgres dialect leaf (§6.1/§6.2) — the product target. The walker is shared with the
// SQLite oracle; this file supplies only the Postgres-specific leaves:
//
//  - JSON assembly via `jsonb_build_object` / `jsonb_agg` / `'[]'::jsonb` (the design's §6.1
//    choice — the direct analogue of `rindle-d2s`'s `json_object`, NOT z2s's `row_to_json`);
//  - explicit `ASC NULLS FIRST` / `DESC NULLS LAST` (§8 — Postgres's default is the opposite
//    of the engine's null-low order);
//  - the outbound projection reconciliation (temporal → epoch-ms);
//  - the inbound filter/paging cast strategy `$N::text::<type>`, ported verbatim from z2s's
//    `sql.ts` — the `::text` intermediate pins the driver's parameter description to text, and
//    the `::<type>` half is value-model↔native reconciliation (§6.2). Values are parameterized.
//
// NOTE: `$N` numbering is per-occurrence (no de-duplication of repeated values). z2s reuses a
// placeholder for a repeated (value,type); that is a plan-cache optimization, not correctness —
// deferred (203 §7 posture).

import type { Dir } from "./ast.ts";
import type { ColumnType } from "./catalog.ts";
import type { Dialect, LitScalar } from "./dialect.ts";
import {
  formatTypeForLookup,
  isPgNativeStringType,
  isPgNumberType,
  isPgStringType,
  isPgTextRepresentedType,
  isTemporalType,
  needsTimeOfDayNormalization,
} from "./pg-types.ts";

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** The Postgres cast for a literal whose type is inferred from its JS value (no column). */
function pgTypeForLiteralType(litType: "boolean" | "number" | "string"): string {
  // `double precision` is IEEE-754 like a JS number, so it round-trips any zql number exactly.
  return litType === "boolean" ? "boolean" : litType === "number" ? "double precision" : "text";
}

/**
 * The inbound value↔native cast for a filter/paging value against a known column type — the
 * verbatim port of z2s's `formatCommonToSingularAndPlural` (`sql.ts:194`). `vp` is the value
 * placeholder (`$N`, or `value` inside an array unnest). `isComparison` widens strings to bare
 * `text` and numbers to `double precision` so neither is forced to the column's width/precision.
 */
function pgCast(vp: string, type: string, isEnum: boolean, isComparison: boolean): string {
  const t = formatTypeForLookup(type);
  // Temporal: epoch-ms ↔ native. The zone-naive spellings get `AT TIME ZONE 'UTC'`.
  if (t === "timestamptz" || t === "timestamp with time zone") {
    return `to_timestamp(${vp}::text::numeric / 1000.0)`;
  }
  if (t === "date" || t === "timestamp" || t === "timestamp without time zone") {
    return `to_timestamp(${vp}::text::numeric / 1000.0) AT TIME ZONE 'UTC'`;
  }
  if (t === "timetz" || t === "time with time zone") {
    return `(${vp}::text::int * interval'1ms')::time`;
  }
  if (t === "time" || t === "time without time zone") {
    return `(${vp}::text::int * interval'1ms')::time AT TIME ZONE 'UTC'`;
  }
  if (t === "uuid") return `${vp}::text::uuid`;
  if (isEnum) return `${vp}::text::"${type}"`;
  if (isPgNativeStringType(type)) return isComparison ? `${vp}::text` : `${vp}::text::${type}`;
  if (isPgTextRepresentedType(type)) return `${vp}::text::${type}`;
  if (isPgNumberType(type)) {
    return isComparison ? `${vp}::text::double precision` : `${vp}::text::${type}`;
  }
  return `${vp}::text::${type}`;
}

/** The text-pinned serialization of a bound value — z2s's `stringify` (`sql.ts:120`). Strings
 *  and enum/string columns bind their raw text; everything else binds its JSON text form so the
 *  `$N::text::…` cast re-parses it authoritatively. */
function stringifyForColumn(value: LitScalar, col: ColumnType): string | null {
  if (value === null) return null;
  if (col.isArray) return JSON.stringify(value);
  if (col.isEnum || isPgStringType(col.type)) return String(value);
  return JSON.stringify(value);
}

function bindValue(
  params: unknown[],
  value: LitScalar,
  col: ColumnType | null,
  isComparison: boolean,
): string {
  const idx = params.length + 1;

  // Free literal (no column context): infer the cast from the JS type.
  if (col === null) {
    if (value === null) {
      params.push(null);
      return `$${idx}`;
    }
    const litType = typeof value as "boolean" | "number" | "string";
    params.push(litType === "string" ? value : JSON.stringify(value));
    return `$${idx}::text::${pgTypeForLiteralType(litType)}`;
  }

  params.push(stringifyForColumn(value, col));
  if (value === null && col.isArray) return `$${idx}`;
  if (col.isArray) {
    // Unnest a JSON array param, casting each element (z2s's `formatPlural`, `sql.ts:260`).
    return `ARRAY(SELECT ${pgCast("value", col.type, col.isEnum, isComparison)} FROM jsonb_array_elements_text($${idx}::text::jsonb))`;
  }
  return pgCast(`$${idx}`, col.type, col.isEnum, isComparison);
}

/**
 * Project a stored column into the value model's representation (§6.2 outbound half). Temporal
 * columns become **integer epoch-ms** — the value model always snaps to whole milliseconds, so
 * `::bigint` rounds off the native µs resolution (unlike z2s, which projects the non-normalized
 * path as an unsnapped float). Time-of-day-with-offset types additionally wrap `mod 1 day`.
 * Enums (their label), uuid, numeric, json, and the scalar types project raw. See
 * `needsTimeOfDayNormalization` for the deliberate divergence from z2s on `timestamptz`.
 *
 * The rounding mode (`::bigint` = round-half-to-even) must match pg-source's projector for exact
 * parity — it is the one shared value-model contract (§10), not an independent choice here.
 */
function projectColumn(colRef: string, col: ColumnType | null): string {
  if (!col || col.isEnum || !isTemporalType(col.type)) return colRef;
  const normalize = needsTimeOfDayNormalization(col.type);
  const toMs = (epoch: string): string =>
    normalize ? `((${epoch})::bigint + 86400000) % 86400000` : `(${epoch})::bigint`;
  if (col.isArray) {
    return `CASE WHEN ${colRef} IS NULL THEN NULL ELSE ARRAY(SELECT ${toMs(`EXTRACT(EPOCH FROM unnest(${colRef})) * 1000`)}) END`;
  }
  return toMs(`EXTRACT(EPOCH FROM ${colRef}) * 1000`);
}

function orderDir(dir: Dir): string {
  // Engine order is null-low; Postgres's default is the opposite, so make it explicit (§8).
  return dir === "asc" ? "ASC NULLS FIRST" : "DESC NULLS LAST";
}

/**
 * The Postgres dialect — the BYO-Postgres server-mutator read target. Emits `(sql, params)`
 * with `$N::text::<type>` parameters and never imports a driver (Invariant 7).
 */
export const postgresDialect: Dialect = {
  name: "postgres",
  objectBuild: (parts) => `jsonb_build_object(${parts.join(", ")})`,
  groupArray: (elem, orderSql) => `jsonb_agg(${elem}${orderSql})`,
  emptyArray: "'[]'::jsonb",
  reassertJson: (x) => x, // jsonb is a real type; no re-assert needed
  trueLit: "TRUE",
  falseLit: "FALSE",
  orderDir,
  projectColumn,
  bindValue,
  quoteIdent,
};
