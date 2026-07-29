// @rindle/normalized — the normalized local-first client glue (NORMALIZED-CHANGES-DESIGN.md §5/§7).
//
// - `NormalizedSync` (Slice 4): the cross-query refcount + GC layer + the `NormalizedOp` wire type.
// - `NormalizedBackend` / `createNormalizedStore` (Slice 5): compose a NORMALIZED server stream
//   (a `NormalizedSource`) with a local `@rindle/wasm` engine, so the ordinary `Store`/`ArrayView`
//   materializes results from base tables fed by the server.

export { NormalizedSync } from "./sync.ts";
export type { ColCounts, NormalizedOp, PkCols } from "./sync.ts";

// Synthetic aggregate tables (§3.3): the byte-exact twin of Rust `agg_table_name` /
// `agg_table_schemas`, plus the local-engine AST rewrite. Exported for tests + advanced glue.
export { aggTableName, aggTableSchemas, rewriteAggregates } from "./agg-table.ts";

export { NormalizedBackend, createNormalizedStore } from "./backend.ts";
export type { NormalizedEvent, NormalizedSource, NormalizedTableSchema } from "./backend.ts";
