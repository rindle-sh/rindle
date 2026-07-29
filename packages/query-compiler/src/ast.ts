// Re-export the canonical Zero-wire AST types from @rindle/client — the single source of
// truth for the query shape the builder emits (`client/src/ast.ts`) and the Rust engine
// deserializes (`rust/src/ast.rs`). The compiler consumes these verbatim, so it can never
// drift from what the client produces.
export type {
  Aggregate,
  Ast,
  Bound,
  Condition,
  CorrelatedSubquery,
  Correlation,
  Dir,
  ExistsOp,
  LitValue,
  OrderPart,
  SimpleOp,
  ValuePosition,
} from "@rindle/client";
