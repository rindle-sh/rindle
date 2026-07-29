// Reject AST features the compiler does not lower, anywhere in the tree — so an unhandled
// field fails loudly rather than producing silently-wrong SQL. Faithful port of
// `rindle-d2s`'s `reject_unsupported` family (lib.rs:194).
//
// `start` (a paging bound) is lowered as a keyset predicate, but only on the **materialized
// collections** the compiler emits (the root and each `related` child). It is *not* honored
// inside an EXISTS/NOT EXISTS subquery, nor inside a `count(child)` aggregate — a `start`
// there is rejected here rather than silently dropped.

import type { Ast, Condition } from "./ast.ts";
import { unsupported } from "./errors.ts";

export function rejectUnsupported(ast: Ast): void {
  for (const rel of ast.related ?? []) {
    if (rel.subquery.aggregate) {
      // A `count(child)` aggregate reduces its rows away: a `start`/`limit`/nested `related`
      // would change the IVM count but `count_expr` ignores them — reject rather than diverge.
      rejectAggregateSubquery(rel.subquery);
    } else {
      rejectUnsupported(rel.subquery); // collection: start honored, recurse
    }
  }
  if (ast.where) {
    rejectUnsupportedCond(ast.where);
  }
}

function rejectAggregateSubquery(ast: Ast): void {
  if (ast.start) throw unsupported("start inside a count aggregate");
  if (ast.limit !== undefined) throw unsupported("limit inside a count aggregate");
  if (ast.related && ast.related.length > 0) {
    throw unsupported("nested related inside a count aggregate");
  }
  if (ast.where) rejectUnsupportedCond(ast.where);
}

function rejectUnsupportedCond(cond: Condition): void {
  switch (cond.type) {
    case "simple":
      return;
    case "correlatedSubquery":
      rejectStartAnywhere(cond.related.subquery); // EXISTS: its `start` is not honored
      return;
    case "and":
    case "or":
      for (const c of cond.conditions) rejectUnsupportedCond(c);
      return;
  }
}

/** Reject a `start` bound *anywhere* in `ast` (the EXISTS subtrees the compiler does not
 *  page). Conservative: rejects even positions a clever lowering might honor. */
function rejectStartAnywhere(ast: Ast): void {
  if (ast.start) throw unsupported("start (paging) inside an EXISTS subquery");
  for (const rel of ast.related ?? []) rejectStartAnywhere(rel.subquery);
  if (ast.where) rejectStartAnywhereCond(ast.where);
}

function rejectStartAnywhereCond(cond: Condition): void {
  switch (cond.type) {
    case "simple":
      return;
    case "correlatedSubquery":
      rejectStartAnywhere(cond.related.subquery);
      return;
    case "and":
    case "or":
      for (const c of cond.conditions) rejectStartAnywhereCond(c);
      return;
  }
}
