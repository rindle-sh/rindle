// Ordering completion — the parity-critical pass (§8). The engine's builder appends the
// full primary key (as `asc`) to every `orderBy`, at every level, for a total order; this is
// a faithful port of `rindle::complete_ordering` (builder.rs:209), which is itself the port
// of the client builder's `completeOrdering`. Running the *same* completion is what makes the
// SELECT's row/child order match the engine's `Skip`/`Sort` tie-breaks exactly.
//
// Mutates `ast` in place (the caller passes a clone). `getPk(table)` yields a table's PK
// column names in PK order.

import type { Ast, Condition } from "./ast.ts";

export function completeOrdering(ast: Ast, getPk: (table: string) => string[]): void {
  const pk = getPk(ast.table);
  for (const csq of ast.related ?? []) {
    completeOrdering(csq.subquery, getPk);
  }
  if (ast.where) {
    completeOrderingInCondition(ast.where, getPk);
  }
  addPrimaryKeys(pk, ast);
}

function completeOrderingInCondition(cond: Condition, getPk: (table: string) => string[]): void {
  switch (cond.type) {
    case "simple":
      return;
    case "correlatedSubquery":
      completeOrdering(cond.related.subquery, getPk);
      return;
    case "and":
    case "or":
      for (const c of cond.conditions) {
        completeOrderingInCondition(c, getPk);
      }
      return;
  }
}

/** Append each PK column not already present (matched by name), as `asc`, in PK order.
 *  Already-present PK columns keep their existing position and direction. */
function addPrimaryKeys(pk: string[], ast: Ast): void {
  const order = (ast.orderBy ??= []);
  for (const pkCol of pk) {
    if (!order.some(([field]) => field === pkCol)) {
      order.push([pkCol, "asc"]);
    }
  }
}
