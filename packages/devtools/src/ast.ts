// Read-only AST helpers for the queries inspector (DEBUG-TOOLS-BROWSER-DESIGN §4.2): the table
// footprint (for the pending-axis intersection) and a one-line human summary. Pure functions over
// the `@rindle/client` `Ast` — no engine, no side effects.

import type { Ast, Condition, ValuePosition } from "@rindle/client";

/** Every base table this query can read: the root, each `related` subquery, and any correlated
 *  subquery in the `where` tree (recursively). Mirrors the optimistic backend's `queryTables`
 *  derivation (DEBUG-TOOLS-BROWSER-DESIGN §4.2 — a `count(comments)` subquery names `comment`, so a
 *  comment mutation flips this query's pending axis). */
export function collectTables(ast: Ast, into: Set<string> = new Set()): Set<string> {
  into.add(ast.table);
  for (const r of ast.related ?? []) collectTables(r.subquery, into);
  if (ast.where) collectTablesFromCondition(ast.where, into);
  return into;
}

function collectTablesFromCondition(cond: Condition, into: Set<string>): void {
  if (cond.type === "and" || cond.type === "or") {
    for (const c of cond.conditions) collectTablesFromCondition(c, into);
  } else if (cond.type === "correlatedSubquery") {
    collectTables(cond.related.subquery, into);
  }
}

/** A compact, one-line description of a query AST for the inspector header — best-effort and
 *  truncation-friendly (the full AST is available for a collapsible pretty-print alongside). */
export function summarizeAst(ast: Ast): string {
  const parts: string[] = [ast.alias ? `${ast.table} as ${ast.alias}` : ast.table];
  if (ast.aggregate) parts.push(ast.aggregate === "count" ? "count(*)" : ast.aggregate);
  if (ast.select?.length) parts.push(`select(${ast.select.join(", ")})`);
  if (ast.where) parts.push(`where(${summarizeCondition(ast.where)})`);
  if (ast.orderBy?.length) parts.push(`order(${ast.orderBy.map(([f, d]) => `${f} ${d}`).join(", ")})`);
  if (ast.limit !== undefined) parts.push(`limit ${ast.limit}`);
  if (ast.one) parts.push("one");
  if (ast.related?.length) {
    const rels = ast.related.map((r) => {
      const sub = r.subquery;
      const tag = sub.aggregate ? `${sub.alias ?? sub.table}(count)` : (sub.alias ?? sub.table);
      return tag;
    });
    parts.push(`{ ${rels.join(", ")} }`);
  }
  return parts.join(" ");
}

function summarizeCondition(cond: Condition): string {
  switch (cond.type) {
    case "simple":
      return `${valueStr(cond.left)} ${cond.op} ${valueStr(cond.right)}`;
    case "and":
      return cond.conditions.map(summarizeCondition).join(" AND ");
    case "or":
      return `(${cond.conditions.map(summarizeCondition).join(" OR ")})`;
    case "correlatedSubquery":
      return `${cond.op} ${cond.related.subquery.table}`;
  }
}

function valueStr(v: ValuePosition): string {
  if (v.type === "column") return v.name;
  const lit = v.value;
  if (typeof lit === "string") return JSON.stringify(lit);
  if (Array.isArray(lit)) return `[${lit.length}]`;
  return String(lit);
}
