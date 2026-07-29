// Why a query could not be compiled. Mirrors `rindle-d2s`'s `CompileError` (lib.rs:133) so
// the two compilers reject the same shapes with the same reasons.

export class CompileError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CompileError";
  }
}

/** A referenced table is not in the catalog. */
export function unknownTable(table: string): CompileError {
  return new CompileError(`unknown table \`${table}\``);
}

/** A `related`/EXISTS relationship's cardinality is not declared in the catalog. */
export function unknownRelationship(table: string, rel: string): CompileError {
  return new CompileError(`unknown relationship \`${rel}\` on table \`${table}\``);
}

/** A `related` subquery is missing an alias (every child must be named). */
export function missingAlias(table: string): CompileError {
  return new CompileError(`related subquery on \`${table}\` has no alias`);
}

/** An AST feature the compiler does not lower in the position it appears — rejected loudly
 *  rather than emitting silently-wrong SQL (e.g. a `start` paging bound inside EXISTS). */
export function unsupported(what: string): CompileError {
  return new CompileError(`unsupported AST feature: ${what}`);
}
