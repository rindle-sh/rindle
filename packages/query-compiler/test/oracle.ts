// The offline differential oracle (§9), realized with Node's built-in `node:sqlite` — zero
// external infrastructure. It compiles an `Ast` to the SQLite dialect, runs the one SELECT
// against a seeded in-memory fixture, and returns the parsed nested-JSON result. Because the
// SQLite dialect shares the whole walker with the Postgres one, proving the walker here
// transfers to Postgres, leaving only the leaf/cast delta for the live-PG lane (M3/Phase D).

import { DatabaseSync } from "node:sqlite";

import { compile, type Catalog } from "../src/index.ts";
import type { Ast } from "../src/index.ts";

// ── the fixture: a tiny issue tracker (int PKs / text — walker-focused, not cast-focused) ──

export const execCatalog: Catalog = {
  tables: {
    user: {
      columns: ["id", "name"],
      primaryKey: ["id"],
      columnTypes: {
        id: { type: "int4", isEnum: false, isArray: false },
        name: { type: "text", isEnum: false, isArray: false },
      },
      relationships: { authoredIssues: "many" },
    },
    issue: {
      columns: ["id", "title", "authorId", "priority", "closed", "assignee"],
      primaryKey: ["id"],
      columnTypes: {
        id: { type: "int4", isEnum: false, isArray: false },
        title: { type: "text", isEnum: false, isArray: false },
        authorId: { type: "int4", isEnum: false, isArray: false },
        priority: { type: "int4", isEnum: false, isArray: false },
        closed: { type: "int4", isEnum: false, isArray: false },
        assignee: { type: "text", isEnum: false, isArray: false },
      },
      relationships: { author: "one", comments: "many" },
    },
    comment: {
      columns: ["id", "issueId", "body", "authorId"],
      primaryKey: ["id"],
      columnTypes: {
        id: { type: "int4", isEnum: false, isArray: false },
        issueId: { type: "int4", isEnum: false, isArray: false },
        body: { type: "text", isEnum: false, isArray: false },
        authorId: { type: "int4", isEnum: false, isArray: false },
      },
      relationships: { author: "one" },
    },
  },
};

const DDL = `
CREATE TABLE "user" ("id" INTEGER PRIMARY KEY, "name" TEXT);
CREATE TABLE "issue" ("id" INTEGER PRIMARY KEY, "title" TEXT, "authorId" INTEGER, "priority" INTEGER, "closed" INTEGER, "assignee" TEXT);
CREATE TABLE "comment" ("id" INTEGER PRIMARY KEY, "issueId" INTEGER, "body" TEXT, "authorId" INTEGER);
`;

const SEED: Record<string, Record<string, unknown>[]> = {
  user: [
    { id: 1, name: "Alice" },
    { id: 2, name: "Bob" },
    { id: 3, name: "Carol" },
    { id: 4, name: "Dave" }, // authored nothing → excluded by EXISTS
  ],
  issue: [
    { id: 10, title: "Login bug", authorId: 1, priority: 3, closed: 0, assignee: "zeta" },
    { id: 11, title: "Slow query", authorId: 1, priority: 1, closed: 1, assignee: null },
    { id: 12, title: "UI polish", authorId: 2, priority: 2, closed: 0, assignee: null },
    { id: 13, title: "Crash", authorId: 3, priority: 3, closed: 0, assignee: "alpha" },
  ],
  comment: [
    { id: 100, issueId: 10, body: "me too", authorId: 2 },
    { id: 101, issueId: 10, body: "fix incoming", authorId: 1 },
    { id: 102, issueId: 12, body: "nice", authorId: 3 },
    // issues 11 and 13 have no comments → empty array / count 0
  ],
};

let cached: DatabaseSync | null = null;

function database(): DatabaseSync {
  if (cached) return cached;
  const db = new DatabaseSync(":memory:");
  // Make `LIKE` case-sensitive so it matches the engine's matcher (and the compiler's
  // `lower(x) LIKE lower(y)` ILIKE folding is meaningful) — as `rindle-d2s`'s harness does.
  db.exec("PRAGMA case_sensitive_like = ON;");
  db.exec(DDL);
  for (const [table, rows] of Object.entries(SEED)) {
    if (rows.length === 0) continue;
    const cols = Object.keys(rows[0]);
    const stmt = db.prepare(
      `INSERT INTO "${table}" (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`,
    );
    for (const row of rows) stmt.run(...cols.map((k) => row[k] as null | number | string));
  }
  cached = db;
  return db;
}

/** Compile `ast` to the SQLite dialect and return `{ sql, params }` (no execution). */
export function compiled(ast: Ast): { sql: string; params: unknown[] } {
  return compile(ast, execCatalog, { dialect: "sqlite" });
}

/** Compile `ast` to SQLite, run it against the seeded fixture, and return the parsed nested
 *  JSON — a JS array for a plural root, or an object / `null` for a `.one()` root. */
export function run(ast: Ast): unknown {
  const { sql, params } = compiled(ast);
  const row = database().prepare(sql).get(...(params as (null | number | string)[])) as
    | { rindle_result: string | null }
    | undefined;
  if (!row) return undefined;
  return JSON.parse(row.rindle_result as string);
}
