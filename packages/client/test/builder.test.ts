import { test } from "node:test";
import assert from "node:assert/strict";

import { boolean, createSchema, number, string, table, tableMeta, tableSpec } from "../src/schema.ts";
import { and, eq, ge, gt, ilike, inList, or } from "../src/operators.ts";
import { defineQuery, exists, existsNoSync, newQueryBuilder, notExists, notExistsNoSync, queries } from "../src/query.ts";

const issue = table("issue")
  .columns({ id: string(), title: string(), closed: boolean(), priority: number() })
  .primaryKey("id");
const comment = table("comment")
  .columns({ id: string(), issueID: string(), body: string() })
  .primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const q = () => queries(schema);

test("bare value where → a simple `=` condition (the eq sugar)", () => {
  const ast: any = q().issue.where.closed(false).ast();
  assert.deepStrictEqual(ast, {
    table: "issue",
    where: {
      type: "simple",
      op: "=",
      left: { type: "column", name: "closed" },
      right: { type: "literal", value: false },
    },
  });
});

test("named operator + multiple wheres AND-combine", () => {
  const ast: any = q().issue.where.priority(gt(3)).where.title(ilike("%bug%")).ast();
  assert.strictEqual(ast.where.type, "and");
  assert.deepStrictEqual(ast.where.conditions[0], {
    type: "simple",
    op: ">",
    left: { type: "column", name: "priority" },
    right: { type: "literal", value: 3 },
  });
  assert.strictEqual(ast.where.conditions[1].op, "ILIKE");
});

test("whereXxx camelCase sugar === where.field", () => {
  const a1: any = q().issue.whereClosed(false).ast();
  const a2: any = q().issue.where.closed(false).ast();
  assert.deepStrictEqual(a1, a2);
});

test("or + exists with NO closure (conditions sourced from the table proxy)", () => {
  const ast: any = q()
    .issue.where(
      or(
        issue.priority(gt(8)),
        issue.closed(eq(true)),
        exists(comment, { parent: ["id"], child: ["issueID"] }, (c) => c.where.body(ilike("%x%"))),
      ),
    )
    .ast();
  assert.strictEqual(ast.where.type, "or");
  assert.strictEqual(ast.where.conditions.length, 3);
  const ex = ast.where.conditions[2];
  assert.strictEqual(ex.type, "correlatedSubquery");
  assert.strictEqual(ex.op, "EXISTS");
  assert.deepStrictEqual(ex.related.correlation, { parentField: ["id"], childField: ["issueID"] });
  assert.strictEqual(ex.related.subquery.table, "comment");
  assert.strictEqual(ex.related.subquery.where.op, "ILIKE");
});

test("existsNoSync / notExistsNoSync stamp system: permissions on the subquery", () => {
  const ast: any = q()
    .issue.where(existsNoSync(comment, { parent: ["id"], child: ["issueID"] }))
    .ast();
  assert.strictEqual(ast.where.type, "correlatedSubquery");
  assert.strictEqual(ast.where.op, "EXISTS");
  assert.strictEqual(ast.where.related.system, "permissions");

  // A plain exists carries no system (defaults to client).
  const plain: any = q()
    .issue.where(exists(comment, { parent: ["id"], child: ["issueID"] }))
    .ast();
  assert.strictEqual(plain.where.related.system, undefined);

  // NOT EXISTS form stamps the same provenance.
  const not: any = q()
    .issue.where(notExistsNoSync(comment, { parent: ["id"], child: ["issueID"] }))
    .ast();
  assert.strictEqual(not.where.op, "NOT EXISTS");
  assert.strictEqual(not.where.related.system, "permissions");
});

test("sub emits an aliased correlated subquery in `related`; orderBy/limit/one set", () => {
  const ast: any = q()
    .issue.sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
    .orderBy("priority", "desc")
    .limit(50)
    .one()
    .ast();
  assert.deepStrictEqual(ast.orderBy, [["priority", "desc"]]);
  assert.strictEqual(ast.limit, 50);
  assert.strictEqual(ast.one, true);
  assert.strictEqual(ast.related.length, 1);
  const r = ast.related[0];
  assert.deepStrictEqual(r.correlation, { parentField: ["id"], childField: ["issueID"] });
  assert.strictEqual(r.subquery.table, "comment");
  assert.strictEqual(r.subquery.alias, "comments");
  assert.deepStrictEqual(r.subquery.orderBy, [["id", "asc"]]);
});

test("countAs marks the related subquery a count aggregate (result key is a scalar)", () => {
  const ast: any = q()
    .issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })
    .ast();
  assert.strictEqual(ast.related.length, 1);
  const r = ast.related[0];
  assert.deepStrictEqual(r.correlation, { parentField: ["id"], childField: ["issueID"] });
  assert.strictEqual(r.subquery.table, "comment");
  assert.strictEqual(r.subquery.alias, "commentCount");
  assert.strictEqual(r.subquery.aggregate, "count");
  // No precomputed flag from the builder — that is the normalized client's local rewrite.
  assert.strictEqual(r.subquery.aggregatePrecomputed, undefined);
});

test("countAs carries the child `where` (count of a filtered relationship)", () => {
  const ast: any = q()
    .issue.countAs("bugCount", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.where.body(ilike("%bug%")))
    .ast();
  const r = ast.related[0];
  assert.strictEqual(r.subquery.aggregate, "count");
  assert.strictEqual(r.subquery.where.op, "ILIKE");
  assert.strictEqual(r.subquery.where.right.value, "%bug%");
});

test("count() marks the root aggregate; a global count carries no groupBy/having/related", () => {
  const ast: any = q().issue.count().ast();
  assert.strictEqual(ast.aggregate, "count");
  assert.strictEqual(ast.groupBy, undefined);
  assert.strictEqual(ast.having, undefined);
  // The aggregate reshapes the query ITSELF — no materialized relationships (REDUCE-DESIGN §8).
  assert.strictEqual(ast.related, undefined);
});

test("groupBy + count + having lowers to the [group…, count] aggregate + post-aggregation filter", () => {
  const ast: any = q()
    .issue.groupBy("priority")
    .count()
    .having((h) => h.count(gt(10)))
    .ast();
  assert.strictEqual(ast.aggregate, "count");
  assert.deepStrictEqual(ast.groupBy, ["priority"]);
  // `having` addresses the aggregate's OUTPUT columns — here the synthetic `count`.
  assert.deepStrictEqual(ast.having, {
    type: "simple",
    op: ">",
    left: { type: "column", name: "count" },
    right: { type: "literal", value: 10 },
  });
});

test("having can predicate a group column too, and combine clauses with and()", () => {
  const ast: any = q()
    .issue.groupBy("priority")
    .count()
    .having((h) => and(h.priority(ge(2)), h.count(gt(5))))
    .ast();
  assert.strictEqual(ast.having.type, "and");
  assert.strictEqual(ast.having.conditions[0].left.name, "priority");
  assert.strictEqual(ast.having.conditions[0].op, ">=");
  assert.strictEqual(ast.having.conditions[1].left.name, "count");
});

test("compound groupBy chains into a multi-column key", () => {
  const ast: any = q().issue.groupBy("priority").groupBy("closed").count().ast();
  assert.deepStrictEqual(ast.groupBy, ["priority", "closed"]);
});

test("where filters BELOW the reduce and coexists with a HAVING above it", () => {
  const ast: any = q()
    .issue.where.closed(false)
    .groupBy("priority")
    .count()
    .having((h) => h.count(gt(1)))
    .ast();
  // `where` (closed = false) is pre-aggregation; `having` (count > 1) is post-aggregation.
  assert.strictEqual(ast.where.op, "=");
  assert.strictEqual(ast.where.left.name, "closed");
  assert.strictEqual(ast.aggregate, "count");
  assert.strictEqual(ast.having.left.name, "count");
});

test("groupBy()/having() without count() throw (the engine would silently ignore them)", () => {
  assert.throws(() => q().issue.groupBy("priority").ast(), /require count\(\)/);
  assert.throws(
    () => q().issue.groupBy("priority").having((h) => h.priority(ge(1))).ast(),
    /require count\(\)/,
  );
});

// --- filtering a PARENT by a child aggregate: .having(alias, op, n) ----------

test("having(alias, op, n) gates the parent by a child count — a hidden EXISTS over the same count + a HAVING", () => {
  const ast: any = q()
    .issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })
    .having("commentCount", ">", 10)
    .ast();
  // The display aggregate is untouched (still its own slot, no HAVING).
  assert.strictEqual(ast.related.length, 1);
  assert.strictEqual(ast.related[0].subquery.alias, "commentCount");
  assert.strictEqual(ast.related[0].subquery.having, undefined);
  // The gate is a `where` EXISTS over the SAME correlation + count aggregate, under a hidden
  // (slot-distinct) alias, carrying `HAVING count > 10`.
  assert.strictEqual(ast.where.type, "correlatedSubquery");
  assert.strictEqual(ast.where.op, "EXISTS");
  const gate = ast.where.related;
  assert.deepStrictEqual(gate.correlation, { parentField: ["id"], childField: ["issueID"] });
  assert.strictEqual(gate.subquery.alias, "__having_commentCount");
  assert.strictEqual(gate.subquery.aggregate, "count");
  assert.deepStrictEqual(gate.subquery.having, {
    type: "simple",
    op: ">",
    left: { type: "column", name: "count" },
    right: { type: "literal", value: 10 },
  });
});

test("having(alias, …) clones the child `where` into the gate (count of a FILTERED relationship)", () => {
  const ast: any = q()
    .issue.countAs("bugCount", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.where.body(ilike("%bug%")))
    .having("bugCount", ">", 2)
    .ast();
  const gate = ast.where.related;
  // The gate clones the display child — including its `where` — so both count the same rows.
  assert.strictEqual(gate.subquery.aggregate, "count");
  assert.strictEqual(gate.subquery.where.op, "ILIKE");
  assert.strictEqual(gate.subquery.where.right.value, "%bug%");
});

test("having(alias, …) AND-combines with a parent where", () => {
  const ast: any = q()
    .issue.where.closed(false)
    .countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })
    .having("commentCount", ">", 5)
    .ast();
  assert.strictEqual(ast.where.type, "and");
  assert.strictEqual(ast.where.conditions[0].left.name, "closed");
  assert.strictEqual(ast.where.conditions[1].type, "correlatedSubquery");
  assert.strictEqual(ast.where.conditions[1].related.subquery.having.right.value, 5);
});

test("having(alias, …) on an unknown aggregate alias throws", () => {
  const bad: any = q().issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] });
  assert.throws(() => bad.having("nope", ">", 1).ast(), /no countAs\("nope"/);
});

test("start emits a name-keyed Bound; exclusive defaults to false, opts.exclusive flips it", () => {
  const inclusive: any = q().issue.orderBy("priority", "desc").start({ priority: 3, id: "abc" }).ast();
  assert.deepStrictEqual(inclusive.start, { row: { priority: 3, id: "abc" }, exclusive: false });

  const exclusive: any = q().issue.start({ id: "abc" }, { exclusive: true }).ast();
  assert.deepStrictEqual(exclusive.start, { row: { id: "abc" }, exclusive: true });
});

test("notExists + inList + and", () => {
  const ast: any = q()
    .issue.where(
      and(issue.priority(inList([1, 2, 3])), notExists(comment, { parent: ["id"], child: ["issueID"] })),
    )
    .ast();
  assert.strictEqual(ast.where.type, "and");
  assert.strictEqual(ast.where.conditions[0].op, "IN");
  assert.deepStrictEqual(ast.where.conditions[0].right.value, [1, 2, 3]);
  assert.strictEqual(ast.where.conditions[1].op, "NOT EXISTS");
});

test("tableSpec derives columns + primary-key indices for registerTable", () => {
  assert.deepStrictEqual(tableSpec(tableMeta(issue)), {
    columns: ["id", "title", "closed", "priority"],
    primaryKey: [0],
  });
});

test("defineQuery stamps name and args, and chaining preserves the remote identity", () => {
  const qb = newQueryBuilder(schema);
  const issuePageQueries = {
    issuesForProject: defineQuery("issuesForProject", (args: { projectID: number }) =>
      qb.issue.where.priority(args.projectID).where.closed(false)),
  };

  const base = issuePageQueries.issuesForProject({ projectID: 42 });
  assert.strictEqual(base.name, "issuesForProject");
  assert.deepStrictEqual(base.args, { projectID: 42 });

  const refined = base.where.id("issue-1");
  assert.strictEqual(refined.name, "issuesForProject");
  assert.deepStrictEqual(refined.args, { projectID: 42 });
  assert.strictEqual((refined.ast() as any).where.type, "and");
});

test("defineQuery: client call validates+builds+stamps; resolve rebuilds the SAME AST from raw wire args", () => {
  const qb = newQueryBuilder(schema);
  const calls: unknown[] = [];
  const issuesPage = defineQuery(
    "issuesPage",
    (raw): { limit: number } => {
      calls.push(raw);
      const { limit } = raw as { limit?: unknown };
      if (typeof limit !== "number" || limit < 1) throw new Error("bad limit");
      return { limit };
    },
    ({ limit }) => qb.issue.where.closed(false).limit(limit),
  );

  // Client call: stamped with the wire identity (name + the raw args it was called with).
  const client = issuesPage({ limit: 25 });
  assert.strictEqual(issuesPage.queryName, "issuesPage");
  assert.strictEqual(client.name, "issuesPage");
  assert.deepStrictEqual(client.args, { limit: 25 });

  // Server: resolve the SAME raw wire args to the authoritative query — byte-identical AST.
  const server = issuesPage.resolve({ limit: 25 });
  assert.deepStrictEqual(server.ast(), client.ast());

  // The validator ran on both tiers (client guard + server authority).
  assert.deepStrictEqual(calls, [{ limit: 25 }, { limit: 25 }]);

  // A malformed wire arg is rejected by resolve (the server's untrusted-input guard).
  assert.throws(() => issuesPage.resolve({ limit: 0 }), /bad limit/);
});

test("defineQuery: no-arg query is callable with no args and stamps null", () => {
  const qb = newQueryBuilder(schema);
  const allIssues = defineQuery("allIssues", () => qb.issue.orderBy("id", "asc"));

  const view = allIssues();
  assert.strictEqual(view.name, "allIssues");
  assert.strictEqual(view.args, null);
  assert.deepStrictEqual(allIssues.resolve(null).ast(), view.ast());
});

test("defineQuery: ctx-scoped query is symmetric, off-wire, and built from each tier's OWN ctx", () => {
  const qb = newQueryBuilder(schema);
  const myIssues = defineQuery(
    "myIssues",
    (raw): { limit: number } => ({ limit: (raw as { limit: number }).limit }),
    ({ limit }, ctx: { user: string }) => qb.issue.where.id(ctx.user).limit(limit),
  );

  // The client passes its OWN session ctx at the callsite; the wire identity excludes it.
  const client = myIssues({ limit: 5 }, { user: "alice" });
  assert.strictEqual(client.name, "myIssues");
  assert.deepStrictEqual(client.args, { limit: 5 }); // ctx is NOT stamped → never crosses the wire

  // The server forwards its AUTHORITATIVE ctx to resolve; same ctx → byte-identical AST.
  assert.deepStrictEqual(myIssues.resolve({ limit: 5 }, { user: "alice" }).ast(), client.ast());

  // The SAME wire args under a different authoritative ctx build a different AST — the server scopes
  // to its own principal, never the client's claim.
  assert.notDeepStrictEqual(myIssues.resolve({ limit: 5 }, { user: "bob" }).ast(), client.ast());

  // Type-level (never executed): ctx is REQUIRED for this query, so omitting it is a compile error.
  () => {
    // @ts-expect-error ctx is required
    myIssues({ limit: 5 });
  };
});

test("defineQuery: optional ctx — omit for the broad (anonymous) AST, pass for the scoped one", () => {
  const qb = newQueryBuilder(schema);
  const issues = defineQuery(
    "issues",
    (raw): { limit: number } => ({ limit: (raw as { limit: number }).limit }),
    ({ limit }, ctx?: { user?: string }) =>
      ctx?.user ? qb.issue.where.id(ctx.user).limit(limit) : qb.issue.limit(limit),
  );

  const broad = issues({ limit: 3 }); // no ctx → broad
  const scoped = issues({ limit: 3 }, { user: "alice" }); // ctx → scoped
  assert.notDeepStrictEqual(scoped.ast(), broad.ast());

  // The anonymous state is symmetric: a client omitting ctx == the server forwarding an anon ctx.
  assert.deepStrictEqual(issues.resolve({ limit: 3 }, { user: undefined }).ast(), broad.ast());
  assert.deepStrictEqual(issues.resolve({ limit: 3 }, { user: "alice" }).ast(), scoped.ast());
});

test("defineQuery: a context-free query rejects a ctx argument (type-level)", () => {
  const qb = newQueryBuilder(schema);
  const plain = defineQuery("plain", (raw): { limit: number } => raw as { limit: number }, ({ limit }) =>
    qb.issue.limit(limit),
  );
  () => {
    // @ts-expect-error a context-free query takes no ctx argument
    plain({ limit: 1 }, { user: "alice" });
  };
});

test("exists with { scalar: true } sets the scalar flag; plain exists omits it", () => {
  const scalarAst: any = q()
    .issue.where(exists(comment, { parent: ["id"], child: ["issueID"] }, undefined, { scalar: true }))
    .ast();
  assert.strictEqual(scalarAst.where.type, "correlatedSubquery");
  assert.strictEqual(scalarAst.where.scalar, true);

  const plainAst: any = q()
    .issue.where(exists(comment, { parent: ["id"], child: ["issueID"] }))
    .ast();
  assert.strictEqual(plainAst.where.scalar, undefined);

  const notScalarAst: any = q()
    .issue.where(notExists(comment, { parent: ["id"], child: ["issueID"] }, undefined, { scalar: true }))
    .ast();
  assert.strictEqual(notScalarAst.where.op, "NOT EXISTS");
  assert.strictEqual(notScalarAst.where.scalar, true);
});
