// FRAGMENT-COMPOSITION-DESIGN.md Phase 0 — composition assembles ONE AST.
//
// The load-bearing claim: a co-located fragment tree, spread via the EXISTING `sub` (nesting
// needs no new builder method) and rooted by applying the fragment to a base query, produces
// exactly the AST you'd write by hand in one shot — so it materializes once and fires one
// `/query` (no request waterfall). No merge pass, no masking: that is Phase 1+.

import { test } from "node:test";
import assert from "node:assert/strict";

import { boolean, createSchema, number, string, table } from "../src/schema.ts";
import { defineFragment, isFragment, queries } from "../src/query.ts";
import { stableKey } from "../src/key.ts";

const user = table("user").columns({ id: string(), name: string(), avatarUrl: string() }).primaryKey("id");
const comment = table("comment")
  .columns({ id: string(), issueId: string(), authorId: string(), body: string() })
  .primaryKey("id");
const issue = table("issue")
  .columns({ id: string(), title: string(), closed: boolean(), priority: number() })
  .primaryKey("id");
const schema = createSchema({ tables: [user, comment, issue] });
const q = () => queries(schema);

// Each component co-locates only its own data need; parents spread children via `sub`.
const UserAvatar = defineFragment(user, (f) => f.select("id", "name", "avatarUrl"));
const CommentRow = defineFragment(comment, (f) =>
  f.select("id", "body", "authorId").sub("author", user, { parent: ["authorId"], child: ["id"] }, UserAvatar),
);
const IssueCard = defineFragment(issue, (f) =>
  f.select("id", "title", "closed").sub("comments", comment, { parent: ["id"], child: ["issueId"] }, CommentRow),
);

test("defineFragment is a callable value carrying its table + brand", () => {
  assert.equal(typeof UserAvatar, "function");
  assert.ok(isFragment(UserAvatar));
  assert.equal(UserAvatar.table, user);
  assert.ok(!isFragment((qq: unknown) => qq), "a plain build fn is not a fragment");
  assert.ok(!isFragment({}));
});

test("a composed fragment tree assembles into ONE AST — identical to the hand-written query", () => {
  // Root the IssueCard fragment onto a base query by APPLYING it (no `include` needed in Phase 0).
  const composed = IssueCard(q().issue.where.id("issue-1")).ast();

  // The same query, written by hand in one shot.
  const handWritten = q()
    .issue.where.id("issue-1")
    .select("id", "title", "closed")
    .sub("comments", comment, { parent: ["id"], child: ["issueId"] }, (c) =>
      c
        .select("id", "body", "authorId")
        .sub("author", user, { parent: ["authorId"], child: ["id"] }, (u) => u.select("id", "name", "avatarUrl")),
    )
    .ast();

  assert.deepStrictEqual(composed, handWritten);
  // …and therefore share ONE viewKey → one cached materialization → one /query.
  assert.equal(stableKey(composed), stableKey(handWritten));
});

test("a fragment is directly usable as sub's `build` arg (nesting needs no new method)", () => {
  const ast = q().issue.sub("comments", comment, { parent: ["id"], child: ["issueId"] }, CommentRow).ast();
  assert.equal(ast.related?.length, 1);
  const comments = ast.related![0].subquery;
  assert.equal(comments.table, "comment");
  assert.equal(comments.alias, "comments");
  assert.deepStrictEqual(comments.select, ["id", "body", "authorId"]);
  // The nested fragment supplied its OWN child correlation (deep composition, no registry).
  assert.equal(comments.related?.length, 1);
  assert.equal(comments.related![0].subquery.alias, "author");
  assert.deepStrictEqual(comments.related![0].subquery.select, ["id", "name", "avatarUrl"]);
  assert.deepStrictEqual(comments.related![0].correlation, { parentField: ["authorId"], childField: ["id"] });
});

test("a fragment sub can apply parent-owned edge ordering and limits", () => {
  const ast = q()
    .issue.sub("comments", comment, { parent: ["id"], child: ["issueId"] }, CommentRow, (c) =>
      c.orderBy("id", "asc").limit(5)
    )
    .ast();

  const comments = ast.related![0].subquery;
  assert.deepStrictEqual(comments.select, ["id", "body", "authorId"]);
  assert.deepStrictEqual(comments.orderBy, [["id", "asc"]]);
  assert.equal(comments.limit, 5);
  assert.equal(comments.related?.[0].subquery.alias, "author");

  assert.throws(
    () =>
      (q().issue as any).sub(
        "comments",
        comment,
        { parent: ["id"], child: ["issueId"] },
        (c: any) => c.select("id"),
        (c: any) => c.limit(5),
      ),
    /edge callback is only supported when the child build is a Fragment/,
  );
});

test("rooting via application threads the base query's where/order/limit into the composed AST", () => {
  const ast = IssueCard(q().issue.where.closed(false).orderBy("priority", "desc").limit(20)).ast();
  assert.equal(ast.table, "issue");
  assert.deepStrictEqual(ast.select, ["id", "title", "closed"]);
  assert.deepStrictEqual(ast.orderBy, [["priority", "desc"]]);
  assert.equal(ast.limit, 20);
  assert.equal((ast.where as { op?: string }).op, "="); // closed=false survived composition
  assert.equal(ast.related?.[0].subquery.alias, "comments");
});
