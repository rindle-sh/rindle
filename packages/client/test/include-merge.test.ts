// FRAGMENT-COMPOSITION-DESIGN.md Phase 1 — same-node composition: `include(fragment)` + the merge
// pass (§5). Two fragments contributing to the SAME node merge into one canonical AST: unioned
// `select`, relationships merged by `alias` (recursively), conflicting same-alias edges rejected
// (§10.2), root `where` from an included fragment rejected (§10.3), and include ORDER irrelevant
// (§10.5 — `include(A).include(B)` and `include(B).include(A)` produce one identical viewKey).

import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, number, string, table, boolean } from "../src/schema.ts";
import { defineFragment, queries } from "../src/query.ts";
import type { FragmentData } from "../src/query.ts";
import { stableKey } from "../src/key.ts";

const user = table("user").columns({ id: string(), name: string(), avatarUrl: string() }).primaryKey("id");
const tag = table("tag").columns({ id: string(), issueId: string(), name: string() }).primaryKey("id");
const comment = table("comment").columns({ id: string(), issueId: string(), body: string() }).primaryKey("id");
const issue = table("issue")
  .columns({ id: string(), title: string(), closed: boolean(), priority: number(), ownerId: string() })
  .primaryKey("id");
const schema = createSchema({ tables: [user, tag, comment, issue] });
const q = () => queries(schema);

// Co-located fragments, each over `issue`, declaring just its own slice.
const Header = defineFragment(issue, (f) => f.select("id", "title"));
const Meta = defineFragment(issue, (f) => f.select("priority", "closed"));
const OwnerCard = defineFragment(issue, (f) =>
  f.select("ownerId").sub("owner", user, { parent: ["ownerId"], child: ["id"] }, (u) => u.select("id", "name")),
);
const TagsCard = defineFragment(issue, (f) =>
  f.select("id").sub("tags", tag, { parent: ["id"], child: ["issueId"] }, (t) => t.select("id", "name")),
);
// Same `owner` edge as OwnerCard, but a different child projection — should DEDUP-MERGE the columns.
const OwnerAvatar = defineFragment(issue, (f) =>
  f.sub("owner", user, { parent: ["ownerId"], child: ["id"] }, (u) => u.select("id", "avatarUrl")),
);

test("include() roots a fragment onto a base query — projects its selection, keeps the base filter", () => {
  const ast = q().issue.where.id("issue-1").include(Header).ast();
  assert.equal(ast.table, "issue");
  assert.deepStrictEqual(ast.select, ["id", "title"]);
  assert.equal((ast.where as { op?: string }).op, "="); // the base's filter survives
});

test("two fragments on the same node union their projections (sorted, deduped)", () => {
  const ast = q().issue.include(Header).include(Meta).ast();
  // {id,title} ∪ {priority,closed} → canonical (sorted) union
  assert.deepStrictEqual(ast.select, ["closed", "id", "priority", "title"]);
});

test("distinct relationship aliases are kept side by side, ordered by alias", () => {
  const ast = q().issue.include(OwnerCard).include(TagsCard).ast();
  assert.deepStrictEqual(ast.select, ["id", "ownerId"]); // ownerId ∪ id
  assert.equal(ast.related?.length, 2);
  assert.deepStrictEqual(
    ast.related?.map((r) => r.subquery.alias),
    ["owner", "tags"],
  );
  const owner = ast.related?.find((r) => r.subquery.alias === "owner");
  assert.deepStrictEqual(owner?.subquery.select, ["id", "name"]);
  assert.deepStrictEqual(owner?.correlation, { parentField: ["ownerId"], childField: ["id"] });
});

test("the SAME relationship alias spread by two fragments merges recursively (child select unioned)", () => {
  const ast = q().issue.include(OwnerCard).include(OwnerAvatar).ast();
  assert.equal(ast.related?.length, 1, "one merged `owner` edge, not two");
  const owner = ast.related?.[0];
  assert.equal(owner?.subquery.alias, "owner");
  assert.deepStrictEqual(owner?.subquery.select, ["avatarUrl", "id", "name"]); // {id,name} ∪ {id,avatarUrl}
});

test("a relationship-only fragment contributes its edge but NO root columns (root select is a union)", () => {
  // SubOnly adds a `tags` edge and selects no root columns — including it must NOT widen the base's
  // projection to "all" (that would defeat masking and silently re-narrow on a later `.select`).
  const SubOnly = defineFragment(issue, (f) =>
    f.sub("tags", tag, { parent: ["id"], child: ["issueId"] }, (t) => t.select("id", "name")),
  );
  const narrowed = q().issue.select("id").include(SubOnly).ast();
  assert.deepStrictEqual(narrowed.select, ["id"], "the base's projection is preserved, not blown open");
  assert.equal(narrowed.related?.length, 1, "the edge is still added");
  // …and chaining a `.select` after the include keeps unioning (it doesn't re-narrow):
  const more = q().issue.include(SubOnly).select("id", "title").ast();
  assert.deepStrictEqual(more.select, ["id", "title"]);
  // Only when nothing anywhere selects does the projection stay "all columns":
  const all = q().issue.include(SubOnly).ast();
  assert.equal(all.select, undefined, "no `select` anywhere ⇒ all columns");
});

test("conflicting same-alias edges are rejected (§10.2)", () => {
  const OwnerWrongCorr = defineFragment(issue, (f) =>
    f.sub("owner", user, { parent: ["id"], child: ["id"] }, (u) => u.select("id")),
  );
  assert.throws(
    () => q().issue.include(OwnerCard).include(OwnerWrongCorr),
    /conflicting definitions for relationship "owner"/,
  );
});

test("an included fragment may not add a root `where` (§10.3)", () => {
  const Filtered = defineFragment(issue, (f) => f.select("id").where.closed(false));
  assert.throws(() => q().issue.include(Filtered), /may not add a root `where`/);
});

test("an included fragment may not set a root orderBy/limit", () => {
  const Ordered = defineFragment(issue, (f) => f.select("id").orderBy("priority", "desc"));
  const Limited = defineFragment(issue, (f) => f.select("id").limit(10));
  assert.throws(() => q().issue.include(Ordered), /orderBy/);
  assert.throws(() => q().issue.include(Limited), /limit/);
});

const CommentFrag = defineFragment(comment, (f) => f.select("id", "body"));
// Type-level: a cross-table include yields `never` (so chaining/reading off it fails to compile).
// Declared, never called — so it does not trigger the runtime throw at import.
const _crossTableYieldsNever = () => {
  const r: never = q().issue.include(CommentFrag); // compiles ONLY because the result type is `never`
  return r;
};
void _crossTableYieldsNever;
test("include() rejects a fragment over a different table (runtime throw)", () => {
  assert.throws(() => q().issue.include(CommentFrag), /over "comment".*over "issue"|same table/i);
});

test("include ORDER does not matter — same canonical AST → one viewKey (§10.5)", () => {
  const a = q().issue.where.id("x").include(OwnerCard).include(TagsCard).include(Meta).ast();
  const b = q().issue.where.id("x").include(Meta).include(TagsCard).include(OwnerCard).ast();
  assert.deepStrictEqual(a, b);
  assert.equal(stableKey(a), stableKey(b));
});

test("masking flows through include: result is the UNION of both fragments' selections + rels", () => {
  const rooted = q().issue.where.id("x").include(OwnerCard).include(TagsCard);
  type Row = ReturnType<(typeof rooted)["materialize"]>["data"][number];
  const see = (r: Row) => {
    const id: string = r.id;
    const ownerId: string = r.ownerId; // selected by OwnerCard
    // @ts-expect-error `title` is selected by neither fragment — masked off.
    void r.title;
    const ownerName: string = r.owner[0].name; // inline sub build, not a fragment ref
    const tagName: string = r.tags[0].name; // inline sub build, not a fragment ref
    return id + ownerId + ownerName + tagName;
  };
  void see;
  assert.ok(true);
});

// A fragment can be DEFINED by `include`-merging other fragments on the same node (real
// co-location: one parent fragment assembled from its children). `include` is what makes this
// typecheck — applying a fragment requires a fresh `Query<C>`, but `include` accepts any query.
const IssueCard = defineFragment(issue, (f) => f.include(OwnerCard).include(TagsCard));
type IssueCardData = FragmentData<typeof IssueCard>;
test("a fragment built by include-merging fragments carries the union shape", () => {
  const see = (r: IssueCardData) => {
    // @ts-expect-error `closed` selected by neither child fragment.
    void r.closed;
    return r.id + r.ownerId + r.owner[0].name + r.tags[0].name;
  };
  void see;
  assert.ok(true);
});
