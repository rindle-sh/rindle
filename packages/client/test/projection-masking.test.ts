// FRAGMENT-COMPOSITION-DESIGN.md Phase 2 — projection → type narrowing (data masking, §6 / §10.6).
//
// `select(...)` narrows the result row TYPE to exactly the named columns; a fragment ref (and thus
// `useFragment`) exposes only what the fragment declared. This is a COMPILE-TIME guarantee, so the
// load-bearing assertions are the `@ts-expect-error` lines below: the package's `test` script runs
// `tsc --noEmit` first, so if masking ever stops narrowing, the unused directive fails the build.
// The runtime `assert`s only keep the test a real (executing) test; nothing about projection changed
// at runtime (the engine/wire projection landed earlier — this phase is purely the type).

import { test } from "node:test";
import assert from "node:assert/strict";

import { boolean, createSchema, number, string, table } from "../src/schema.ts";
import { defineFragment, queries } from "../src/query.ts";
import type { FragmentData, FragmentRef } from "../src/query.ts";

const user = table("user").columns({ id: string(), name: string(), avatarUrl: string() }).primaryKey("id");
const issue = table("issue")
  .columns({ id: string(), title: string(), closed: boolean(), priority: number() })
  .primaryKey("id");
const schema = createSchema({ tables: [user, issue] });

// ---------------------------------------------------------------- query-level: select() narrows
const projected = queries(schema).issue.select("id", "title");
type ProjRow = ReturnType<(typeof projected)["materialize"]>["data"][number];

// `where`/`orderBy` still see the full column set even though the ROW is masked: filtering by a
// column you didn't project is legal (and compiles), it just doesn't come back in the result.
const _filterUnselected = queries(schema).issue.select("id").where.closed(false).orderBy("priority", "desc");
void _filterUnselected;

// ---------------------------------------------------------------- fragment-level: masked refs
const UserBadge = defineFragment(user, (f) => f.select("id", "name"));
const FullUser = defineFragment(user, (f) => f); // no select ⇒ all columns (§10.6 kept, type-honest)
const IssueCard = defineFragment(issue, (f) =>
  f.select("id", "title").sub("author", user, { parent: ["id"], child: ["id"] }, UserBadge, (u) =>
    u.orderBy("name", "asc").limit(1)
  ),
);

type BadgeRef = FragmentRef<typeof UserBadge>;
type BadgeData = FragmentData<typeof UserBadge>;
type FullUserData = FragmentData<typeof FullUser>;
type CardData = FragmentData<typeof IssueCard>;

test("select() narrows the query's result row to exactly the projection", () => {
  const seeRow = (r: ProjRow) => {
    const id: string = r.id;
    const title: string = r.title;
    // @ts-expect-error `closed` was not selected — masked off the projected row type.
    void r.closed;
    return id + title;
  };
  void seeRow;
  assert.ok(true);
});

test("a fragment ref is opaque; fragment data exposes only selected columns", () => {
  const seeRef = (r: BadgeRef) => {
    // @ts-expect-error refs are handles, not already-projected row data.
    void r.name;
    return r.table;
  };
  const seeBadge = (r: BadgeData) => {
    const id: string = r.id;
    const name: string = r.name;
    // @ts-expect-error `avatarUrl` was not selected — the fragment can't read a sibling's column.
    void r.avatarUrl;
    return id + name;
  };
  void seeRef;
  void seeBadge;
  assert.ok(true);
});

test("a fragment with no select stays the full row (no-select ⇒ all columns, §10.6)", () => {
  const seeFull = (r: FullUserData) => r.id + r.name + r.avatarUrl; // all three present, no error
  void seeFull;
  assert.ok(true);
});

test("nested fragment sub returns a child ref, not child-owned payload", () => {
  const seeCard = (r: CardData) => {
    const id: string = r.id;
    const title: string = r.title;
    // @ts-expect-error `closed` not selected on the parent issue.
    void r.closed;
    const authorRef: BadgeRef = r.author[0];
    // @ts-expect-error child-owned fields are read through useFragment(UserBadge, authorRef).
    void r.author[0].name;
    return id + title + authorRef.table;
  };
  void seeCard;
  assert.ok(true);
});
