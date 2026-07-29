<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/mutators.md (https://rindle.sh/docs/mutators)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# Isomorphic mutators

> One generator body per write, run on both tiers — the browser drives it synchronously as the optimistic prediction, the API server drives the same body into SQL as the authority. The op vocabulary, reads, the acting principal, and the determinism rules.

A write in a Rindle app is a call to a **named mutator** — and a mutator is written
**once**, then run on **both** tiers. The browser drives it as the optimistic
prediction; the API server drives the *same body* as the authority. Because the two
tiers execute one function, the prediction and the commit can't drift apart — there
is no hand-written SQL twin to keep in sync, and only `(name, args)` ever crosses
the wire.

This page is the deep-dive on that contract. The
[synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) walks the surrounding module
(`shared/app-def.ts`) end to end; here is everything the walk-through glosses.

## One body, two drivers

A mutator is a **generator** that `yield`s logical write ops
(`yield tx.insert(...)`) instead of touching a database directly. A generator is
neither sync nor async — that's the trick. Each tier supplies its own driver:

- The **browser** drives the body **synchronously** against its local wasm engine.
  Every affected view updates before the call returns. The prediction is
  **re-invoked on every rebase**, so the body must be deterministic (rules below).
- The **API server** drives the same body **asynchronously** inside an
  authoritative transaction, rendering each yielded op to dialect SQL. See
  [the API server](https://rindle.sh/docs/api-server#driving-the-shared-mutators) for the wiring.

Pair each body with the schema for its args at one site using `shared(args, gen)`,
and bind `shared` to your schema with `defineMutators` so every op checks its
table, column names, value types, and pk columns at compile time:

```ts
// shared/app-def.ts — imported by BOTH the browser and the API server
import { defineMutators } from "@rindle/client";
import type { MutationGen, MutatorCtx } from "@rindle/client";
import type { ClientRegistry } from "@rindle/optimistic";
import { z } from "zod";
import { schema } from "./schema.gen.ts";

const { shared } = defineMutators(schema);

export const createIssueArgs = z.object({
  id: z.string(), title: z.string(), status: z.string(), priority: z.string(), createdAt: z.number(),
});
export type CreateIssueArgs = z.infer<typeof createIssueArgs>;

// Normalization runs INSIDE the one body, so both tiers normalize identically.
export function cleanTitle(t: string): string { return t.trim().slice(0, 200); }

export const mutators = {
  createIssue: shared(createIssueArgs, function* (tx, a: CreateIssueArgs, ctx: MutatorCtx): MutationGen {
    const title = cleanTitle(a.title);
    if (!title) return;                                  // a no-op prediction is fine
    yield tx.insertIgnore("user", { id: ctx.user, name: ctx.user });
    yield tx.insert("issue", {
      id: a.id, title, status: a.status, priority: a.priority,
      ownerId: ctx.user, createdAt: a.createdAt, updatedAt: a.createdAt,
    });
  }),
  setStatus: shared(
    z.object({ id: z.string(), status: z.string(), updatedAt: z.number() }),
    function* (tx, a): MutationGen {
      yield tx.update("issue", { id: a.id, status: a.status, updatedAt: a.updatedAt });
    },
  ),
} satisfies ClientRegistry;
```

The arg schema does double duty: the **server** parses the untrusted wire args
through it before the body runs (a failed parse is a hard reject), and **both**
tiers derive the arg *type* from it with `z.infer`. The client trusts its typed
callsites and skips the parse.

## The op vocabulary

`tx` is a stateless **effect factory** — every method just *builds* an op to
`yield` (it performs no I/O), and ops are keyed by column name, independent of
column order:

- `yield tx.insert(table, row)` — a **full** row (every column present).
- `yield tx.update(table, row)` — the pk plus **only the columns that change**; a
  missing row is a no-op.
- `yield tx.upsert(table, row)` — a full row; replaces the non-pk columns on a pk
  conflict.
- `yield tx.insertIgnore(table, row)` — a full row; does nothing on a pk conflict
  (renders `ON CONFLICT DO NOTHING` server-side). The isomorphic twin of
  `if (!exists) insert`.
- `yield tx.delete(table, { pk })` — pk columns only.

A mutator that spans several tables just `yield`s each op in turn. **Helpers**
follow one convention: a multi-op (or reading) helper is itself a generator and is
spread with `yield*` (`yield* applyTags(tx, a)`); a single-op helper *returns* one
op and is plain-`yield`ed. Prefer returning ops for single-op helpers — a
forgotten `yield` leaves an obvious dead statement, where a forgotten `yield*` on
a generator is a silent no-op.

## Reads inside a mutator

A read is a yield whose **expression evaluates to the result** — the one yield
suspends the generator while the driver resolves it and feeds it back:

```ts
bumpVersion: shared(z.object({ id: z.string() }), function* (tx, a): MutationGen {
  const cur = (yield tx.row("issue", { id: a.id })) as Issue | undefined;
  if (!cur) return;
  yield tx.update("issue", { id: a.id, version: cur.version + 1 });
}),
```

- `yield tx.row(table, { pk })` — a point read by primary key.
- `yield tx.query(builder)` — a full ad-hoc query (`where` / `orderBy` / `limit` /
  joins) evaluating to its **rows** — always an array, in the query's order (a
  root `.one()` is not unwrapped; take `[0]`). Build it with the same
  `newQueryBuilder(schema)` your app-def exports.
- `yield tx.all([tx.row(...), tx.row(...)])` — fan point reads out; resolved
  concurrently on the server, in array order on the client, results returned in
  the **same order** on both tiers so the body stays deterministic.

Every read sees the **current base plus this transaction's own staged writes**
(read-your-writes) — on the browser engine and in the server's authoritative
transaction alike. That symmetry is what makes read-dependent writes correct under
rebase: the body replays the *intent* against whatever state it lands on, not a
stale effect. (Driving mutators against a Postgres authority instead of the
daemon? Point reads (`tx.row`) work today; full `tx.query` support there is on the
way.)

Two consequences worth internalizing:

- **Ownership checks can live in the one body.** `deleteIssue` in the
  [quickstart](https://rindle.sh/docs/synced-app-quickstart) reads the row and returns early for a
  non-owner — a no-op locally *and* in the authoritative run, where `ctx.user` is
  the verified principal.
- **A reading mutator can't be folded** — see
  [high-frequency writes](#high-frequency-writes-must-be-absorbing).

## The acting principal: `ctx.user`

Every shared body receives `ctx: MutatorCtx` — `{ user }`, the authenticated
identity of whoever is writing — as its third argument.

- The **client** injects its local user: the `user: () => currentUser()` option of
  [`createRindleClient`](https://rindle.sh/docs/client) (re-read per invoke, stable across a rebase
  re-invoke).
- The **server** injects its **authenticated** principal — see `sharedCtx` in
  [the API server](https://rindle.sh/docs/api-server#driving-the-shared-mutators).

`ctx.user` is **off-wire**: the wire carries only `{ name, args }`, so the actor
can't be spoofed. Never model the acting identity as an `owner`/`author` *arg* —
args are data, `ctx.user` is the actor. (A real `owner` field on a row is fine as
an arg when it isn't the actor.)

## The determinism rules

A mutator body re-runs on every rebase, and the server replays it from
`(name, args)` alone. So the body must be a pure function of
`(args, ctx, reads)`:

1. **No `Date.now()`, no `Math.random()`, no I/O.** Generate ids and timestamps at
   the **callsite** and pass them in as args.
2. **No reading component or module state** — everything the body needs arrives as
   args, `ctx`, or a `yield`ed read.
3. **No local-only tables.** A mutator replays from `(name, args)` on the server,
   so it can't depend on private browser rows — use
   [`store.writeLocal`](https://rindle.sh/docs/client#local-only-tables-drafts-selections-prefs)
   for those.
4. **Normalize inside the body** (trim, clamp, default) so both tiers normalize
   identically — a helper like `cleanTitle` above keeps prediction == commit.

`throw` inside a body to hard-reject a write; on the server that rolls the
transaction back and the client's optimistic prediction snaps back on its own.
The full rejection model (hard reject vs. accepted-but-no-op) lives in
[the API server](https://rindle.sh/docs/api-server#driving-the-shared-mutators).

## High-frequency writes must be absorbing

`app.mutate.<name>.folded(opts, args)` collapses a run of same-key calls into one
pending entry — the local view updates on every call, the server sees only the
last (see [the browser client](https://rindle.sh/docs/client#folded-writes-high-frequency-drags)
for the mechanics). The constraint lives with the mutator: a folded mutator must
be **absorbing** — replaying only the last args must equal replaying all of them
(`setScore(8)` after `setScore(5)` is just `8`). An `increment()`-style body is
not absorbing and must not be folded; the folded path refuses a mutator that
reads state (`yield tx.row` / `tx.query`) by throwing.

## Next steps

- [The browser client](https://rindle.sh/docs/client) — how predictions apply, rebase, and snap
  back; folded-write mechanics.
- [The API server](https://rindle.sh/docs/api-server#driving-the-shared-mutators) —
  `sharedApiMutators`, server-only authority (policy guards, the raw-SQL escape
  hatch), and the two rejection shapes.
- [Synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) — the full
  `shared/app-def.ts` contract in context.
- [Schema & migrations](https://rindle.sh/docs/schema) — the generated schema these ops typecheck
  against.
- [Troubleshooting](https://rindle.sh/docs/troubleshooting) — the ways a mutator goes subtly wrong.
