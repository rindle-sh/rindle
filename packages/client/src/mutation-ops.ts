// Isomorphic mutation ops — the backend-agnostic "rindle mutation" vocabulary
// (MUTATORS-ISOMORPHIC plan). A mutator names WHAT it writes (insert/update/upsert/
// insertIgnore/delete over a keyed row), not HOW — the server renders dialect SQL and the
// client applies to its local engine. This lives in `@rindle/client` (the leaf both tiers
// import); the SQL renderer that consumes it is server-only (`@rindle/api-server`).

import type { Ast } from "./ast.ts";
import type { ColsMap, InsertOf, PkColsOf, PkMap, PkOf, Schema, UpdateOf } from "./schema.ts";
import type { WireValue } from "./types.ts";

/** A keyed row: column name → cell. The ergonomic write shape (validated against the schema at
 *  runtime). JSON columns carry their raw JSON string (a {@link WireValue}), never a parsed object. */
export type KeyedRow = Record<string, WireValue>;

/** One structured write intent — the discriminated union that mirrors the write half of the client
 *  `MutationTx` 1:1. Keyed (column-name addressed), so it is independent of column order. */
export type MutationOp =
  | { kind: "insert"; table: string; row: KeyedRow } // a FULL row (every column present)
  | { kind: "upsert"; table: string; row: KeyedRow } // full row; replace non-pk cols on pk conflict
  | { kind: "insertIgnore"; table: string; row: KeyedRow } // full row; do nothing on pk conflict
  | { kind: "update"; table: string; row: KeyedRow } // pk cols + the non-pk cols to change
  | { kind: "delete"; table: string; pk: KeyedRow }; // pk cols only

/** The ASYNC server-side write surface a mutator runs against. Semantically the twin of the client's
 *  synchronous `MutationTx` write methods — same names, same keyed-row arguments — but every op is a
 *  Promise so a Postgres backend can execute it live against an open transaction (read-your-writes).
 *  A rindle/daemon backend resolves writes immediately (accumulate) and serves `row` from committed
 *  state. `insert` requires every column; `update`/`delete` require the pk columns. */
export interface ServerWriteTx {
  /** Insert a FULL row (every column named; missing or unknown columns throw). */
  insert(table: string, row: KeyedRow): Promise<void>;
  /** Update the row identified by the pk columns; only the named non-pk columns change. A missing
   *  row is a NO-OP. */
  update(table: string, row: KeyedRow): Promise<void>;
  /** Insert, or fully replace when the pk already exists (a FULL row, like `insert`). */
  upsert(table: string, row: KeyedRow): Promise<void>;
  /** Insert a FULL row, or do nothing if the pk already exists (the SQL twin of the client
   *  `if (!tx.row(pk)) tx.insert(row)` pattern). */
  insertIgnore(table: string, row: KeyedRow): Promise<void>;
  /** Delete the row identified by the pk columns. A missing row is a NO-OP. */
  delete(table: string, pk: KeyedRow): Promise<void>;
  /** Read one row by primary key, through the OPEN transaction on both backends
   *  (read-your-writes: live on Postgres; via an interactive mutation session on the daemon,
   *  DAEMON-INTERACTIVE-TXN-DESIGN.md §5.3). */
  row(table: string, pk: KeyedRow): Promise<KeyedRow | undefined>;
}

// --------------------------------------------------------------------------- shared (generator) mutators
//
// The isomorphic mutator seam (MUTATORS-ISOMORPHIC plan §"one body, two tiers"): a mutator is a
// GENERATOR that `yield`s effects instead of an async/sync function. A generator is neither sync nor
// async — the tier's DRIVER decides — so ONE body runs synchronously against the browser wasm engine
// AND against a live async Postgres transaction on the server. Writes are `yield`ed and pipelined by
// the driver (never individually awaited by the body); a read is the thing that suspends. Three read
// shapes: `yield tx.row(...)` (a point pk read, evaluating to the row), `yield tx.query(builder)` (a
// full `where`/`orderBy`/`limit`/join query, evaluating to its rows), and `yield tx.all([...])` (a
// fan-out — Promise.all on the server, in-order on the client). All are order-preserving on both
// tiers, so the body stays deterministic. Every read is read-your-writes (sees this mutator's own
// writes-so-far). `tx.query` runs the SAME query engine live queries use (the wasm IVM on the client;
// `@rindle/query-compiler`'s SQLite SELECT through the open session on the daemon backend).

/** A point read a generator mutator yields; the driver resolves it and feeds the row back through
 *  `gen.next(row)`. Read-your-writes on every tier: the client reads its local engine, and both
 *  server backends read the open transaction (the daemon via an interactive mutation session). */
export type ReadEffect = { kind: "row"; table: string; pk: KeyedRow };

/** A fan-out a generator mutator yields: run several effects "together". The server driver resolves
 *  them with `Promise.all`; the client driver runs them in array order (already synchronous). Results
 *  return in the SAME order on both tiers, so the mutator stays deterministic. The `yield` evaluates
 *  to `(KeyedRow | undefined)[]` at runtime (cast it — the generator's single next-type is a row). */
export type BatchEffect = { kind: "all"; effects: readonly YieldEffect[] };

/** A row returned by a {@link QueryEffect}: column name → cell, plus each materialized relationship
 *  name → its nested row(s) — an array (a plural relationship) or a single row / `null` (a `.one()`
 *  relationship). Recursive. Presented identically on both tiers (a `view.data` row of the same
 *  query). */
export type QueryResultRow = { [key: string]: WireValue | QueryResultRow | QueryResultRow[] };

/** What {@link IsoTx.query} accepts: a query handle whose `.ast()` lowers to the wire {@link Ast} —
 *  exactly what the typed query builder produces (`newQueryBuilder(schema).<table>…`, the tier-agnostic
 *  server-scope builder both tiers can construct because it performs no I/O). Structural so the seam
 *  need not carry the builder's heavy generics. */
export type QueryArg = { ast(): Ast };

/** A full-shape read a generator mutator yields — a `where`/`orderBy`/`limit`/join query over the
 *  state this mutator is mutating (read-your-writes, like {@link ReadEffect} but an arbitrary shape).
 *  Evaluates to {@link QueryResultRow}`[]` — ALWAYS an array of the matching rows, in the query's
 *  order, on BOTH tiers (a root `.one()` is not unwrapped here: take `[0]`). The single next-type is
 *  a row, so cast the yield (`(yield tx.query(q)) as unknown as QueryResultRow[]`), same as `all`. */
export type QueryEffect = { kind: "query"; query: QueryArg };

/** Everything a generator mutator may `yield`: a write {@link MutationOp}, a point {@link ReadEffect},
 *  a full-query {@link QueryEffect}, or a {@link BatchEffect} fan-out. */
export type YieldEffect = MutationOp | ReadEffect | BatchEffect | QueryEffect;

/** The tier-AGNOSTIC effect factory a generator mutator writes against. Every method just BUILDS an
 *  effect to `yield` — it performs no I/O and holds no state, so the single {@link isoTx} instance is
 *  shared by every mutator on both tiers; only the driver differs. `insert`/`upsert`/`insertIgnore`
 *  take a FULL row; `update`/`delete` take the pk columns (`update` adds the columns to change). */
export interface IsoTx<S extends ColsMap = ColsMap, P extends Record<string, string> = PkMap<S>> {
  /** Insert a FULL row — nullable columns may be omitted (filled `null`, design 206 §6.2/§7). */
  insert<N extends keyof S & string>(table: N, row: InsertOf<S[N]>): MutationOp;
  /** Update the row identified by its pk columns (REQUIRED); only the named non-pk columns change. */
  update<N extends keyof S & string>(table: N, row: UpdateOf<S[N], PkColsOf<S, P, N>>): MutationOp;
  /** Insert, or fully replace on pk conflict (a FULL row, like {@link IsoTx.insert}). */
  upsert<N extends keyof S & string>(table: N, row: InsertOf<S[N]>): MutationOp;
  /** Insert a FULL row, or do nothing if the pk already exists. */
  insertIgnore<N extends keyof S & string>(table: N, row: InsertOf<S[N]>): MutationOp;
  /** Delete the row identified by its pk columns. */
  delete<N extends keyof S & string>(table: N, pk: PkOf<S[N], PkColsOf<S, P, N>>): MutationOp;
  /** Read one row by primary key (read-your-writes). */
  row<N extends keyof S & string>(table: N, pk: PkOf<S[N], PkColsOf<S, P, N>>): ReadEffect;
  /** Run a full query (`where`/`orderBy`/`limit`/join) over the state this mutator is mutating —
   *  read-your-writes, like {@link row} but for arbitrary shapes. Pass a query from the tier-agnostic
   *  builder, e.g. `tx.query(q.issue.where("ownerId", "=", ctx.user))` where `q = newQueryBuilder(schema)`.
   *  The `yield` evaluates to {@link QueryResultRow}`[]` (cast it — the generator's single next-type is a row). */
  query(query: QueryArg): QueryEffect;
  all(effects: readonly YieldEffect[]): BatchEffect;
}

/** The one shared effect factory (stateless — see {@link IsoTx}). Its methods just BUILD a
 *  {@link MutationOp}, so the single instance serves every schema; the generic {@link IsoTx} view is
 *  applied at the authoring site (a `json<T>` cell is a parsed object here and is stringified by the
 *  funnels, {@link toCell}), hence the cast — the runtime shape is schema-agnostic. */
export const isoTx: IsoTx = {
  insert: (table: string, row: KeyedRow): MutationOp => ({ kind: "insert", table, row }),
  update: (table: string, row: KeyedRow): MutationOp => ({ kind: "update", table, row }),
  upsert: (table: string, row: KeyedRow): MutationOp => ({ kind: "upsert", table, row }),
  insertIgnore: (table: string, row: KeyedRow): MutationOp => ({ kind: "insertIgnore", table, row }),
  delete: (table: string, pk: KeyedRow): MutationOp => ({ kind: "delete", table, pk }),
  row: (table: string, pk: KeyedRow): ReadEffect => ({ kind: "row", table, pk }),
  query: (query: QueryArg): QueryEffect => ({ kind: "query", query }),
  all: (effects: readonly YieldEffect[]): BatchEffect => ({ kind: "all", effects }),
} as unknown as IsoTx;

/** The minimal per-invocation context a shared mutator sees on BOTH tiers: the acting principal. The
 *  server injects its AUTHENTICATED user; the client injects its local user. (The server's own
 *  `MutationContext` is a superset of this.) */
export interface MutatorCtx {
  user: string;
}

/** What a generator mutator IS: `yield tx.<op>()` on every side effect; a `yield tx.row()` expression
 *  evaluates to the row. Neither sync nor async — the tier's driver decides, which is what lets one
 *  body run synchronously on the client and against a live async transaction on the server. */
export type MutationGen = Generator<YieldEffect, void, KeyedRow | undefined>;

/** A generator (isomorphic) mutator, shared verbatim by both tiers: the client trusts typed `args`,
 *  the server parses untrusted args into `Args` before invoking. */
export type SharedMutator<Args, Ctx extends MutatorCtx = MutatorCtx> = (
  tx: IsoTx,
  args: Args,
  ctx: Ctx,
) => MutationGen;

/** The minimal arg validator a shared mutator can carry so the SERVER can parse UNTRUSTED wire args
 *  before driving it (the client trusts its typed callsites and never calls this). Structural on
 *  purpose — a zod schema satisfies it as-is — so `@rindle/client` stays validator-library-agnostic. */
export interface ArgSchema<Args> {
  parse(raw: unknown): Args;
}

/** A shared mutator that CARRIES its own arg validator, co-located at the def site (`shared(schema,
 *  gen)`). The client registers it exactly like a bare generator mutator — the `.args` validator is
 *  inert there (typed callsites skip the parse); the server ({@link runSharedMutation}, via the
 *  api-server's `sharedApiMutators`) reads `.args` to parse untrusted wire args before driving the
 *  SAME body. */
export type SharedMutatorWithArgs<Args, Ctx extends MutatorCtx = MutatorCtx> = SharedMutator<Args, Ctx> & {
  args: ArgSchema<Args>;
};

/** Co-locate a shared (generator) mutator with the validator for its args — pairing the arg SHAPE
 *  with the body that consumes it at ONE site, so neither tier restates it (the client derives its
 *  callsite type from `Args`; the server parses untrusted args through `.args`). Returns the SAME
 *  generator function with an `args` property attached (`Object.assign` mutates + returns it), so the
 *  registered value is byte-for-byte what the client drove before: {@link isGeneratorMutator} still
 *  detects it and it still `satisfies ClientRegistry`. */
export function shared<Args, Ctx extends MutatorCtx = MutatorCtx>(
  args: ArgSchema<Args>,
  run: SharedMutator<Args, Ctx>,
): SharedMutatorWithArgs<Args, Ctx> {
  return Object.assign(run, { args });
}

/** Bind a schema to the mutator authoring surface: `const { shared } = defineMutators(schema)`.
 *
 *  The returned `shared` is the schema-typed twin of the bare {@link shared} — its `tx` is an
 *  {@link IsoTx} parameterized by THIS schema, so `tx.insert`/`update`/`upsert`/`insertIgnore`/`delete`
 *  check the table name, every column name, each column's value type, nullable-omit on inserts, and
 *  the exact primary-key columns on `update`/`delete` — all at compile time. It registers identically
 *  to the bare form (same runtime value, still `satisfies ClientRegistry`, still an isomorphic
 *  generator the server drives): only the AUTHORING types tighten. `schema` is read for its TYPE only
 *  (never at runtime). Reads (`yield tx.row(...)`) stay loosely typed — a generator's single
 *  next-type can't carry a per-table row. */
/** The typed {@link IsoTx} for a given schema — `IsoTxOf<typeof schema>`. Use it to annotate a helper
 *  that a mutator body passes its `tx` to (e.g. `const ensureUser = (tx: IsoTxOf<typeof schema>, …)`),
 *  so the helper gets the same table/column/pk typing the `defineMutators` `shared` callback does. */
export type IsoTxOf<Sch extends Schema> = Sch extends Schema<infer S, infer P> ? IsoTx<S, P> : never;

export function defineMutators<S extends ColsMap, P extends Record<string, string>>(_schema: Schema<S, P>) {
  return {
    shared<Args, Ctx extends MutatorCtx = MutatorCtx>(
      args: ArgSchema<Args>,
      run: (tx: IsoTx<S, P>, args: Args, ctx: Ctx) => MutationGen,
    ): SharedMutatorWithArgs<Args, Ctx> {
      // The typed `tx: IsoTx<S, P>` is authoring-only; the driver invokes with the loose `isoTx`
      // singleton (cast at the call boundary, like the bare `shared`), so the value is registry-shaped.
      return Object.assign(run as unknown as SharedMutator<Args, Ctx>, { args });
    },
  };
}

/** True iff `fn` is a generator function (a shared/isomorphic mutator) rather than a plain function —
 *  the driver accepts both forms. Detected structurally (native `GeneratorFunction`). */
export function isGeneratorMutator(fn: unknown): fn is (...args: never[]) => MutationGen {
  return (
    typeof fn === "function" &&
    (fn as { constructor?: { name?: string } }).constructor?.name === "GeneratorFunction"
  );
}

/** A tier's SYNCHRONOUS effect executor (the browser wasm engine): apply a write, resolve a read —
 *  both immediate. */
export interface SyncEffectExec {
  apply(op: MutationOp): void;
  read(table: string, pk: KeyedRow): KeyedRow | undefined;
  query(q: QueryArg): QueryResultRow[];
}

/** Drive a generator mutator SYNCHRONOUSLY (the client, inside the wasm write transaction). Each
 *  yielded write applies immediately; each read (point or query) is resolved and fed back; a batch
 *  runs in order. */
export function driveMutationSync(gen: MutationGen, exec: SyncEffectExec): void {
  const run = (eff: YieldEffect): KeyedRow | undefined => {
    if (eff.kind === "row") return exec.read(eff.table, eff.pk);
    if (eff.kind === "query") return exec.query(eff.query) as unknown as KeyedRow | undefined;
    if (eff.kind === "all") return eff.effects.map(run) as unknown as KeyedRow | undefined;
    exec.apply(eff);
    return undefined;
  };
  for (let step = gen.next(); !step.done; step = gen.next(run(step.value)));
}

/** A tier's ASYNCHRONOUS effect executor (the server transaction): every op is a Promise. */
export interface AsyncEffectExec {
  apply(op: MutationOp): Promise<void>;
  read(table: string, pk: KeyedRow): Promise<KeyedRow | undefined>;
  query(q: QueryArg): Promise<QueryResultRow[]>;
}

/** Drive a generator mutator ASYNCHRONOUSLY (the server, against the open transaction). Writes are
 *  awaited (harmless: a single interactive Postgres connection serializes statements anyway, and the
 *  daemon backend resolves them instantly), reads (point or query) suspend, and a batch fans out with
 *  `Promise.all`. */
export async function driveMutationAsync(gen: MutationGen, exec: AsyncEffectExec): Promise<void> {
  const run = async (eff: YieldEffect): Promise<KeyedRow | undefined> => {
    if (eff.kind === "row") return exec.read(eff.table, eff.pk);
    if (eff.kind === "query") return (await exec.query(eff.query)) as unknown as KeyedRow | undefined;
    if (eff.kind === "all") return (await Promise.all(eff.effects.map(run))) as unknown as KeyedRow | undefined;
    await exec.apply(eff);
    return undefined;
  };
  for (let step = gen.next(); !step.done; step = gen.next(await run(step.value)));
}
