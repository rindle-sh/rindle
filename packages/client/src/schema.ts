// The typed schema: `table("issue").columns({...}).primaryKey("id")` + `createSchema`.
//
// A table definition doubles as a field→condition factory (a runtime Proxy):
// `issue.priority(gt(8))` produces a `Cond<RowOf<issue>>`. THAT is where the table type is
// bound — which is why `or`/`exists` are plain top-level functions, no closure needed
// (WASM-CLIENT-DESIGN.md §6). Schema metadata is stored under a `unique symbol` key so it
// never collides with a column name.

import type { Arg, Cond } from "./operators.ts";
import { fieldCondition } from "./operators.ts";
import type { ColType, WireValue } from "./types.ts";

/** A column descriptor. `type` drives the comparator + JSON parsing; `__t` is a phantom. A
 *  nullable column is a `Col<T | null>` — that is the ONLY thing nullability changes at the type
 *  level, so `RowOf` (and the field-condition factory) widen automatically.
 *
 *  `optional` is the runtime companion of that phantom: set by {@link ColBuilder.nullable}, it is
 *  what the write funnels read to make a nullable column omittable from an insert (filled with
 *  `null`, design 206 §6.2). It mirrors the engine's `ColumnDef.optional`
 *  (`pragma_table_info.notnull == 0`); absent ⇒ `NOT NULL` / required. */
export interface Col<T> {
  readonly type: ColType;
  readonly optional?: boolean;
  readonly __t?: T;
}
export type ColT<X> = X extends Col<infer T> ? T : never;
export type AnyCols = Record<string, Col<unknown>>;
export type RowOf<C extends AnyCols> = { [K in keyof C]: ColT<C[K]> };

/** The chainable form returned by the column factories: a {@link Col} plus `.nullable()`.
 *
 *  `.nullable()` widens the column's value type to `T | null` — its `Row<…>` field becomes
 *  `T | null` — and sets the runtime {@link Col.optional} marker so it may be omitted from an insert
 *  (design 206 §6.2). `rindle schema gen` emits `.nullable()` for every nullable (non-`NOT NULL`) SQL
 *  column; you can also call it by hand on a local-only table's columns. It is idempotent and stays
 *  chainable. */
export interface ColBuilder<T> extends Col<T> {
  nullable(): ColBuilder<T | null>;
}
const makeCol = <T>(type: ColType, optional = false): ColBuilder<T> => ({
  type,
  ...(optional ? { optional: true } : null),
  nullable: () => makeCol<T | null>(type, true),
});

export const string = <T extends string = string>(): ColBuilder<T> => makeCol<T>("string");
export const number = <T extends number = number>(): ColBuilder<T> => makeCol<T>("number");
export const boolean = (): ColBuilder<boolean> => makeCol<boolean>("boolean");
export const json = <T = unknown>(): ColBuilder<T> => makeCol<T>("json");
/** The exact-i64 column plane (design 226, `BIGINT`/`INT8` decltype): typed `bigint` in
 *  application code. The vocabulary exists from Stage C4 so generated schemas can name it;
 *  no exact integer cell enters the browser IVM until Stage E — until then the daemon
 *  refuses IVM queries whose footprint touches the column, and the SQL plane carries it. */
export const int64 = <T extends bigint = bigint>(): ColBuilder<T> => makeCol<T>("int64");

/** Metadata key on a {@link TableDef} (a `unique symbol`, so no column name collides). */
export const SCHEMA: unique symbol = Symbol("rindle.schema");

export interface TableMeta<N extends string = string, C extends AnyCols = AnyCols, PK extends string = string> {
  readonly name: N;
  readonly columns: C;
  // `PK` is an INDEPENDENT param (a plain string union), NOT `keyof C`: `keyof C` would be
  // contravariant and make `TableMeta` non-covariant in `C`, breaking `TableDef<concrete>` →
  // `AnyTable`. Key validity is enforced on the `primaryKey()` builder (`K extends keyof C`); PK is
  // captured there so the mutator tx can type `update`/`delete` pk args (`PkOf`/`UpdateOf`). The
  // default `string` erases it (older 2-arg `TableMeta<N, C>` refs still resolve).
  readonly primaryKey: readonly PK[];
  /** A **local-only** table (`201-LOCAL-ONLY-TABLES-DESIGN.md`): client-authoritative,
   *  never synced/tracked/rebased. The single marker the whole design keys off (§4) — it is
   *  immutable for the table's lifetime (N2) and never crosses the wire (C2). Absent ⇒ an
   *  ordinary synced table.
   *
   *  `true` ⇒ eligible for the local-persistence plane (durable + cross-tab when the client
   *  enables `persistLocal`, `207-LOCAL-TABLE-PERSISTENCE-DESIGN.md`). `"session"` ⇒ local but
   *  EPHEMERAL: outside the plane entirely — never persisted, never replicated across tabs,
   *  per-client-instance state that empties on reload (201's original behavior) even when
   *  `persistLocal` is on. Every OTHER locality rule (untracked source, M1/M2 guards, E3/Q1
   *  wire exclusion) treats both variants identically. */
  readonly local?: boolean | "session";
}

/** Options for {@link table}. */
export interface TableOptions {
  /** Declare a {@link TableMeta.local local-only} table (selection state, draft text, view
   *  prefs, scratch rows): client-authoritative, never synced or rebased. See
   *  `201-LOCAL-ONLY-TABLES-DESIGN.md`. Pass `"session"` for a local table that must stay
   *  EPHEMERAL and per-tab even when the client enables `persistLocal` — e.g. selection state
   *  that should not follow the user across tabs or reloads (207 §5.4). */
  local?: boolean | "session";
}

export type TableDef<N extends string, C extends AnyCols, PK extends string = string> = {
  readonly [SCHEMA]: TableMeta<N, C, PK>;
} & {
  readonly [K in keyof C]: (arg: Arg<ColT<C[K]>>) => Cond<RowOf<C>>;
};

/** Any table, for positions that only read its metadata. The field-factory part of a
 *  `TableDef` is invariant in `C`, so callers constrain on the `[SCHEMA]` meta only — to
 *  which every concrete `TableDef<N, C>` is assignable. */
export type TableLike<C extends AnyCols> = { readonly [SCHEMA]: TableMeta<string, C> };
export type AnyTable = TableLike<AnyCols>;

/** The row type of a table definition: `Row<typeof issue>` → `{ id: string; … }`. The
 *  whole-table ergonomic form of {@link RowOf}, so app code derives its row interfaces from
 *  the schema instead of hand-maintaining a parallel twin. */
export type Row<T extends AnyTable> = RowOf<T[typeof SCHEMA]["columns"]>;

/** Flatten an intersection of mapped types into a single object type (preserving `?`/`readonly`) so
 *  {@link InsertOf} reads as one clean shape in editor hovers, not `A & B`. */
type Simplify<T> = { [K in keyof T]: T[K] };

/** True for the top types `unknown`/`any` (`unknown extends any` too), where `[null] extends [T]` is
 *  vacuously true and so can't tell a `.nullable()` apart. */
type IsTop<T> = unknown extends T ? true : false;

/** Whether column `X` may be OMITTED from an insert: it admits `null` at the type level — EXCLUDING
 *  the top types. A bare `json()` is `json<unknown>()`, and `unknown | null` collapses back to
 *  `unknown`, erasing whether `.nullable()` was applied; treating it as required is the safe
 *  direction (a `NOT NULL` json column is never wrongly made optional). Declare `json<T>()` to make a
 *  nullable json column omittable. The runtime companion is `Col.optional` (design 206 §6.2/§7). */
type InsertOptional<X> = IsTop<ColT<X>> extends true ? false : [null] extends [ColT<X>] ? true : false;

/** The INSERT shape of a column map: {@link RowOf} with every NULLABLE column made OPTIONAL (`?`) —
 *  it may be omitted and is filled with `null` by both write funnels (design 206 §6.2/§7). `NOT NULL`
 *  columns stay required. The insert-side twin of {@link RowOf}, mirroring Drizzle's `$inferInsert`
 *  vs `$inferSelect` split. */
export type InsertOf<C extends AnyCols> = Simplify<
  { [K in keyof C as InsertOptional<C[K]> extends true ? never : K]: ColT<C[K]> } & {
    [K in keyof C as InsertOptional<C[K]> extends true ? K : never]?: ColT<C[K]>;
  }
>;

/** The insert type of a table def: `Insert<typeof issue>` → `{ id: string; … assignee?: string | null }`.
 *  The whole-table ergonomic form of {@link InsertOf} (the insert-side twin of {@link Row}). */
export type Insert<T extends AnyTable> = InsertOf<T[typeof SCHEMA]["columns"]>;

/** The exact PRIMARY-KEY columns of a table (each typed), required — the identity a `delete`/`row`,
 *  and the WHERE half of an `update`, take. `PK` is the table's pk-column union (the schema's `__pk`). */
export type PkOf<C extends AnyCols, PK extends string> = { [K in PK & keyof C]: ColT<C[K & keyof C]> };

/** The UPDATE shape of a table: its {@link PkOf primary-key columns} REQUIRED (they identify the row)
 *  plus every non-pk column OPTIONAL (`?`) and typed — a nullable one stays `T | null` (settable to
 *  null), a `NOT NULL` one is `T`. Omitted non-pk columns are left unchanged. */
export type UpdateOf<C extends AnyCols, PK extends string> = Simplify<
  PkOf<C, PK> & { [K in Exclude<keyof C, PK>]?: ColT<C[K]> }
>;

/** The primary-key column union for table `N` of a schema, read from its `__pk` map `P` and narrowed
 *  to `N`'s actual columns (falling back to all columns when `P` doesn't name `N` — e.g. the loose
 *  default schema). Feeds {@link PkOf}/{@link UpdateOf} in the typed mutator tx. */
export type PkColsOf<S extends ColsMap, P extends Record<string, string>, N extends keyof S> = (N extends keyof P
  ? P[N]
  : keyof S[N] & string) &
  keyof S[N] &
  string;

/** `table("issue").columns({ id: string(), … }).primaryKey("id")`. Pass `{ local: true }` for a
 *  {@link TableMeta.local local-only} table (`201-LOCAL-ONLY-TABLES-DESIGN.md`). */
export function table<N extends string>(name: N, opts?: TableOptions) {
  return {
    columns<C extends AnyCols>(cols: C) {
      return {
        primaryKey<K extends keyof C & string>(...keys: K[]): TableDef<N, C, K> {
          return makeTableDef<N, C, K>({ name, columns: cols, primaryKey: keys, local: opts?.local });
        },
      };
    },
  };
}

function makeTableDef<N extends string, C extends AnyCols, PK extends string>(meta: TableMeta<N, C, PK>): TableDef<N, C, PK> {
  return new Proxy({} as Record<string | symbol, unknown>, {
    get(_target, prop) {
      if (prop === SCHEMA) return meta;
      if (typeof prop === "string") return (arg: unknown) => fieldCondition(prop, arg);
      return undefined;
    },
  }) as unknown as TableDef<N, C, PK>;
}

/** Read a table's metadata (columns / PK / name). */
export function tableMeta(t: AnyTable): TableMeta {
  return t[SCHEMA];
}

/** name → columns, derived from the tables array (for typing `store.query.<table>`). */
export type SchemaOf<T extends readonly AnyTable[]> = {
  [E in T[number] as E[typeof SCHEMA]["name"]]: E[typeof SCHEMA]["columns"];
};

/** name → its primary-key column union, derived from the tables array (the PK captured by the
 *  `primaryKey(...)` builder). Powers the mutator tx's typed `update`/`delete`/`row` pk args. */
export type PkMapOf<T extends readonly AnyTable[]> = {
  [E in T[number] as E[typeof SCHEMA]["name"]]: E[typeof SCHEMA]["primaryKey"][number];
};

/** name → columns, the resolved schema map carried in the {@link Schema} type. */
export type ColsMap = Record<string, AnyCols>;

/** name → (some subset of its column names): the loose shape a {@link Schema}'s pk-map satisfies. The
 *  fallback when a schema wasn't built through {@link createSchema} — every column could be the pk. */
export type PkMap<S extends ColsMap> = { [N in keyof S]: keyof S[N] & string };

export interface Schema<S extends ColsMap = ColsMap, P extends Record<string, string> = PkMap<S>> {
  readonly tables: Readonly<Record<string, TableMeta>>;
  /** Phantom carrying name→columns for query-root inference (never read at runtime). */
  readonly __cols: S;
  /** Phantom carrying name→pk-column-union for the typed mutator tx (never read at runtime). */
  readonly __pk: P;
}

/** Table-name prefixes reserved by the engine for SYNTHETIC tables — `__agg_<fnv>` aggregate
 *  bases (`AGGREGATE-SYNC-DESIGN.md` §3.3) and `_rindle_*` system tables (e.g. the lmid table).
 *  A user table (synced OR local) under one of these would shadow a synthetic source — and since
 *  `registerTable` is idempotent-on-name and base tables register before synthetics, the later
 *  synthetic registration would silently no-op and the agg join would read the user table as the
 *  count (silent corruption, `201-LOCAL-ONLY-TABLES-DESIGN.md` N1). The ban makes that
 *  structurally impossible — checked once, statically, at {@link createSchema}. */
export const RESERVED_TABLE_PREFIXES = ["__agg_", "_rindle_"] as const;

/** Whether `name` collides with an engine-reserved synthetic prefix ({@link RESERVED_TABLE_PREFIXES}). */
export function isReservedTableName(name: string): boolean {
  return RESERVED_TABLE_PREFIXES.some((p) => name.startsWith(p));
}

export function createSchema<const T extends readonly AnyTable[]>(opts: {
  tables: T;
}): Schema<SchemaOf<T>, PkMapOf<T>> {
  const tables: Record<string, TableMeta> = {};
  for (const t of opts.tables) {
    const m = t[SCHEMA];
    addTableMeta(tables, m, "createSchema");
  }
  return { tables } as unknown as Schema<SchemaOf<T>, PkMapOf<T>>;
}

/** Extend a generated/synced schema with client-authoritative local-only tables.
 *
 *  This is the ergonomic path for SQL-first apps: keep `schema.gen.ts` fully generated, define
 *  private UI tables in a tiny hand-written file, then hand the combined schema to the browser
 *  client. The added tables MUST be `table(name, { local: true })`: `extendSchema` deliberately
 *  refuses to append ordinary synced tables, because those need to come from daemon introspection
 *  (`rindle schema gen`) so the server and client cannot drift. */
export function extendSchema<S extends ColsMap, P extends Record<string, string>, const T extends readonly AnyTable[]>(
  base: Schema<S, P>,
  opts: { tables: T },
): Schema<S & SchemaOf<T>, P & PkMapOf<T>> {
  const tables: Record<string, TableMeta> = { ...base.tables };
  for (const t of opts.tables) {
    const m = t[SCHEMA];
    if (!m.local) {
      throw new Error(
        `extendSchema: table "${m.name}" is not local-only — synced tables must be generated from the daemon schema.`,
      );
    }
    addTableMeta(tables, m, "extendSchema");
  }
  return { tables } as unknown as Schema<S & SchemaOf<T>, P & PkMapOf<T>>;
}

function addTableMeta(tables: Record<string, TableMeta>, m: TableMeta, caller: "createSchema" | "extendSchema") {
  // N1: ban reserved synthetic prefixes for ANY user table (synced or local). One source per
  // name, statically disjoint from the dynamic `__agg_*`/`_rindle_*` namespace.
  if (isReservedTableName(m.name)) {
    throw new Error(
      `${caller}: table "${m.name}" uses a reserved prefix (${RESERVED_TABLE_PREFIXES.join(", ")}) — ` +
        `these namespaces are owned by the engine's synthetic tables (201-LOCAL-ONLY-TABLES-DESIGN.md N1).`,
    );
  }
  // Same N1 rule for the room-table namespace (302 §2): a room's engine twin is `table@sourceKey`,
  // so a user table containing `@` could alias a twin and let a room channel's deltas fold into
  // user-authoritative rows. `roomEngineTable`'s no-collision claim is enforced HERE.
  if (m.name.includes("@")) {
    throw new Error(
      `${caller}: table "${m.name}" contains "@" — reserved for the room-table namespace ` +
        `(302-ROOM-STORE-SEPARATION: a room's engine twin is "table@sourceKey").`,
    );
  }
  // N1/N2: one name → one source. A duplicate (incl. a local/synced name clash, since both
  // register from this one array) is a genuine collision, not a silent overwrite.
  if (Object.prototype.hasOwnProperty.call(tables, m.name)) {
    throw new Error(`${caller}: duplicate table "${m.name}" — table names must be unique.`);
  }
  tables[m.name] = m;
}

// ----------------------------- refinement (narrowing generated column types) ---------------------
//
// SQL carries a column's KIND (string/number/boolean/json) but not a refinement WITHIN a kind — the
// element type of `json<T>()`, or a string/number literal union. `refineTable` re-types those
// columns on a generated `TableDef` (so its field conditions, `rel(...)`s, and `Row<typeof t>` all
// narrow), and `refineSchema` swaps the refined defs into the generated schema (so query roots and
// result rows narrow too). Both are runtime-validated IDENTITIES: a refinement narrows the phantom
// TS type but can never change a column's runtime kind, so the def still matches the daemon's wire
// schema exactly. Keep refinements in a hand-written module (beside `extendSchema`'s local tables);
// `schema.gen.ts` stays a pure, wholesale-regenerated artifact.

/** Per-column narrowings for {@link refineTable}: each entry must keep the column's kind and narrow
 *  its TS type (`Col<T2>` with `T2 extends T`) — `json<Meta>()` on a json column, a literal-union
 *  `string<"a" | "b">()` on a string column. Kind changes are rejected (cross-kind at compile time,
 *  same-phantom kind flips like `string()` on a json column at runtime). */
export type ColRefinements<C extends AnyCols> = {
  readonly [K in keyof C]?: Col<ColT<C[K]>>;
};

/** `C` with the columns named in `R` re-typed to their refined `Col`s. */
export type RefinedCols<C extends AnyCols, R extends ColRefinements<C>> = {
  [K in keyof C]: K extends keyof R ? (R[K] extends Col<unknown> ? R[K] : C[K]) : C[K];
};

/** Narrow a generated table's column TYPES without touching its runtime shape.
 *
 *  Returns the SAME def, re-typed (identity, after validating that every refined column exists and
 *  keeps its kind) — so conditions built from it, `rel(...)`s anchored on it, and `Row<typeof t>`
 *  all see the narrowed types. Pass the result to {@link refineSchema} so query roots narrow too. */
export function refineTable<N extends string, C extends AnyCols, R extends ColRefinements<C>>(
  base: TableDef<N, C>,
  cols: R,
): TableDef<N, RefinedCols<C, R>> {
  const meta = base[SCHEMA];
  for (const [name, col] of Object.entries(cols) as [string, Col<unknown> | undefined][]) {
    if (col === undefined) continue;
    const baseCol = meta.columns[name];
    if (baseCol === undefined) {
      throw new Error(`refineTable: table "${meta.name}" has no column "${name}".`);
    }
    if (col.type !== baseCol.type) {
      throw new Error(
        `refineTable: column "${meta.name}.${name}" is ${baseCol.type}, not ${col.type} — a refinement ` +
          `may only narrow the TS type WITHIN a column's kind, never change the kind itself.`,
      );
    }
  }
  return base as unknown as TableDef<N, RefinedCols<C, R>>;
}

/** A table def acceptable to {@link refineSchema} over `Schema<S>`: its NAME must be one of `S`'s
 *  tables (a def for an unknown table is a compile error at the call site). */
export type RefinableTable<S extends ColsMap> = {
  readonly [SCHEMA]: TableMeta<Extract<keyof S, string>, AnyCols>;
};

/** `S` with each table named in `T` re-typed to that def's (refined) columns. (The inner
 *  `extends AnyCols` guard is how the checker proves the remapped `SchemaOf<T>[K]` is a column
 *  map while `T` is still generic; it always holds for a concrete `T`.) */
export type RefinedColsMap<S extends ColsMap, T extends readonly AnyTable[]> = {
  [K in keyof S]: K extends keyof SchemaOf<T> ? (SchemaOf<T>[K] extends AnyCols ? SchemaOf<T>[K] : S[K]) : S[K];
};

/** Swap {@link refineTable}-narrowed table defs into a generated schema, re-typing those tables for
 *  everything downstream of the schema (`newQueryBuilder`/`queries` roots, store row types).
 *
 *  Runtime-validated identity: each def must name a table already in the schema and match its
 *  runtime shape exactly (same columns, kinds, primary key, and locality) — refinement narrows TS
 *  types, never what's on the wire. Composes with {@link extendSchema} in either order. */
export function refineSchema<S extends ColsMap, P extends Record<string, string>, const T extends readonly RefinableTable<S>[]>(
  base: Schema<S, P>,
  opts: { tables: T },
): Schema<RefinedColsMap<S, T>, P> {
  const seen = new Set<string>();
  for (const t of opts.tables) {
    const m = t[SCHEMA];
    const baseMeta = base.tables[m.name];
    if (baseMeta === undefined) {
      throw new Error(
        `refineSchema: schema has no table "${m.name}" — refinement re-types existing tables only ` +
          `(add local-only tables with extendSchema).`,
      );
    }
    if (seen.has(m.name)) {
      throw new Error(`refineSchema: table "${m.name}" is refined twice.`);
    }
    seen.add(m.name);
    assertRefinementMatches(m, baseMeta);
  }
  return base as unknown as Schema<RefinedColsMap<S, T>, P>;
}

/** The refined def must be the same table the schema already carries — identical column set, kinds,
 *  primary key, and locality — so swapping its TYPE in cannot change any runtime behavior. */
function assertRefinementMatches(m: TableMeta, baseMeta: TableMeta): void {
  const cols = Object.keys(m.columns);
  const baseCols = Object.keys(baseMeta.columns);
  const matches =
    cols.length === baseCols.length &&
    cols.every((c) => baseMeta.columns[c] !== undefined && m.columns[c].type === baseMeta.columns[c].type) &&
    m.primaryKey.length === baseMeta.primaryKey.length &&
    m.primaryKey.every((k, i) => baseMeta.primaryKey[i] === k) &&
    // Locality must match EXACTLY, including the persisted-vs-session variant (a refinement
    // flipping `true` ↔ `"session"` would silently change the table's durability).
    (m.local ?? false) === (baseMeta.local ?? false);
  if (!matches) {
    throw new Error(
      `refineSchema: table "${m.name}" does not match the schema's table of that name — pass the ` +
        `output of refineTable over the SAME generated def (identical columns, kinds, primary key, ` +
        `and locality).`,
    );
  }
}

/** Whether `table` is a {@link TableMeta.local local-only} table in `schema` (an unknown table
 *  reads as non-local). The single locality predicate the backends key off. BOTH variants —
 *  `true` and `"session"` — are local here; the persisted/ephemeral split matters only to the
 *  persistence plane ({@link persistedLocalTableNames}). */
export function isLocalTable<S extends ColsMap>(schema: Schema<S>, table: string): boolean {
  return Boolean(schema.tables[table]?.local);
}

/** The set of local-only table names in `schema` (`201-LOCAL-ONLY-TABLES-DESIGN.md` §4) — BOTH
 *  variants (`true` and `"session"`); every locality rule except persistence keys off this set. */
export function localTableNames<S extends ColsMap>(schema: Schema<S>): Set<string> {
  return new Set(Object.keys(schema.tables).filter((n) => schema.tables[n].local));
}

/** The subset of {@link localTableNames} eligible for the persistence plane — `local: true` only.
 *  A `local: "session"` table stays outside it: never persisted, never replicated across tabs
 *  (`207-LOCAL-TABLE-PERSISTENCE-DESIGN.md` §5.4). */
export function persistedLocalTableNames<S extends ColsMap>(schema: Schema<S>): Set<string> {
  return new Set(Object.keys(schema.tables).filter((n) => schema.tables[n].local === true));
}

/** A stable fingerprint of the schema's PERSISTED local tables only — the persistence gate's
 *  `schemaHash` (`207-LOCAL-TABLE-PERSISTENCE-DESIGN.md` §3.3 / P7). Per `local: true` table:
 *  `(name, ordered column names + types + optionality, pk columns)`. Column order is kept (rows are
 *  positional); tables are sorted by name so registration order can't skew it; synced-table AND
 *  `local: "session"` changes never move it (reshaping an ephemeral table must not wipe durable
 *  data). The value is the canonical descriptor itself, not a digest — local-table sets are tiny,
 *  and exactness (no collision can ever skip a P7 clear) beats compactness here. */
export function localSchemaHash<S extends ColsMap>(schema: Schema<S>): string {
  // Derived from persistedLocalTableNames — the hash's contract is "fingerprints exactly the
  // tables the plane persists", so the two must share one predicate, not two copies of it.
  const tables = [...persistedLocalTableNames(schema)]
    .sort()
    .map((n) => {
      const meta = schema.tables[n];
      const cols = Object.keys(meta.columns).map((c) => [c, meta.columns[c].type, meta.columns[c].optional === true]);
      return [n, cols, meta.primaryKey];
    });
  return `v1:${JSON.stringify(tables)}`;
}

// ----------------------------- relationships (FRAGMENT-COMPOSITION-DESIGN §4.2, named edges) -----
//
// A relationship is the correlation (`parent.col → child.col`) declared ONCE as a value, so `sub`,
// `countAs`, and `exists` don't restate `{ parent, child }` keys at every spread/filter site. It is a
// plain typed value (not registered on the schema), passed where `(child, corr)` used to go.

/** Brand on a {@link Relationship} value (a `unique symbol`, distinct from a {@link TableDef}). */
const RELATIONSHIP_BRAND: unique symbol = Symbol("rindle.relationship");

/**
 * A reusable, typed JOIN between two tables — the correlation declared once (design §4). Built with
 * {@link rel}; parameterized by the parent columns `PC` (so a `sub` checks the relationship belongs to
 * the query's table) and the child columns `CC` (which flow into the nested result type). Pass it to
 * `sub`/`countAs`/`exists` in place of an explicit `child` + `{ parent, child }` correlation.
 */
export interface Relationship<PC extends AnyCols, CC extends AnyCols> {
  /** The child table the relationship points at. */
  readonly child: TableLike<CC>;
  /** Correlation keys: `parent[i]` (a parent column) joins to `child[i]` (a child column). */
  readonly correlation: { readonly parent: readonly string[]; readonly child: readonly string[] };
  /** Phantom binding the parent columns so `Query<C>.sub(alias, rel)` rejects a rel for another table. */
  readonly __parent?: PC;
  readonly [RELATIONSHIP_BRAND]: true;
}

/** Any relationship, for positions that only read its correlation / child table. */
export type AnyRelationship = Relationship<AnyCols, AnyCols>;

/**
 * Declare a relationship once: `rel(issue, user, { ownerId: "id" })` means `issue.ownerId → user.id`.
 * `mapping` is `{ [parentColumn]: childColumn }` (a composite join is multiple entries). The `parent`
 * table is used only to type-check the keys; pass the result to `sub`/`countAs`/`exists`.
 */
export function rel<PC extends AnyCols, CC extends AnyCols>(
  _parent: TableLike<PC>,
  child: TableLike<CC>,
  mapping: Partial<Record<keyof PC & string, keyof CC & string>>,
): Relationship<PC, CC> {
  const parent = Object.keys(mapping);
  const childKeys = parent.map((k) => mapping[k as keyof PC & string] as string);
  return { child, correlation: { parent, child: childKeys }, [RELATIONSHIP_BRAND]: true };
}

/** A typed registry of named {@link Relationship}s — `defineRelationships({ issueOwner: rel(...) })`.
 *  A thin identity helper that names the bag and constrains its values; the keys are yours to choose. */
export function defineRelationships<R extends Record<string, AnyRelationship>>(rels: R): R {
  return rels;
}

/** Runtime guard: is `v` a {@link Relationship} value (not a table or a plain object)? */
export function isRelationship(v: unknown): v is AnyRelationship {
  return typeof v === "object" && v !== null && (v as Partial<AnyRelationship>)[RELATIONSHIP_BRAND] === true;
}

/** The `SchemaSpec` (`columns` + `primaryKey` indices) the wasm `Db.registerTable` wants. */
export function tableSpec(meta: TableMeta): { columns: string[]; primaryKey: number[] } {
  const columns = Object.keys(meta.columns);
  const primaryKey = meta.primaryKey.map((k) => columns.indexOf(k));
  return { columns, primaryKey };
}

/** A table's insert-completeness plan, derived once from its `Col` markers and shared by BOTH write
 *  funnels — the client `trackingTx` and the server `renderOp` — so their required-sets can't drift
 *  (design 206 §6.1/§6.2). A `NOT NULL` column is `required`; a nullable column (`.nullable()` set
 *  `Col.optional`) may be omitted and is filled with `null` (see {@link insertCell}). PK columns are
 *  never nullable (introspection forces them non-null), so they are always required. */
export interface InsertPlan {
  /** Every column, in wire order. */
  readonly columns: string[];
  /** Columns that MUST be present on a full insert — the non-nullable ones. */
  readonly required: string[];
  /** The nullable (omittable-to-null) columns, for a fast membership test in the fill. */
  readonly nullable: ReadonlySet<string>;
}

/** Derive a table's {@link InsertPlan} from its column markers. */
export function insertPlan(meta: TableMeta): InsertPlan {
  const columns = Object.keys(meta.columns);
  const nullable = new Set(columns.filter((c) => meta.columns[c].optional === true));
  return { columns, required: columns.filter((c) => !nullable.has(c)), nullable };
}

/** The cell a full insert writes for column `c`: the given value, or `null` when a nullable column
 *  is omitted (design 206 §6.2). The caller's completeness check ({@link InsertPlan.required})
 *  guarantees a non-nullable column is present, so its omission never reaches here. */
export function insertCell(row: Record<string, WireValue>, c: string): WireValue {
  return c in row ? row[c] : null;
}

/** Encode a keyed-row cell to its wire {@link WireValue} for a column KIND — the one place both write
 *  funnels stringify a `json<T>` object (the typed mutator surface / design 206 §7). An
 *  already-stringified json value (a `string`) or any non-json cell passes through unchanged, so a
 *  mutator may pass EITHER a parsed object OR a JSON string. Mirrors `store.positionalize`. */
export function toCell(v: WireValue | object, type: ColType): WireValue {
  if (type === "json" && v !== null && typeof v === "object") return JSON.stringify(v);
  return v as WireValue;
}

/** The client's per-table flat schema (name + column order + PK indices), the shape a
 *  normalized `hello` advertises (NORMALIZED-CHANGES-DESIGN.md §3). Used to validate a
 *  server hello against the CLIENT's own typed schema so a column-order / PK skew is caught
 *  instead of silently transposing positional cells (CRIT#4). Sorted by name for stable
 *  ordering.
 *
 *  Local-only tables are **omitted** (`201-LOCAL-ONLY-TABLES-DESIGN.md` E1): the client never
 *  claims them to the server, the server never expects/ships them, and they stay out of the schema
 *  fingerprint (`normalizedFp`) so a local table can't skew it vs. the server's. */
export function normalizedTableSchemas<S extends ColsMap>(
  schema: Schema<S>,
): { name: string; columns: string[]; primaryKey: number[] }[] {
  return Object.keys(schema.tables)
    .filter((name) => !schema.tables[name].local)
    .sort()
    .map((name) => ({ name, ...tableSpec(schema.tables[name]) }));
}
