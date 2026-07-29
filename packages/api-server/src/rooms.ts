// Room profiles — the DECLARATION layer of Rindle Realtime query enablement
// (designs/RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §2): the api-server owns a small set of
// NAMED room profiles (key derivation + canonical unwindowed footprint + read-only context
// tables), and client queries opt into one via their `defineQuery` realtime label. This module is
// the pure-TS compile/validate half — "loud at registration" (§2.3): every rule checkable at
// `createRindleApiServer` construction throws (or warns) THERE, not at a 3am room boot.
//
// The serve DECISION (lease wire changes, room tokens) consumes the {@link CompiledRoomProfile}
// records this module produces — room-serving trusts the declaration (302 §5), no coverage proof. Only
// {@link RoomProfile}, {@link queryResultToAst} and {@link queryRealtimeLabel} are re-exported
// from the package index — the compiled shapes and the wire-key helpers stay internal.

import type { Ast, Condition, CorrelatedSubquery, RealtimeQueryLabel, Schema } from "@rindle/client";

// Type-only (erased at runtime) — no import cycle with ./index.ts.
import type { ApiContext, ApiQuery, ApiQueryResult, MaybePromise } from "./index.ts";

// ------------------------------------------------------------------------------- the declaration

/**
 * One NAMED room profile (§2.1). Rooms are document-scoped, not query-scoped: a profile is what
 * stands up and feeds a room — the flat `resolveFootprint` promoted to a named, reusable unit.
 * Labeled queries opt into a profile; the wire room key for a named profile is minted SERVER-side
 * as `"<profile>/<key>"`, and `/room-boot` splits it back to resolve the footprint.
 */
export interface RoomProfile<User = unknown> {
  /** Derive the profile-local room key from the (label-mapped) query args. Runs server-side under
   *  authoritative inputs — the client learns the key from the lease, it never computes one. */
  key: (args: any) => string;
  /** Build the room's canonical footprint — the replica boundary the room loads and follows —
   *  from the profile-local doc key (this profile's `key` output; what `/room-boot` receives
   *  after the prefix split). MUST be unwindowed (§2.3): no `limit`/`start`/`one` anywhere in the
   *  AST — enforced loudly at construction when statically resolvable, and again at every boot.
   *  Should be a TOTAL builder over any key (a shape constructor, not an authorizer). */
  footprint: (docKey: string, ctx: ApiContext<User>) => MaybePromise<ApiQueryResult>;
  /** Loaded-but-never-room-written tables (§2.2 — the "followed" set: real external writers, the
   *  daemon stays write-authoritative for them). Must be a subset of the footprint's tables; the
   *  room's writable scope is footprint minus context. Defaults to `[]`. */
  context?: readonly string[];
}

export function queryResultToAst(result: ApiQueryResult): Ast {
  if (result && typeof result === "object" && "ast" in result && typeof result.ast === "function") {
    return result.ast();
  }
  return result as Ast;
}

// ------------------------------------------------------------------------------- wire room keys

/** The wire room-key delimiter: a named profile's doc is `"<profile>/<key>"`; a doc with no known
 *  profile prefix belongs to the legacy anonymous profile (`resolveFootprint`, bare-key form). */
export const ROOM_DOC_SEPARATOR = "/";

/** Mint the wire room doc for a named profile (the lease path — G-iv-b — mints these). */
export function mintRoomDoc(profile: string, key: string): string {
  return `${profile}${ROOM_DOC_SEPARATOR}${key}`;
}

/** Split a wire room doc at the FIRST separator. `undefined` for a bare doc (no separator, or an
 *  empty would-be profile name). The CALLER decides whether the prefix actually names a profile —
 *  a legacy doc may itself contain `/`, and then falls through to the legacy resolver whole. */
export function splitRoomDoc(doc: string): { profile: string; key: string } | undefined {
  const i = doc.indexOf(ROOM_DOC_SEPARATOR);
  if (i <= 0) return undefined;
  return { profile: doc.slice(0, i), key: doc.slice(i + 1) };
}

// --------------------------------------------------------------------- §2.3: unwindowed footprints

interface WindowHit {
  kind: string;
  path: string;
}

/**
 * §2.3 "profile footprints are unwindowed", enforced: throw loudly (with the offending AST path)
 * if `limit` / `start` (cursor paging — the skip lowering) / `one` appears ANYWHERE in the
 * footprint tree — the root, a related subquery, or an EXISTS child. A window computed over an
 * unwindowed superset equals the window over the full store, which is what makes room-serving of
 * windowed *queries* sound; a windowed *footprint* would silently break that, so it is rejected
 * by construction. Windows belong on the labeled queries a room serves, never on the footprint.
 */
export function assertUnwindowedFootprint(ast: Ast, profile: string): void {
  const hit = findWindow(ast, "footprint");
  if (hit === undefined) return;
  throw new Error(
    `room profile "${profile}": the footprint must be UNWINDOWED — found ${hit.kind} at ${hit.path}. ` +
      `Windows (limit/start/one) belong on the labeled queries a room serves, never on the room's ` +
      `footprint (RINDLE-REALTIME-QUERY-ENABLEMENT §2.3).`,
  );
}

function findWindow(ast: Ast, path: string): WindowHit | undefined {
  if (ast.limit !== undefined) return { kind: `limit(${ast.limit})`, path: `${path}.limit` };
  if (ast.start !== undefined) return { kind: "start (cursor paging)", path: `${path}.start` };
  if (ast.one === true) return { kind: "one()", path: `${path}.one` };
  for (const [i, rel] of (ast.related ?? []).entries()) {
    const hit = findWindow(rel.subquery, `${path}.related[${rel.subquery.alias ?? i}]`);
    if (hit !== undefined) return hit;
  }
  return (
    findWindowInCondition(ast.where, `${path}.where`) ?? findWindowInCondition(ast.having, `${path}.having`)
  );
}

function findWindowInCondition(cond: Condition | undefined, path: string): WindowHit | undefined {
  if (cond === undefined) return undefined;
  switch (cond.type) {
    case "simple":
      return undefined;
    case "and":
    case "or": {
      for (const [i, c] of cond.conditions.entries()) {
        const hit = findWindowInCondition(c, `${path}.${cond.type}[${i}]`);
        if (hit !== undefined) return hit;
      }
      return undefined;
    }
    case "correlatedSubquery": {
      const label = cond.op === "EXISTS" ? "exists" : "notExists";
      const sub = cond.related.subquery;
      return findWindow(sub, `${path}.${label}[${sub.alias ?? sub.table}]`);
    }
  }
}

// -------------------------------------------------------- footprint facts (tables + join keys)

export interface FootprintScan {
  tables: Set<string>;
  joinKeys: Map<string, Set<string>>;
}

/** Walk the footprint: every base table it draws rows from, and — per table — the correlation
 *  (join-key) columns every related/EXISTS edge binds on it. These are §3.2's routing metadata
 *  and slice H's "room mutators never write join keys" enforcement input; G-iv-b ships them to
 *  the room engine as part of the RoomTableSpec compilation ({@link compileRoomTableSpecs}). */
export function scanFootprint(ast: Ast): FootprintScan {
  const scan: FootprintScan = { tables: new Set(), joinKeys: new Map() };
  scanAst(ast, scan);
  return scan;
}

function addJoinKeys(scan: FootprintScan, table: string, cols: readonly string[]): void {
  let set = scan.joinKeys.get(table);
  if (set === undefined) {
    set = new Set();
    scan.joinKeys.set(table, set);
  }
  for (const c of cols) set.add(c);
}

function scanAst(ast: Ast, scan: FootprintScan): void {
  scan.tables.add(ast.table);
  for (const rel of ast.related ?? []) scanEdge(ast.table, rel, scan);
  scanCondition(ast.table, ast.where, scan);
  scanCondition(ast.table, ast.having, scan);
}

function scanEdge(parentTable: string, edge: CorrelatedSubquery, scan: FootprintScan): void {
  addJoinKeys(scan, parentTable, edge.correlation.parentField);
  addJoinKeys(scan, edge.subquery.table, edge.correlation.childField);
  scanAst(edge.subquery, scan);
}

function scanCondition(table: string, cond: Condition | undefined, scan: FootprintScan): void {
  if (cond === undefined) return;
  switch (cond.type) {
    case "simple":
      return;
    case "and":
    case "or":
      for (const c of cond.conditions) scanCondition(table, c, scan);
      return;
    case "correlatedSubquery":
      scanEdge(table, cond.related, scan);
      return;
  }
}

// ------------------------------------------------------- RoomTableSpec (the lease wire, G-iv-b)

/**
 * One footprint table's spec on the lease wire (`QueryLeaseResponse.realtime.tables`): the §2.2
 * owned/followed split plus §3.2's per-table routing metadata, compiled from the RESOLVED
 * footprint AST at lease time (so key-dependent and non-static profiles compile too).
 *
 * `writable`:
 * - `none` — a context table (§2.2 "followed"): loaded by the room, never room-written; the
 *   daemon stays write-authoritative.
 * - `predicate` — a writable table. `where` is the ROW-LOCAL part of the footprint's predicate
 *   for this table (simple column-vs-literal conditions composed with and/or); an ABSENT `where`
 *   means no row-local constraint — every row the room holds for this table is in the writable
 *   scope. `joinKeyCols` are the correlation columns the footprint binds on this table (slice
 *   H's "room mutators never write join keys" enforcement input).
 *
 * Dropping the non-row-local parts (correlated/EXISTS conditions, column-vs-column comparisons)
 * only ever WIDENS the predicate, which is sound here: the room only relays rows the footprint
 * materialized, so the held-row set — not this predicate — is the real outer bound; `where` is a
 * row-local refinement of it, and a wider refinement can never mark a held footprint row
 * non-writable that the full predicate would have allowed. (An OR with any non-row-local
 * disjunct is dropped WHOLE — keeping only some disjuncts would narrow, which is not sound.)
 *
 * `footprintWhere` (H-iii — the lease-wire flip): the EXACT footprint-membership predicate,
 * identical to {@link RoomScopeSpec.footprintWhere} (ONE compiler feeds both wires — the lease's
 * table specs and the boot wire's scopes are now the SAME objects). The client's §3 router
 * consumes it for the pk-membership read proof; it is advisory routing metadata, never a
 * credential (the room gate re-proves engine-side, H-iv).
 */
export type RoomTableSpec = {
  table: string;
  footprintWhere?: Condition;
  writable:
    | { kind: "none" }
    | { kind: "predicate"; where?: Condition; joinKeyCols: string[] };
};

/**
 * One footprint table's FULL scope spec (slice H-iv-b): byte-compatible with
 * `rindle-room-core`'s `TableScopeSpec` (scope.rs), the wasm room's `enableWritesV2` input. It
 * rides the boot wire (`RoomBootResponse.scopes`); since slice H-iii the LEASE wire's
 * {@link RoomTableSpec} carries `footprintWhere` too (the client-side routing proof), so the two
 * shapes are now structurally IDENTICAL — kept as two names because they are two wires with two
 * consumers (the room gate vs the client router).
 *
 * `footprintWhere` is computed for EVERY footprint table — context (`kind: "none"`) ones
 * included, because the gate proves ABSENT READS on any readable table with it (§3.1: an absent
 * read is covered only when the footprint predicate is decidable from the pk columns alone and
 * evaluates true on the read key). ABSENT (no exact membership predicate exists) ⇒ no absent
 * read on that table is provable — the room gate fails closed to a deopt.
 *
 * **`footprintWhere` is EXACT-only — the opposite discipline from the writable `where`.** The
 * writable side ships the WIDENING extraction ({@link tablePredicate}) and that is sound (the
 * held-row set is the real outer bound). The absent-read proof points the other way: it uses
 * `footprintWhere` to conclude "a row with this pk would have been IN the footprint, so the
 * room's absence is truth's absence" — a WIDENED predicate over-claims exactly there (a root
 * `where` mixing pk-column conjuncts with a dropped EXISTS, or a correlated child whose own
 * `where` reads only its pk, would prove an absence the dropped part can contradict). So
 * `footprintWhere` is emitted ONLY when the membership predicate is exact
 * ({@link exactMembershipPredicate}): every node reading the table is the footprint ROOT (a
 * child node's implicit correlation to its parent is itself a dropped, non-row-local constraint)
 * and the root's `where` extraction dropped nothing. An unconstrained exact root (the whole
 * table rides the footprint) emits the vacuous-true `{type:"and",conditions:[]}` — the gate's
 * combinator pins empty-AND = true — so whole-table footprints keep provable absent reads.
 * Child/correlated tables get NO `footprintWhere` and their absent reads deopt (fail closed);
 * propagating a parent constraint through a pk-covering correlation is a future refinement.
 */
export type RoomScopeSpec = {
  table: string;
  footprintWhere?: Condition;
  writable: RoomTableSpec["writable"];
};

/** Compile the per-table {@link RoomScopeSpec}s from a RESOLVED footprint AST + the profile's
 *  context set — the ONE compiler both wires derive from (the boot wire ships it whole; the
 *  lease wire serializes the {@link RoomTableSpec} projection via {@link compileRoomTableSpecs}).
 *  Deterministic output order (tables sorted by name). */
export function compileRoomScopeSpecs(footprint: Ast, context: ReadonlySet<string>): RoomScopeSpec[] {
  const scan = scanFootprint(footprint);
  const specs: RoomScopeSpec[] = [];
  for (const table of [...scan.tables].sort()) {
    const where = tablePredicate(footprint, table);
    const spec: RoomScopeSpec = { table, writable: { kind: "none" } };
    // footprintWhere is EXACT-only (see the RoomScopeSpec doc): the widened `where` feeds the
    // writable side below, never the absent-read proof.
    const membership = exactMembershipPredicate(footprint, table);
    if (membership !== undefined) spec.footprintWhere = membership;
    if (!context.has(table)) {
      const joinKeyCols = [...(scan.joinKeys.get(table) ?? [])].sort();
      spec.writable =
        where === undefined ? { kind: "predicate", joinKeyCols } : { kind: "predicate", where, joinKeyCols };
    }
    specs.push(spec);
  }
  return specs;
}

/** The vacuous-true wire condition (`empty AND` — the room gate's combinator pins it true): what
 *  an EXACT but unconstrained footprint root emits as its membership predicate. */
const CONDITION_TRUE: Condition = { type: "and", conditions: [] };

/** The footprint's EXACT membership predicate for `table`, or `undefined` when none exists (see
 *  the {@link RoomScopeSpec} doc for why absent-read proofs must never see a widened one):
 *  defined iff every node reading the table is the footprint ROOT node (a child node's implicit
 *  parent correlation is a dropped constraint, so any child/EXISTS occurrence forfeits exactness)
 *  and the root's `where` extraction is LOSSLESS. An exact unconstrained root (whole table in the
 *  footprint) yields {@link CONDITION_TRUE}. */
function exactMembershipPredicate(footprint: Ast, table: string): Condition | undefined {
  const nodes: CollectedNode[] = [];
  collectTableNodes(footprint, table, nodes);
  if (nodes.length === 0 || !nodes.every((n) => n.isRoot)) return undefined;
  // isRoot can only be true for the top-level node, so an all-root list has exactly one entry.
  const { cond, exact } = rowLocalExtraction(nodes[0].node.where);
  if (!exact) return undefined;
  return cond ?? CONDITION_TRUE;
}

/** Compile the per-table {@link RoomTableSpec}s from a RESOLVED footprint AST + the profile's
 *  context set — since H-iii the lease wire carries `footprintWhere` exactly where the boot wire
 *  does, so this IS {@link compileRoomScopeSpecs} (one compiler, two wires; the identity is
 *  pinned by serve-proof.test.ts). Kept as a named entry point for the lease call site. */
export function compileRoomTableSpecs(footprint: Ast, context: ReadonlySet<string>): RoomTableSpec[] {
  return compileRoomScopeSpecs(footprint, context);
}

/** The footprint's row-local predicate for `table`: per NODE reading the table, the row-local
 *  extraction of its `where`; multiple nodes OR together (a row held under EITHER node's
 *  predicate is in the footprint's row set). Any unconstrained node ⇒ `undefined` (no row-local
 *  constraint at all — the widest sound answer). */
function tablePredicate(ast: Ast, table: string): Condition | undefined {
  const nodes: CollectedNode[] = [];
  collectTableNodes(ast, table, nodes);
  const parts: Condition[] = [];
  for (const { node } of nodes) {
    const local = rowLocalCondition(node.where);
    if (local === undefined) return undefined; // one unconstrained node ⇒ the table is unconstrained
    parts.push(local);
  }
  if (parts.length === 0) return undefined;
  return parts.length === 1 ? parts[0] : { type: "or", conditions: parts };
}

/** One occurrence of a table in the footprint tree. `isRoot` is true only for the TOP-LEVEL node
 *  — a `related`/EXISTS child carries an implicit correlation to its parent, which is a dropped,
 *  non-row-local membership constraint ({@link exactMembershipPredicate} forfeits on it). */
interface CollectedNode {
  node: Ast;
  isRoot: boolean;
}

function collectTableNodes(ast: Ast, table: string, out: CollectedNode[], isRoot = true): void {
  if (ast.table === table) out.push({ node: ast, isRoot });
  for (const rel of ast.related ?? []) collectTableNodes(rel.subquery, table, out, false);
  collectTableNodesInCondition(ast.where, table, out);
  collectTableNodesInCondition(ast.having, table, out);
}

function collectTableNodesInCondition(cond: Condition | undefined, table: string, out: CollectedNode[]): void {
  if (cond === undefined) return;
  switch (cond.type) {
    case "simple":
      return;
    case "and":
    case "or":
      for (const c of cond.conditions) collectTableNodesInCondition(c, table, out);
      return;
    case "correlatedSubquery":
      collectTableNodes(cond.related.subquery, table, out, false);
      return;
  }
}

/** Extract the row-local part of one node's condition tree, WIDENING (see {@link RoomTableSpec}):
 *  `undefined` = "no constraint kept". The writable side's entry point — for absent-read proofs
 *  use {@link exactMembershipPredicate}, never this. */
function rowLocalCondition(cond: Condition | undefined): Condition | undefined {
  return rowLocalExtraction(cond).cond;
}

/** The row-local extraction WITH exactness: `exact` means `cond` is equivalent to the input —
 *  nothing was dropped (an `undefined` input is exactly the vacuous-true constraint). Kept:
 *  simple column-vs-literal comparisons; `and` keeps its extractable conjuncts (dropping the
 *  rest widens ⇒ inexact); `or` survives only when EVERY disjunct is extractable (dropping a
 *  disjunct would narrow — the whole OR drops instead, inexact); `correlatedSubquery` always
 *  drops (inexact). */
function rowLocalExtraction(cond: Condition | undefined): { cond: Condition | undefined; exact: boolean } {
  if (cond === undefined) return { cond: undefined, exact: true };
  switch (cond.type) {
    case "simple": {
      const columnVsLiteral =
        (cond.left.type === "column" && cond.right.type === "literal") ||
        (cond.left.type === "literal" && cond.right.type === "column");
      return columnVsLiteral ? { cond, exact: true } : { cond: undefined, exact: false };
    }
    case "and": {
      const parts = cond.conditions.map(rowLocalExtraction);
      const kept = parts.map((p) => p.cond).filter((c): c is Condition => c !== undefined);
      const exact = parts.every((p) => p.exact);
      if (kept.length === 0) return { cond: undefined, exact };
      return { cond: kept.length === 1 ? kept[0] : { type: "and", conditions: kept }, exact };
    }
    case "or": {
      const parts = cond.conditions.map(rowLocalExtraction);
      // DROPPING a disjunct would narrow (unsound even for the widening direction) — the whole
      // OR drops. But a disjunct that merely WIDENED (a partially-extracted AND) stays: widening
      // every disjunct widens the OR, which is fine for the writable side and marked inexact.
      if (parts.some((p) => p.cond === undefined)) return { cond: undefined, exact: false };
      const kept = parts.map((p) => p.cond as Condition);
      return {
        cond: kept.length === 1 ? kept[0] : { type: "or", conditions: kept },
        exact: parts.every((p) => p.exact),
      };
    }
    case "correlatedSubquery":
      return { cond: undefined, exact: false }; // never row-local; the room relays footprint rows
  }
}

// ------------------------------------------------------------------------- the compiled profile

/** Statically-derived footprint facts (symbolic-key resolution at construction). Absent when the
 *  footprint could not be statically resolved (async / ctx-dependent / throwing on the probe) —
 *  then the unwindowed rule is enforced at each boot instead and the join keys are underivable
 *  until boot. @internal — consumed by G-iv-b; not part of the package's public API. */
export interface CompiledFootprintFacts {
  /** The footprint AST resolved with {@link PROBE_DOC_KEY} — its SHAPE is representative, its
   *  literals are not (they embed the probe key). */
  readonly ast: Ast;
  /** Every base table the footprint draws rows from. */
  readonly tables: ReadonlySet<string>;
  /** §2.2 writable scope = footprint tables minus context. */
  readonly writableTables: ReadonlySet<string>;
  /** table → the correlation (join-key) columns the footprint binds on it. */
  readonly joinKeys: ReadonlyMap<string, ReadonlySet<string>>;
}

/** One compiled room profile — the internal record the G-iv-b serve decision + RoomTableSpec
 *  compilation consume (name, key fn, footprint resolver, context set, per-table join keys).
 *  @internal — deliberately NOT re-exported from the package index. */
export interface CompiledRoomProfile<User = unknown> {
  readonly name: string;
  readonly key: (args: any) => string;
  readonly footprint: (docKey: string, ctx: ApiContext<User>) => MaybePromise<ApiQueryResult>;
  /** §2.2 followed set (room-read-only; the daemon stays write-authoritative for these). */
  readonly context: ReadonlySet<string>;
  /** Present iff the footprint was statically resolvable at construction. */
  readonly static: CompiledFootprintFacts | undefined;
}

/** The symbolic doc key construction-time validation resolves each footprint with. A profile
 *  footprint should be a TOTAL builder over any key; one that throws on this key (or is async /
 *  ctx-dependent) simply defers its unwindowed check to boot, with a loud warning. */
export const PROBE_DOC_KEY = "__rindle-room-profile-probe__";

export interface CompileRoomProfilesInput<User> {
  rooms: Record<string, RoomProfile<User>> | undefined;
  /** The typed schema, when configured — enables the context-table existence check. */
  schema: Schema | undefined;
  /** Loud-diagnostics sink (defaults to `console.warn`); injectable for tests. */
  warn?: (message: string) => void;
}

/**
 * Compile + validate the named room profiles — §2.3 "loud at registration". Rules enforced here:
 * the profile name is wire-key-safe (no `/`); (b) the footprint is UNWINDOWED (throw, with the
 * offending AST path); (c) context tables exist in the schema and are a subset of the footprint's
 * tables (throw); (d) per-table join-key columns are derived and RECORDED on the compiled profile
 * (a loud WARNING when the footprint isn't statically resolvable and they can't be).
 */
export function compileRoomProfiles<User>(
  input: CompileRoomProfilesInput<User>,
): Map<string, CompiledRoomProfile<User>> {
  const warn = input.warn ?? ((message: string) => console.warn(message));
  const out = new Map<string, CompiledRoomProfile<User>>();
  for (const [name, profile] of Object.entries(input.rooms ?? {})) {
    if (name.length === 0 || name.includes(ROOM_DOC_SEPARATOR)) {
      throw new Error(
        `realtime.rooms: invalid profile name ${JSON.stringify(name)} — profile names are non-empty and may ` +
          `not contain "${ROOM_DOC_SEPARATOR}" (it delimits the wire room key "<profile>${ROOM_DOC_SEPARATOR}<key>").`,
      );
    }
    if (typeof profile.key !== "function") {
      throw new Error(`realtime.rooms.${name}: \`key\` must be a function (args) => string.`);
    }
    if (typeof profile.footprint !== "function") {
      throw new Error(`realtime.rooms.${name}: \`footprint\` must be a function (docKey, ctx) => Query | Ast.`);
    }
    const context: ReadonlySet<string> = new Set(profile.context ?? []);
    if (input.schema !== undefined) {
      for (const t of context) {
        if (input.schema.tables[t] === undefined) {
          throw new Error(
            `realtime.rooms.${name}: context table "${t}" is not in the schema — \`context\` lists the ` +
              `footprint tables the room follows read-only (§2.2).`,
          );
        }
      }
    }
    // Statically resolve the footprint with a symbolic key. An async / ctx-dependent / throwing
    // footprint can't be checked here — warn loudly and defer the unwindowed rule to boot time.
    let ast: Ast | undefined;
    let unresolvable: string | undefined;
    try {
      const result = profile.footprint(PROBE_DOC_KEY, { user: undefined as User });
      if (isThenable(result)) {
        // The probe's promise is deliberately abandoned — swallow a late rejection.
        void Promise.resolve(result).then(undefined, () => undefined);
        unresolvable = "the footprint is async";
      } else {
        ast = queryResultToAst(result);
      }
    } catch (e) {
      unresolvable = `the footprint threw on the symbolic probe key: ${String((e as Error)?.message ?? e)}`;
    }
    let staticFacts: CompiledFootprintFacts | undefined;
    if (ast !== undefined) {
      assertUnwindowedFootprint(ast, name); // (b) — throws with the offending path
      const scan = scanFootprint(ast);
      for (const t of context) {
        if (!scan.tables.has(t)) {
          throw new Error(
            `realtime.rooms.${name}: context table "${t}" is not loaded by the footprint (footprint tables: ` +
              `${[...scan.tables].sort().map((x) => `"${x}"`).join(", ")}) — context must be a SUBSET of the ` +
              `footprint's tables: followed means loaded-but-room-read-only (§2.2).`,
          );
        }
      }
      staticFacts = {
        ast,
        tables: scan.tables,
        writableTables: new Set([...scan.tables].filter((t) => !context.has(t))),
        joinKeys: scan.joinKeys,
      };
    } else {
      warn(
        `realtime.rooms.${name}: the footprint could not be statically resolved at registration ` +
          `(${unresolvable}) — the unwindowed check (§2.3) will run at each room boot instead, and the ` +
          `per-table join-key columns (§3.2 routing metadata) could not be derived.`,
      );
    }
    out.set(name, { name, key: profile.key, footprint: profile.footprint, context, static: staticFacts });
  }
  return out;
}

function isThenable(v: unknown): v is PromiseLike<unknown> {
  return v !== null && typeof v === "object" && typeof (v as PromiseLike<unknown>).then === "function";
}

// ------------------------------------------------------------------ realtime-label carry-through
//
// `registerQueries` erases everything but a bare resolve function at its seam; the realtime label
// must survive it so the lease path (G-iv-b) can look up (profile, args mapping) by query name.
// It rides a symbol property on the registered wrapper; {@link queryRealtimeLabel} is the ONE
// accessor — G-iv-b reads through it, never through ad-hoc properties.

const API_QUERY_REALTIME_LABEL: unique symbol = Symbol("rindle.apiQuery.realtimeLabel");

/** @internal Stamp a registered query wrapper with its `defineQuery` realtime label. */
export function attachRealtimeLabel<F extends object>(wrapper: F, label: RealtimeQueryLabel): F {
  return Object.assign(wrapper, { [API_QUERY_REALTIME_LABEL]: label });
}

/** The realtime label a registered query carries (stamped by `registerQueries` from its
 *  `defineQuery` options), or `undefined` for an unlabeled query. The lease path uses this to
 *  derive `(room profile, args mapping)` for a named query. */
export function queryRealtimeLabel(
  query: ApiQuery<any, any> | undefined,
): RealtimeQueryLabel | undefined {
  if (typeof query !== "function") return undefined;
  return (query as { [API_QUERY_REALTIME_LABEL]?: RealtimeQueryLabel })[API_QUERY_REALTIME_LABEL];
}

/** Rule (a) of the §2.3 "loud at registration" checks: every labeled registered query must name
 *  an existing room profile — a label pointing at nothing is config drift that would otherwise
 *  surface as a query that silently never room-serves. */
export function assertLabeledProfilesExist<User>(
  queries: Record<string, ApiQuery<User, any>> | undefined,
  profiles: ReadonlyMap<string, CompiledRoomProfile<User>>,
): void {
  for (const [name, q] of Object.entries(queries ?? {})) {
    const label = queryRealtimeLabel(q);
    if (label === undefined) continue;
    if (!profiles.has(label.room)) {
      const configured = [...profiles.keys()];
      throw new Error(
        `query "${name}" is labeled realtime (room profile "${label.room}") but no such profile is ` +
          `configured — ` +
          (configured.length > 0
            ? `configured profiles: ${configured.map((p) => `"${p}"`).join(", ")}. `
            : `no realtime.rooms profiles are configured. `) +
          `Add realtime.rooms["${label.room}"] to createRindleApiServer or remove the label ` +
          `(RINDLE-REALTIME-QUERY-ENABLEMENT §2.1).`,
      );
    }
  }
}
