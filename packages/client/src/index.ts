// @rindle/client — the backend-agnostic flat-change client core: typed schema, query builder,
// comparator, Backend seam, and the ArrayView contract. (The ArrayView folding impl + the
// Store/backend wiring land in later slices.)

export * from "./schema.ts"; // table/columns/primaryKey, createSchema/extendSchema, refineTable/refineSchema, string/number/boolean/json, Col, RowOf, Row, InsertOf, Insert, insertPlan, insertCell, TableDef, tableSpec, SCHEMA
export * from "./operators.ts"; // eq/ne/gt/ge/lt/le/like/notLike/ilike/notIlike/inList/notInList/is/isNot, and/or, Pred, Cond, Arg
export * from "./query.ts"; // Query, QueryRoot, queries/newQueryBuilder, defineQuery/NamedQuery, exists, notExists, defineFragment/Fragment/FragmentRef/isFragment
export * from "./view.ts"; // ArrayView, SingularArrayView, FlatArrayView, SingularView, ViewTypes
export * from "./store.ts"; // Store, WriteTx, AssembledNode, DehydratedQuery, DehydratedState
export * from "./ensure.ts"; // QueryEnsureCache, EnsureQueryOptions, QueryEnsurer
export * from "./ssr.ts"; // createServerStore, ServerStore, OneShotBackend, OneShotQueryFn, OneShotResult, ServerStoreOptions

export { resolveChange, subRow } from "./resolve.ts"; // positional FlatChange → named rows (inverse of the wire encoder)
export type { NamedRow, ResolvedChange } from "./resolve.ts";

export type { KeyedRow, MutationOp, ServerWriteTx } from "./mutation-ops.ts"; // isomorphic mutation op vocabulary
// shared (generator) mutator seam — one body, sync on the client + async on the server
export {
  defineMutators,
  driveMutationAsync,
  driveMutationSync,
  isGeneratorMutator,
  isoTx,
  shared,
} from "./mutation-ops.ts";
export type {
  ArgSchema,
  AsyncEffectExec,
  BatchEffect,
  IsoTx,
  IsoTxOf,
  MutationGen,
  MutatorCtx,
  QueryArg,
  QueryEffect,
  QueryResultRow,
  ReadEffect,
  SharedMutator,
  SharedMutatorWithArgs,
  SyncEffectExec,
  YieldEffect,
} from "./mutation-ops.ts";

export { stableKey } from "./key.ts"; // canonical viewKey for an AST (shared with @rindle/react)

export { COMPARATOR_VERSION, compareNumber, compareRows, compareString, compareValue } from "./compare.ts";

export type {
  Backend,
  BackendDevObserver,
  BackendServerDelta,
  ChangeEvent,
  ColType,
  FlatChange,
  FlatOp,
  Mutation,
  MutationEnvelope,
  MutationOutcomeFrame,
  NormalizedEvent,
  NormalizedOp,
  NormalizedSource,
  NormalizedTableSchema,
  OptimisticSource,
  PathSeg,
  ProgressFrame,
  QueryId,
  RemoteQuery,
  ResultType,
  WireNode,
  WireProjection,
  WireRel,
  WireSchema,
  WireValue,
} from "./types.ts";

export { CLIENT_MUTATIONS_SCHEMA, CLIENT_MUTATIONS_TABLE, LMID_QUERY_NAME } from "./types.ts";

// The Zero-wire AST types (for backends / advanced use).
export type {
  Aggregate,
  Ast,
  Bound,
  Condition,
  CorrelatedSubquery,
  Correlation,
  Dir,
  ExistsOp,
  LitValue,
  OrderPart,
  SimpleOp,
  ValuePosition,
} from "./ast.ts";
