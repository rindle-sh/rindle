/** A value accepted by Rindle SQL's v1 client. Binary values are deliberately unsupported. */
export type SqlValue = null | string | number | bigint | boolean;

export type SqlArgs = readonly SqlValue[] | Readonly<Record<string, SqlValue>>;

export interface Statement {
  sql: string;
  args?: SqlArgs;
  wantRows?: boolean;
}

export interface Column {
  name: string;
  decltype: string | null;
}

export interface StatementResult {
  columns: Column[];
  rows: SqlValue[][];
  rowsAffected: number;
  lastInsertRowid: string | null;
  rowsRead: number | null;
  rowsWritten: number | null;
}

export interface RoutingMetadata {
  servedBy: "master" | "follower";
  appliedLagMs: number | null;
  fenceFallback: boolean;
}

export interface ExecuteResult {
  result: StatementResult;
  commitCursor: string | null;
  routing: RoutingMetadata;
}

export interface BatchResult {
  results: StatementResult[];
  commitCursor: string | null;
  routing: RoutingMetadata;
}

export interface MigrationInput {
  id: string;
  checksum: string;
  statements: string[];
}

export interface MigrationResult {
  applied: boolean;
  commitCursor: string;
}

export type ReadConsistency = "session" | "strong" | "eventual";
export type IntMode = "bigint" | "number" | "string";
export type RetryScope = "request" | "transaction" | "closure" | "never";
export type TransactionState = "open" | "closed" | "unknown";

export interface OperationOptions {
  signal?: AbortSignal;
}

export interface ExecuteOptions extends OperationOptions {
  consistency?: ReadConsistency;
  sessionCursor?: string | null;
}

export type BatchOptions = ExecuteOptions;

export interface TransactionOptions extends OperationOptions {
  readOnly?: boolean;
  isolation?: "serializable" | "snapshot";
  consistency?: ReadConsistency;
  sessionCursor?: string | null;
}

/** The optimistic mutation whose effects a mutation-aware SQL transaction commits. `mid` is the
 *  requested mutation id; `lmid` is server-owned state and therefore only appears in receipts. */
export interface MutationIdentity {
  clientId: string;
  mid: number;
}

/** The authoritative outcome of a mutation commit or lmid-only rejection. */
export interface MutationReceipt {
  applied: boolean;
  lmid: number;
  commitCursor: string | null;
}

/** Positional rows returned by a read inside an open mutation transaction. */
export interface MutationRows {
  columns: string[];
  rows: SqlValue[][];
}

export interface ExecuteMutationInput extends MutationIdentity {
  /** May be empty: an accepted no-op must still advance lmid and retire the prediction. */
  statements: Statement[];
}

export interface BeginMutationInput extends MutationIdentity {
  /** A pure-write prefix accumulated before the mutator's first read. */
  statements?: Statement[];
  /** Optional first read, coalesced with begin to preserve the mutation fast path. */
  query?: Statement | string;
}

export interface RejectMutationInput extends MutationIdentity {
  reason?: string;
}

export interface SqlMutationTransaction {
  execute(statement: Statement | string, options?: OperationOptions): Promise<void>;
  batch(statements: Statement[], options?: OperationOptions): Promise<void>;
  query(statement: Statement | string, options?: OperationOptions): Promise<MutationRows>;
  commit(options?: OperationOptions): Promise<MutationReceipt>;
  rollback(options?: OperationOptions): Promise<void>;
}

/** Begin-time mid dedup can absorb a replay without opening a transaction; callers must skip the
 *  mutator body in that branch. */
export type BeginMutationResult =
  | { absorbed: true; receipt: MutationReceipt }
  | { absorbed: false; transaction: SqlMutationTransaction; read?: MutationRows };

export interface RetryOptions extends TransactionOptions {
  /** Total closure attempts, including the first. Defaults to 5. */
  maxAttempts?: number;
  /** Initial closure-retry delay. Defaults to 10ms. */
  baseDelayMs?: number;
  /** Maximum closure-retry delay. Defaults to 250ms. */
  maxDelayMs?: number;
}

/** Portable subset of the WHATWG fetch signature used by the client. Keeping the input to the
 *  URL forms we actually emit avoids leaking the DOM-only `RequestInfo` alias into Node/Worker
 *  consumers that compile this package from source. */
export type Fetch = (input: string | URL, init?: RequestInit) => Promise<Response>;

export interface ClientOptions {
  url: string;
  authToken: string;
  consistency?: ReadConsistency;
  sessionCursor?: string | null;
  intMode?: IntMode;
  /** Test/service-binding seam. The default is the runtime's global fetch. */
  fetch?: Fetch;
}

export interface SqlTransaction {
  execute(statement: Statement | string, options?: OperationOptions): Promise<StatementResult>;
  batch(statements: Statement[], options?: OperationOptions): Promise<StatementResult[]>;
  commit(options?: OperationOptions): Promise<{ commitCursor: string | null }>;
  rollback(options?: OperationOptions): Promise<void>;
}

export interface SqlSession {
  execute(statement: Statement | string, options?: ExecuteOptions): Promise<ExecuteResult>;
  batch(statements: Statement[], options?: BatchOptions): Promise<BatchResult>;
  begin(options?: TransactionOptions): Promise<SqlTransaction>;
  withTransaction<T>(fn: (tx: SqlTransaction) => Promise<T>, options?: TransactionOptions): Promise<T>;
  withTransactionRetry<T>(fn: (tx: SqlTransaction) => Promise<T>, options?: RetryOptions): Promise<T>;
  /** One-round-trip optimistic mutation commit (effects + lmid in the same atomic unit). */
  executeMutation(input: ExecuteMutationInput, options?: OperationOptions): Promise<MutationReceipt>;
  /** Open an interactive optimistic mutation transaction, or absorb a replay before it runs. */
  beginMutation(input: BeginMutationInput, options?: OperationOptions): Promise<BeginMutationResult>;
  /** Process a business/pre-flight rejection as an lmid-only commit. */
  rejectMutation(input: RejectMutationInput, options?: OperationOptions): Promise<MutationReceipt>;
  executeDdl(sql: string, options?: OperationOptions): Promise<ExecuteResult>;
  migrate(input: MigrationInput, options?: OperationOptions): Promise<MigrationResult>;
  executeMultiple(sql: string, options?: OperationOptions): Promise<void>;
  session(cursor?: string | null): SqlSession;
  getSessionCursor(): string | null;
  resetSessionCursor(): void;
  ping(options?: OperationOptions): Promise<void>;
}

export interface SqlClient extends SqlSession {
  close(): void;
}

// Frozen v1 JSON value and statement DTOs.
export interface WireI64 {
  $rindle: "i64";
  value: string;
}

export interface WireFloat {
  $rindle: "float";
  value: "Infinity" | "-Infinity";
}

export type WireSqlValue = null | string | number | WireI64 | WireFloat;
export type WireArgs = WireSqlValue[] | Record<string, WireSqlValue>;

export interface WireStatement {
  sql: string;
  args?: WireArgs;
  want_rows?: boolean;
}

export interface WireColumn {
  name: string;
  decltype: string | null;
}

export interface WireStatementResult {
  columns: WireColumn[];
  rows: WireSqlValue[][];
  rows_affected: number;
  last_insert_rowid: string | null;
  rows_read: number | null;
  rows_written: number | null;
}

export interface WireRoutingMetadata {
  served_by: "master" | "follower";
  applied_lag_ms: number | null;
  fence_fallback: boolean;
}

export interface WireExecuteResponse {
  result: WireStatementResult;
  commit_cursor: string | null;
  routing: WireRoutingMetadata;
}

export interface WireBatchResponse {
  results: WireStatementResult[];
  commit_cursor: string | null;
  routing: WireRoutingMetadata;
}
