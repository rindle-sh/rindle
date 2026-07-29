import { RindleSqlError, isRindleSqlError, protocolError } from "./errors.ts";
import { newIdempotencyKey, newRequestId } from "./id.ts";
import type {
  BatchOptions,
  BatchResult,
  BeginMutationInput,
  BeginMutationResult,
  ClientOptions,
  ExecuteMutationInput,
  ExecuteOptions,
  ExecuteResult,
  Fetch,
  IntMode,
  MigrationInput,
  MigrationResult,
  MutationIdentity,
  MutationReceipt,
  MutationRows,
  OperationOptions,
  ReadConsistency,
  RejectMutationInput,
  RetryOptions,
  RetryScope,
  RoutingMetadata,
  SqlClient,
  SqlMutationTransaction,
  SqlSession,
  SqlTransaction,
  Statement,
  StatementResult,
  TransactionOptions,
  TransactionState,
  WireRoutingMetadata,
  WireStatementResult,
} from "./types.ts";
import { decodeSqlValue, encodeStatement } from "./value.ts";

type JsonObject = Record<string, unknown>;

interface CursorState {
  cursor: string | null;
}

interface RequestOptions {
  method?: "GET" | "POST";
  body?: unknown;
  signal?: AbortSignal;
  authenticated?: boolean;
  retrySafe?: boolean;
  expectJson?: boolean;
  /** Scope advertised after this invocation exhausts its bounded request retries. */
  exhaustedRequestRetryScope?: RetryScope;
}

const REQUEST_ATTEMPTS = 3;

/** How many times `withTransaction` re-drives a commit whose outcome is still unknown. */
const COMMIT_RESOLUTION_ATTEMPTS = 3;
const COMMIT_RESOLUTION_BASE_DELAY_MS = 10;

function isRecord(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function requiredString(object: JsonObject, key: string): string {
  const value = object[key];
  if (typeof value !== "string") throw protocolError(`server response field ${key} must be a string`);
  return value;
}

function nullableString(object: JsonObject, key: string): string | null {
  const value = object[key];
  if (value !== null && typeof value !== "string") {
    throw protocolError(`server response field ${key} must be a string or null`);
  }
  return value;
}

function nullableNumber(object: JsonObject, key: string): number | null {
  const value = object[key];
  if (value !== null && (typeof value !== "number" || !Number.isFinite(value))) {
    throw protocolError(`server response field ${key} must be a finite number or null`);
  }
  return value;
}

function requiredNumber(object: JsonObject, key: string): number {
  const value = object[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw protocolError(`server response field ${key} must be a finite number`);
  }
  return value;
}

function optionalString(object: JsonObject, key: string): string | null {
  const value = object[key];
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") throw protocolError(`server response field ${key} must be a string when present`);
  return value;
}

function assertMutationIdentity(input: MutationIdentity): void {
  if (!isRecord(input)) throw new TypeError("mutation input must be an object");
  if (typeof input.clientId !== "string" || input.clientId.length === 0) {
    throw new TypeError("mutation clientId must be a non-empty string");
  }
  if (!Number.isSafeInteger(input.mid) || input.mid < 1) {
    throw new TypeError("mutation mid must be a positive safe integer");
  }
}

function normalizeRetryScope(value: unknown): RetryScope {
  return value === "request" || value === "transaction" || value === "closure" || value === "never"
    ? value
    : "never";
}

function normalizeTransactionState(value: unknown): TransactionState | undefined {
  return value === "open" || value === "closed" || value === "unknown" ? value : undefined;
}

function httpError(response: Response, payload: unknown, intMode: IntMode, requestId: string): RindleSqlError {
  const outer = isRecord(payload) ? payload : undefined;
  const body = outer && isRecord(outer.error) ? outer.error : outer;
  const fallbackMessage = `Rindle SQL request failed with HTTP ${response.status}`;
  const message = body && typeof body.message === "string"
    ? body.message
    : outer && typeof outer.error === "string"
      ? outer.error
      : fallbackMessage;
  const code = body && typeof body.code === "string" ? body.code : `HTTP_${response.status}`;
  const sqliteCodeRaw = body?.sqlite_code ?? body?.sqliteCode;
  const sqliteCode = typeof sqliteCodeRaw === "number" ? sqliteCodeRaw : undefined;
  const retryScopeRaw = body?.retry_scope ?? body?.retryScope;
  const inferredRetryScope = response.status >= 500 ? "request" : "never";
  const retryScope = retryScopeRaw === undefined ? inferredRetryScope : normalizeRetryScope(retryScopeRaw);
  const transactionStateRaw = body?.transaction_state ?? body?.transactionState;
  const statementIndexRaw = body?.statement_index ?? body?.statementIndex;
  const hasStatementIndex = statementIndexRaw !== undefined;
  let partialResults: StatementResult[] | undefined;
  const partialResultsRaw = body?.partial_results ?? body?.partialResults;
  const hasPartialResults = partialResultsRaw !== undefined;
  if (hasStatementIndex !== hasPartialResults) {
    throw protocolError("server error statement_index and partial_results must occur together");
  }
  let statementIndex: number | undefined;
  if (hasStatementIndex) {
    if (typeof statementIndexRaw !== "number" || !Number.isSafeInteger(statementIndexRaw) || statementIndexRaw < 0) {
      throw protocolError("server error statement_index must be a non-negative safe integer");
    }
    if (!Array.isArray(partialResultsRaw) || partialResultsRaw.length !== statementIndexRaw) {
      throw protocolError("server error partial_results length must equal statement_index");
    }
    statementIndex = statementIndexRaw;
    partialResults = partialResultsRaw.map((result) => decodeStatementResult(result, intMode));
  }
  return new RindleSqlError({
    code,
    message,
    sqliteCode,
    retryScope,
    transactionState: normalizeTransactionState(transactionStateRaw),
    status: response.status,
    // This is the logical request identity minted by this transport. A proxy's X-Request-Id is a
    // different, per-hop concept; a missing/malformed echo must not replace our stable identity.
    requestId,
    statementIndex,
    partialResults,
  });
}

function replaceRetryScope(error: RindleSqlError, retryScope: RetryScope): RindleSqlError {
  return new RindleSqlError({
    code: error.code,
    message: error.message,
    sqliteCode: error.sqliteCode,
    retryScope,
    transactionState: error.transactionState,
    status: error.status,
    requestId: error.requestId,
    statementIndex: error.statementIndex,
    partialResults: error.partialResults,
    cause: error,
  });
}

function isAbort(error: unknown, signal?: AbortSignal): boolean {
  return signal?.aborted === true || (error instanceof Error && error.name === "AbortError");
}

function throwAbort(signal?: AbortSignal): never {
  if (signal?.reason !== undefined) throw signal.reason;
  if (typeof DOMException !== "undefined") throw new DOMException("The operation was aborted", "AbortError");
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  throw error;
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) throwAbort(signal);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, ms);
    const abort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      try {
        throwAbort(signal);
      } catch (error) {
        reject(error);
      }
    };
    signal?.addEventListener("abort", abort, { once: true });
  });
}

function linkedSignal(one: AbortSignal | undefined, two: AbortSignal): { signal: AbortSignal; dispose: () => void } {
  if (one === undefined) return { signal: two, dispose: () => {} };
  const controller = new AbortController();
  const abortOne = (): void => controller.abort(one.reason);
  const abortTwo = (): void => controller.abort(two.reason);
  if (one.aborted) abortOne();
  else one.addEventListener("abort", abortOne, { once: true });
  if (two.aborted) abortTwo();
  else two.addEventListener("abort", abortTwo, { once: true });
  return {
    signal: controller.signal,
    dispose: () => {
      one.removeEventListener("abort", abortOne);
      two.removeEventListener("abort", abortTwo);
    },
  };
}

function retryAfterMs(response: Response, attempt: number): number {
  const value = response.headers.get("retry-after");
  if (value !== null) {
    const seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) return Math.min(seconds * 1_000, 1_000);
    const date = Date.parse(value);
    if (Number.isFinite(date)) return Math.max(0, Math.min(date - Date.now(), 1_000));
  }
  return Math.min(20 * 2 ** attempt, 160);
}

class Transport {
  readonly baseUrl: string;
  readonly authToken: string;
  readonly consistency: ReadConsistency;
  readonly intMode: IntMode;
  private readonly fetchImpl: Fetch;
  private readonly closeController = new AbortController();
  private closed = false;

  constructor(options: ClientOptions) {
    if (typeof options.url !== "string" || options.url.trim() === "") throw new TypeError("url must be a non-empty string");
    if (typeof options.authToken !== "string" || options.authToken === "") {
      throw new TypeError("authToken must be a non-empty string");
    }
    if (options.consistency !== undefined && !["session", "strong", "eventual"].includes(options.consistency)) {
      throw new TypeError(`unsupported consistency: ${String(options.consistency)}`);
    }
    if (options.intMode !== undefined && !["bigint", "number", "string"].includes(options.intMode)) {
      throw new TypeError(`unsupported intMode: ${String(options.intMode)}`);
    }
    const runtimeFetch = globalThis.fetch?.bind(globalThis) as Fetch | undefined;
    this.fetchImpl = options.fetch ?? runtimeFetch ?? (() => Promise.reject(new TypeError("global fetch is unavailable")));
    this.baseUrl = options.url.replace(/\/+$/, "");
    this.authToken = options.authToken;
    this.consistency = options.consistency ?? "session";
    this.intMode = options.intMode ?? "bigint";
  }

  isClosed(): boolean {
    return this.closed;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.closeController.abort(new RindleSqlError({ code: "CLIENT_CLOSED", message: "SQL client is closed" }));
  }

  async request(path: string, options: RequestOptions = {}): Promise<unknown> {
    if (this.closed) throw new RindleSqlError({ code: "CLIENT_CLOSED", message: "SQL client is closed" });
    const method = options.method ?? "POST";
    const authenticated = options.authenticated ?? true;
    const expectJson = options.expectJson ?? true;
    const encodedBody = options.body === undefined ? undefined : JSON.stringify(options.body);
    const requestId = newRequestId();
    const attempts = options.retrySafe === true ? REQUEST_ATTEMPTS : 1;
    let lastError: unknown;

    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const linked = linkedSignal(options.signal, this.closeController.signal);
      try {
        if (linked.signal.aborted) throwAbort(linked.signal);
        const headers: Record<string, string> = {
          Accept: "application/json",
          "Rindle-Request-Id": requestId,
        };
        if (encodedBody !== undefined) headers["Content-Type"] = "application/json";
        if (authenticated) headers.Authorization = `Bearer ${this.authToken}`;
        const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
          method,
          headers,
          body: encodedBody,
          signal: linked.signal,
        });
        let payload: unknown;
        const text = response.status === 204 ? "" : await response.text();
        if (text !== "") {
          try {
            payload = JSON.parse(text) as unknown;
          } catch (cause) {
            if (response.ok) throw protocolError("Rindle SQL returned invalid JSON", cause);
          }
        }
        if (response.ok) {
          if (expectJson && payload === undefined) throw protocolError("Rindle SQL returned an empty JSON response");
          return payload;
        }

        const error = httpError(response, payload, this.intMode, requestId);
        lastError = error;
        if (attempt + 1 >= attempts || error.retryScope !== "request") throw error;
        await delay(retryAfterMs(response, attempt), options.signal);
      } catch (error) {
        if (isAbort(error, linked.signal)) {
          if (this.closed) throw new RindleSqlError({ code: "CLIENT_CLOSED", message: "SQL client is closed", cause: error });
          throwAbort(options.signal ?? linked.signal);
        }
        if (isRindleSqlError(error)) {
          lastError = error;
          if (attempt + 1 >= attempts) {
            const exhaustedScope = options.exhaustedRequestRetryScope ?? "never";
            throw error.retryScope === "request" && exhaustedScope !== "request"
              ? replaceRetryScope(error, exhaustedScope)
              : error;
          }
          if (error.retryScope !== "request") throw error;
        } else {
          lastError = error;
          if (attempt + 1 >= attempts) {
            throw new RindleSqlError({
              code: "TRANSPORT_ERROR",
              message: error instanceof Error ? error.message : "Rindle SQL transport failed",
              retryScope: options.exhaustedRequestRetryScope ?? "never",
              cause: error,
            });
          }
        }
        await delay(Math.min(20 * 2 ** attempt, 160), options.signal);
      } finally {
        linked.dispose();
      }
    }
    throw lastError;
  }
}

function decodeStatementResult(value: unknown, intMode: IntMode): StatementResult {
  if (!isRecord(value)) throw protocolError("server statement result must be an object");
  if (!Array.isArray(value.columns)) throw protocolError("server statement result columns must be an array");
  const columns = value.columns.map((column) => {
    if (!isRecord(column) || typeof column.name !== "string" || (column.decltype !== null && typeof column.decltype !== "string")) {
      throw protocolError("server returned a malformed result column");
    }
    return { name: column.name, decltype: column.decltype };
  });
  if (!Array.isArray(value.rows)) throw protocolError("server statement result rows must be an array");
  const rows = value.rows.map((row) => {
    if (!Array.isArray(row)) throw protocolError("server returned a malformed result row");
    if (row.length !== columns.length) throw protocolError("server returned a result row with the wrong column count");
    return row.map((cell) => decodeSqlValue(cell, intMode));
  });
  return {
    columns,
    rows,
    rowsAffected: requiredNumber(value, "rows_affected"),
    lastInsertRowid: nullableString(value, "last_insert_rowid"),
    rowsRead: nullableNumber(value, "rows_read"),
    rowsWritten: nullableNumber(value, "rows_written"),
  };
}

function decodeRouting(value: unknown): RoutingMetadata {
  if (!isRecord(value)) throw protocolError("server routing metadata must be an object");
  if (value.served_by !== "master" && value.served_by !== "follower") {
    throw protocolError("server routing served_by must be master or follower");
  }
  if (typeof value.fence_fallback !== "boolean") throw protocolError("server routing fence_fallback must be boolean");
  return {
    servedBy: value.served_by,
    appliedLagMs: nullableNumber(value, "applied_lag_ms"),
    fenceFallback: value.fence_fallback,
  };
}

function decodeExecute(value: unknown, intMode: IntMode): ExecuteResult {
  if (!isRecord(value)) throw protocolError("server execute response must be an object");
  return {
    result: decodeStatementResult(value.result as WireStatementResult, intMode),
    commitCursor: nullableString(value, "commit_cursor"),
    routing: decodeRouting(value.routing as WireRoutingMetadata),
  };
}

function decodeBatch(value: unknown, intMode: IntMode): BatchResult {
  if (!isRecord(value) || !Array.isArray(value.results)) throw protocolError("server batch response must contain results");
  return {
    results: value.results.map((result) => decodeStatementResult(result, intMode)),
    commitCursor: nullableString(value, "commit_cursor"),
    routing: decodeRouting(value.routing),
  };
}

function decodeMutationReceipt(value: unknown): MutationReceipt {
  if (!isRecord(value) || typeof value.applied !== "boolean") {
    throw protocolError("server mutation response must contain applied and lmid");
  }
  const lmid = requiredNumber(value, "lmid");
  if (!Number.isSafeInteger(lmid) || lmid < 0) {
    throw protocolError("server mutation lmid must be a non-negative safe integer");
  }
  return {
    applied: value.applied,
    lmid,
    commitCursor: optionalString(value, "cursor"),
  };
}

function decodeMutationRows(value: unknown, intMode: IntMode): MutationRows {
  if (!isRecord(value) || !Array.isArray(value.cols) || !value.cols.every((column) => typeof column === "string")) {
    throw protocolError("server mutation read response must contain string cols");
  }
  if (!Array.isArray(value.rows)) throw protocolError("server mutation read response must contain rows");
  const columns = value.cols as string[];
  const rows = value.rows.map((row) => {
    if (!Array.isArray(row) || row.length !== columns.length) {
      throw protocolError("server mutation read row has the wrong column count");
    }
    // The mutation-session wire predates the public SQL tagged-value response and can still carry
    // a raw boolean. All other cells use the public decoder (including future tagged values).
    return row.map((cell) => (typeof cell === "boolean" ? cell : decodeSqlValue(cell, intMode)));
  });
  return { columns, rows };
}

function addCursor(body: JsonObject, options: { sessionCursor?: string | null } | undefined, state: CursorState): void {
  const hasOverride = options?.sessionCursor !== undefined;
  const cursor = hasOverride ? options.sessionCursor! : state.cursor;
  if (cursor !== null || hasOverride) body.session_cursor = cursor;
}

function acceptCursor(state: CursorState, cursor: string | null): void {
  if (cursor === null) return;

  // The current server emits `w:` plus sixteen lowercase hex digits. Responses from concurrent
  // requests can arrive out of order, so do not let an older acknowledgment move that
  // same-format fence backwards. Any other shape remains opaque and authoritative: a future
  // timeline-aware encoding must be allowed to re-anchor the client rather than being compared
  // with this process-local compatibility rule.
  const current = state.cursor;
  const parseCurrentCursor = (value: string): bigint | null => {
    if (!/^w:[0-9a-f]{16}$/.test(value)) return null;
    return BigInt(`0x${value.slice(2)}`);
  };
  if (current !== null) {
    const currentSequence = parseCurrentCursor(current);
    const nextSequence = parseCurrentCursor(cursor);
    if (currentSequence !== null && nextSequence !== null && nextSequence < currentSequence) return;
  }
  state.cursor = cursor;
}

class Transaction implements SqlTransaction {
  private open = true;
  private busy = false;
  private operationSequence = 0n;
  private pendingExecute: { operationId: string; requestIdentity: string | null } | null = null;
  private commitOperationId: string | null = null;
  private readonly transport: Transport;
  private readonly id: string;
  private readonly cursorState: CursorState;

  constructor(transport: Transport, id: string, cursorState: CursorState) {
    this.transport = transport;
    this.id = id;
    this.cursorState = cursorState;
  }

  private ensureUsable(): void {
    if (!this.open) {
      throw new RindleSqlError({
        code: "TRANSACTION_CLOSED",
        message: "transaction is closed",
        retryScope: "never",
        transactionState: "closed",
      });
    }
    if (this.busy) {
      throw new RindleSqlError({
        code: "TRANSACTION_BUSY",
        message: "transaction operations must be awaited serially",
        retryScope: "never",
        transactionState: "open",
      });
    }
  }

  private path(suffix: string): string {
    return `/v1/sql/transactions/${encodeURIComponent(this.id)}/${suffix}`;
  }

  /** Transaction-local canonical operation IDs: 1, 2, 3, ... */
  private nextOperationId(): string {
    this.operationSequence += 1n;
    return this.operationSequence.toString();
  }

  private async cancelAndRollback(operationId: string): Promise<void> {
    if (!this.open || this.transport.isClosed()) return;
    try {
      await this.transport.request(this.path("cancel"), {
        body: { operation_id: operationId },
        retrySafe: true,
        expectJson: false,
      });
    } catch {
      // The original abort remains the caller-visible error.
    }
    try {
      await this.transport.request(this.path("rollback"), {
        body: { operation_id: this.nextOperationId() },
        retrySafe: true,
        expectJson: false,
      });
    } catch {
      // Best effort cleanup after cancellation.
    }
    this.open = false;
  }

  private async executeStatements(statements: Statement[], options?: OperationOptions): Promise<StatementResult[]> {
    this.ensureUsable();
    if (statements.length === 0) throw new TypeError("transaction batch requires at least one statement");
    if (this.commitOperationId !== null) {
      throw new RindleSqlError({
        code: "TRANSACTION_COMMIT_PENDING",
        message: "the commit outcome is pending; retry commit() or roll back this transaction",
        retryScope: "never",
        transactionState: "unknown",
      });
    }
    const encodedStatements = statements.map((statement) => encodeStatement(statement));
    const requestIdentity = JSON.stringify(encodedStatements);
    const pendingExecute = this.pendingExecute;
    if (pendingExecute !== null && pendingExecute.requestIdentity !== null && pendingExecute.requestIdentity !== requestIdentity) {
      throw new RindleSqlError({
        code: "TRANSACTION_OPERATION_PENDING",
        message: "retry the same transaction statement request before starting a different operation",
        retryScope: "never",
        transactionState: "open",
      });
    }
    const operationId = this.pendingExecute?.operationId ?? this.nextOperationId();
    this.pendingExecute = { operationId, requestIdentity };
    this.busy = true;
    try {
      const payload = await this.transport.request(this.path("execute"), {
        body: { statements: encodedStatements, operation_id: operationId },
        signal: options?.signal,
        retrySafe: true,
        // Unlike an autocommit method, this transaction object can preserve the exact operation
        // identity across a later explicit retry after all internal transport attempts fail.
        exhaustedRequestRetryScope: "request",
      });
      if (!isRecord(payload) || !Array.isArray(payload.results)) {
        throw protocolError("server transaction execute response must contain results");
      }
      const results = payload.results.map((result) => decodeStatementResult(result, this.transport.intMode));
      if (results.length !== encodedStatements.length) {
        throw protocolError("server transaction execute result count must match the statement count");
      }
      this.pendingExecute = null;
      return results;
    } catch (error) {
      const requestRetryable =
        isRindleSqlError(error) && error.retryScope === "request" && error.transactionState !== "closed";
      const rejectedBeforeAdmission =
        isRindleSqlError(error) &&
        error.retryScope !== "request" &&
        (error.transactionState === "unknown" ||
          (error.transactionState === undefined &&
            (error.status === 400 || error.status === 413) &&
            error.code === `HTTP_${error.status}`));
      if (rejectedBeforeAdmission) this.pendingExecute = { operationId, requestIdentity: null };
      else if (!requestRetryable) this.pendingExecute = null;
      if (options?.signal?.aborted) {
        this.pendingExecute = null;
        await this.cancelAndRollback(operationId);
      }
      if (isRindleSqlError(error) && error.transactionState === "closed") this.open = false;
      throw error;
    } finally {
      this.busy = false;
    }
  }

  async execute(statement: Statement | string, options?: OperationOptions): Promise<StatementResult> {
    const results = await this.executeStatements([typeof statement === "string" ? { sql: statement } : statement], options);
    return results[0]!;
  }

  batch(statements: Statement[], options?: OperationOptions): Promise<StatementResult[]> {
    return this.executeStatements(statements, options);
  }

  async commit(options?: OperationOptions): Promise<{ commitCursor: string | null }> {
    this.ensureUsable();
    const pendingExecute = this.pendingExecute;
    if (pendingExecute !== null && pendingExecute.requestIdentity !== null) {
      throw new RindleSqlError({
        code: "TRANSACTION_OPERATION_PENDING",
        message: "retry the pending transaction statement request before committing, or roll back",
        retryScope: "never",
        transactionState: "open",
      });
    }
    // A commit may have reached the server even when its response did not reach the caller. Reuse
    // the same terminal operation identity across explicit retries so an open session still sees
    // its next expected sequence and a closed session can replay the retained terminal outcome.
    const reusableOperationId = pendingExecute?.operationId;
    this.pendingExecute = null;
    const operationId =
      this.commitOperationId ??
      (this.commitOperationId = reusableOperationId ?? this.nextOperationId());
    this.busy = true;
    try {
      const payload = await this.transport.request(this.path("commit"), {
        body: { operation_id: operationId },
        signal: options?.signal,
        retrySafe: true,
        // commitOperationId is retained across an explicit later commit(), so an exhausted
        // request error remains safely request-retryable by callers.
        exhaustedRequestRetryScope: "request",
      });
      if (!isRecord(payload)) throw protocolError("server transaction commit response must be an object");
      const commitCursor = nullableString(payload, "commit_cursor");
      acceptCursor(this.cursorState, commitCursor);
      this.open = false;
      return { commitCursor };
    } catch (error) {
      if (options?.signal?.aborted) await this.cancelAndRollback(operationId);
      if (
        isRindleSqlError(error) &&
        (error.transactionState === "closed" ||
          error.code === "TRANSACTION_CONFLICT" ||
          error.code === "TRANSACTION_EXPIRED" ||
          error.code === "TRANSACTION_CLOSED")
      ) {
        this.open = false;
      }
      throw error;
    } finally {
      this.busy = false;
    }
  }

  async rollback(options?: OperationOptions): Promise<void> {
    if (!this.open) return;
    this.ensureUsable();
    // If a commit transport failed with an unknown outcome, its terminal sequence is still the
    // server's next expected operation when the request never arrived. Reusing it lets rollback
    // close that live transaction; if the commit did arrive, rollback of the now-missing handle is
    // already an idempotent success.
    const operationId = this.commitOperationId ?? this.nextOperationId();
    this.busy = true;
    try {
      await this.transport.request(this.path("rollback"), {
        body: { operation_id: operationId },
        signal: options?.signal,
        retrySafe: true,
        expectJson: false,
        exhaustedRequestRetryScope: "request",
      });
      this.open = false;
    } catch (error) {
      if (options?.signal?.aborted) await this.cancelAndRollback(operationId);
      if (isRindleSqlError(error) && error.transactionState === "closed") this.open = false;
      throw error;
    } finally {
      this.busy = false;
    }
  }
}

class MutationTransaction implements SqlMutationTransaction {
  /** `closed` means the SERVER declared the transaction gone (410, or an explicit closed state), so
   *  no rollback is owed. `unknown` means a request failed without an answer (5xx / transport): the
   *  writer may still be held, so rollback MUST still go out even though no further work may. Only
   *  `closed` suppresses the rollback — collapsing these two is what leaked the server-side writer
   *  until its deadline. */
  private state: "open" | "unknown" | "closed" = "open";
  private busy = false;
  private readonly transport: Transport;
  private readonly id: string;
  private readonly cursorState: CursorState;

  constructor(transport: Transport, id: string, cursorState: CursorState) {
    this.transport = transport;
    this.id = id;
    this.cursorState = cursorState;
  }

  private ensureUsable(): void {
    if (this.state !== "open") {
      throw new RindleSqlError({
        code: "TRANSACTION_CLOSED",
        message:
          this.state === "closed"
            ? "mutation transaction is closed"
            : "mutation transaction is in an unknown state after a failed request",
        retryScope: "never",
        transactionState: this.state === "closed" ? "closed" : "unknown",
      });
    }
    if (this.busy) {
      throw new RindleSqlError({
        code: "TRANSACTION_BUSY",
        message: "mutation transaction operations must be awaited serially",
        retryScope: "never",
        transactionState: "open",
      });
    }
  }

  private path(suffix: string): string {
    return `/v1/sql/mutations/transactions/${encodeURIComponent(this.id)}/${suffix}`;
  }

  /** Classify a failed request. Only a server-DECLARED closure retires the rollback obligation; an
   *  unanswered request leaves the writer possibly held, which is `unknown`, not `closed`. */
  private noteTerminalError(error: unknown): void {
    if (!isRindleSqlError(error)) return;
    if (error.status === 410 || error.transactionState === "closed") {
      this.state = "closed";
      return;
    }
    if (error.status !== undefined && error.status >= 500) this.state = "unknown";
  }

  async execute(statement: Statement | string, options?: OperationOptions): Promise<void> {
    await this.batch([typeof statement === "string" ? { sql: statement } : statement], options);
  }

  async batch(statements: Statement[], options?: OperationOptions): Promise<void> {
    this.ensureUsable();
    if (statements.length === 0) throw new TypeError("mutation transaction batch requires at least one statement");
    this.busy = true;
    try {
      await this.transport.request(this.path("execute"), {
        body: { statements: statements.map((statement) => encodeStatement(statement)) },
        signal: options?.signal,
        retrySafe: false,
      });
    } catch (error) {
      this.noteTerminalError(error);
      throw error;
    } finally {
      this.busy = false;
    }
  }

  async query(statement: Statement | string, options?: OperationOptions): Promise<MutationRows> {
    this.ensureUsable();
    this.busy = true;
    try {
      return decodeMutationRows(
        await this.transport.request(this.path("query"), {
          body: { query: encodeStatement(statement) },
          signal: options?.signal,
          retrySafe: false,
        }),
        this.transport.intMode,
      );
    } catch (error) {
      this.noteTerminalError(error);
      throw error;
    } finally {
      this.busy = false;
    }
  }

  async commit(options?: OperationOptions): Promise<MutationReceipt> {
    this.ensureUsable();
    this.busy = true;
    try {
      const receipt = decodeMutationReceipt(
        await this.transport.request(this.path("commit"), {
          body: {},
          signal: options?.signal,
          retrySafe: false,
        }),
      );
      acceptCursor(this.cursorState, receipt.commitCursor);
      this.state = "closed";
      return receipt;
    } catch (error) {
      this.noteTerminalError(error);
      throw error;
    } finally {
      this.busy = false;
    }
  }

  async rollback(options?: OperationOptions): Promise<void> {
    // Only a server-declared closure retires the obligation. An `unknown` transaction still owes a
    // rollback — that is the whole point of the state split — so it deliberately does NOT go through
    // `ensureUsable`, which refuses everything except `open`.
    if (this.state === "closed") return;
    if (this.busy) {
      throw new RindleSqlError({
        code: "TRANSACTION_BUSY",
        message: "mutation transaction operations must be awaited serially",
        retryScope: "never",
        transactionState: "open",
      });
    }
    this.busy = true;
    try {
      await this.transport.request(this.path("rollback"), {
        body: {},
        signal: options?.signal,
        retrySafe: true,
        expectJson: false,
        exhaustedRequestRetryScope: "request",
      });
      this.state = "closed";
    } catch (error) {
      // A rollback of an already-gone transaction is the outcome the caller wanted: the server
      // answering 410 (or declaring it closed) IS the success case, not a failure to report.
      if (isRindleSqlError(error) && (error.status === 410 || error.transactionState === "closed")) {
        this.state = "closed";
        return;
      }
      throw error;
    } finally {
      this.busy = false;
    }
  }
}

class Session implements SqlSession {
  protected readonly transport: Transport;
  protected readonly cursorState: CursorState;

  constructor(transport: Transport, cursorState: CursorState) {
    this.transport = transport;
    this.cursorState = cursorState;
  }

  async execute(statement: Statement | string, options?: ExecuteOptions): Promise<ExecuteResult> {
    const body: JsonObject = {
      statement: encodeStatement(statement),
      default_consistency: this.transport.consistency,
      idempotency_key: newIdempotencyKey(),
    };
    if (options?.consistency !== undefined) body.consistency = options.consistency;
    addCursor(body, options, this.cursorState);
    const result = decodeExecute(
      await this.transport.request("/v1/sql/execute", { body, signal: options?.signal, retrySafe: true }),
      this.transport.intMode,
    );
    acceptCursor(this.cursorState, result.commitCursor);
    return result;
  }

  async batch(statements: Statement[], options?: BatchOptions): Promise<BatchResult> {
    if (statements.length === 0) throw new TypeError("batch requires at least one statement");
    const body: JsonObject = {
      statements: statements.map((statement) => encodeStatement(statement)),
      default_consistency: this.transport.consistency,
      idempotency_key: newIdempotencyKey(),
    };
    if (options?.consistency !== undefined) body.consistency = options.consistency;
    addCursor(body, options, this.cursorState);
    const result = decodeBatch(
      await this.transport.request("/v1/sql/batch", { body, signal: options?.signal, retrySafe: true }),
      this.transport.intMode,
    );
    if (result.results.length !== statements.length) {
      throw protocolError("server batch result count must match the statement count");
    }
    acceptCursor(this.cursorState, result.commitCursor);
    return result;
  }

  async begin(options?: TransactionOptions): Promise<SqlTransaction> {
    const readOnly = options?.readOnly ?? false;
    const body: JsonObject = {
      read_only: readOnly,
      isolation: options?.isolation ?? "serializable",
    };
    if (readOnly) body.default_consistency = this.transport.consistency;
    if (options?.consistency !== undefined) body.consistency = options.consistency;
    const hasCursorOverride = options?.sessionCursor !== undefined;
    if (readOnly || hasCursorOverride) addCursor(body, options, this.cursorState);
    const payload = await this.transport.request("/v1/sql/transactions", {
      body,
      signal: options?.signal,
      retrySafe: false,
    });
    if (!isRecord(payload)) throw protocolError("server transaction begin response must be an object");
    return new Transaction(this.transport, requiredString(payload, "transaction_id"), this.cursorState);
  }

  async withTransaction<T>(fn: (tx: SqlTransaction) => Promise<T>, options?: TransactionOptions): Promise<T> {
    if (typeof fn !== "function") throw new TypeError("withTransaction requires a callback");
    const tx = await this.begin(options);
    let value: T;
    try {
      value = await fn(tx);
    } catch (error) {
      // The callback failed, so nothing was ever submitted for commit: rolling back is both
      // truthful and the fastest way to release the server's writer connection.
      try {
        await tx.rollback();
      } catch {
        // Preserve the callback error.
      }
      throw error;
    }
    await this.commitToKnownOutcome(tx, options);
    return value;
  }

  /**
   * Drive a transaction's commit to a KNOWN outcome.
   *
   * A commit whose response is lost is outcome-*unknown*, not failed — the server may already hold
   * a durable terminal record for it. `commit()` retains its operation id precisely so repeating
   * the same call reads that record back, and reports `retryScope: "request"` to say the call is
   * safe to repeat. Re-driving it here is the only way an application using this ergonomic surface
   * can reach that machinery.
   *
   * If the outcome is still unknown once the attempts are spent, the error is rethrown WITHOUT a
   * rollback. Rolling back would report a possibly-durable commit as aborted and leave the session
   * fence unadvanced, so the caller's natural response — re-running the mutation — would
   * double-apply it. A genuinely-open transaction is instead reclaimed by the server's session
   * lease, and the retained operation id keeps `tx.commit()` resolvable until then.
   */
  private async commitToKnownOutcome(tx: SqlTransaction, options?: TransactionOptions): Promise<void> {
    for (let attempt = 1; ; attempt += 1) {
      try {
        await tx.commit({ signal: options?.signal });
        return;
      } catch (error) {
        const outcomeUnknown = isRindleSqlError(error) && error.retryScope === "request";
        if (!outcomeUnknown) {
          // A definite negative (conflict, closed, rejected before admission): nothing committed,
          // so release the transaction before surfacing it.
          try {
            await tx.rollback();
          } catch {
            // Preserve the commit error.
          }
          throw error;
        }
        if (attempt >= COMMIT_RESOLUTION_ATTEMPTS || options?.signal?.aborted) throw error;
        await delay(COMMIT_RESOLUTION_BASE_DELAY_MS * 2 ** (attempt - 1), options?.signal);
      }
    }
  }

  async withTransactionRetry<T>(fn: (tx: SqlTransaction) => Promise<T>, options: RetryOptions = {}): Promise<T> {
    const maxAttempts = options.maxAttempts ?? 5;
    const baseDelayMs = options.baseDelayMs ?? 10;
    const maxDelayMs = options.maxDelayMs ?? 250;
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) throw new TypeError("maxAttempts must be a positive integer");
    if (!Number.isFinite(baseDelayMs) || baseDelayMs < 0) throw new TypeError("baseDelayMs must be non-negative");
    if (!Number.isFinite(maxDelayMs) || maxDelayMs < baseDelayMs) {
      throw new TypeError("maxDelayMs must be at least baseDelayMs");
    }
    const transactionOptions: TransactionOptions = {};
    if (options.readOnly !== undefined) transactionOptions.readOnly = options.readOnly;
    if (options.isolation !== undefined) transactionOptions.isolation = options.isolation;
    if (options.consistency !== undefined) transactionOptions.consistency = options.consistency;
    if (options.sessionCursor !== undefined) transactionOptions.sessionCursor = options.sessionCursor;
    if (options.signal !== undefined) transactionOptions.signal = options.signal;

    for (let attempt = 1; ; attempt += 1) {
      try {
        return await this.withTransaction(fn, transactionOptions);
      } catch (error) {
        const conflict = isRindleSqlError(error) && error.code === "TRANSACTION_CONFLICT";
        if (!conflict || attempt >= maxAttempts) throw error;
        const cap = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
        await delay(Math.random() * cap, options.signal);
      }
    }
  }

  async executeMutation(input: ExecuteMutationInput, options?: OperationOptions): Promise<MutationReceipt> {
    assertMutationIdentity(input);
    if (!Array.isArray(input.statements)) throw new TypeError("mutation statements must be an array");
    const body: JsonObject = {
      client_id: input.clientId,
      mid: input.mid,
      statements: input.statements.map((statement) => encodeStatement(statement)),
    };
    const receipt = decodeMutationReceipt(
      await this.transport.request("/v1/sql/mutations/execute", {
        body,
        signal: options?.signal,
        // The server's mid dedup makes a response-loss retry safe and returns the stored lmid.
        retrySafe: true,
        exhaustedRequestRetryScope: "request",
      }),
    );
    acceptCursor(this.cursorState, receipt.commitCursor);
    return receipt;
  }

  async beginMutation(input: BeginMutationInput, options?: OperationOptions): Promise<BeginMutationResult> {
    assertMutationIdentity(input);
    if (input.statements !== undefined && !Array.isArray(input.statements)) {
      throw new TypeError("mutation statements must be an array when present");
    }
    const body: JsonObject = {
      client_id: input.clientId,
      mid: input.mid,
      statements: (input.statements ?? []).map((statement) => encodeStatement(statement)),
    };
    if (input.query !== undefined) body.query = encodeStatement(input.query);
    const payload = await this.transport.request("/v1/sql/mutations/transactions", {
      body,
      signal: options?.signal,
      // A lost begin response may have left a live writer transaction, so begin is never retried.
      retrySafe: false,
    });
    if (!isRecord(payload)) throw protocolError("server mutation begin response must be an object");
    if (payload.absorbed === true) {
      return { absorbed: true, receipt: decodeMutationReceipt(payload) };
    }
    const transactionId = requiredString(payload, "sessionId");
    const result: BeginMutationResult = {
      absorbed: false,
      transaction: new MutationTransaction(this.transport, transactionId, this.cursorState),
    };
    if (payload.read !== undefined) result.read = decodeMutationRows(payload.read, this.transport.intMode);
    return result;
  }

  async rejectMutation(input: RejectMutationInput, options?: OperationOptions): Promise<MutationReceipt> {
    assertMutationIdentity(input);
    if (input.reason !== undefined && typeof input.reason !== "string") {
      throw new TypeError("mutation rejection reason must be a string when present");
    }
    const body: JsonObject = { client_id: input.clientId, mid: input.mid };
    if (input.reason !== undefined) body.reason = input.reason;
    const receipt = decodeMutationReceipt(
      await this.transport.request("/v1/sql/mutations/reject", {
        body,
        signal: options?.signal,
        retrySafe: true,
        exhaustedRequestRetryScope: "request",
      }),
    );
    acceptCursor(this.cursorState, receipt.commitCursor);
    return receipt;
  }

  async executeDdl(sql: string, options?: OperationOptions): Promise<ExecuteResult> {
    if (typeof sql !== "string" || sql.length === 0) throw new TypeError("sql must be a non-empty string");
    const body: JsonObject = {
      statement: encodeStatement(sql),
      idempotency_key: newIdempotencyKey(),
    };
    const result = decodeExecute(
      await this.transport.request("/v1/sql/execute", { body, signal: options?.signal, retrySafe: true }),
      this.transport.intMode,
    );
    acceptCursor(this.cursorState, result.commitCursor);
    return result;
  }

  async migrate(input: MigrationInput, options?: OperationOptions): Promise<MigrationResult> {
    if (!isRecord(input) || typeof input.id !== "string" || input.id.length === 0) {
      throw new TypeError("migration id must be a non-empty string");
    }
    if (typeof input.checksum !== "string" || input.checksum.length === 0) {
      throw new TypeError("migration checksum must be a non-empty string");
    }
    if (!Array.isArray(input.statements) || input.statements.length === 0 || !input.statements.every((sql) => typeof sql === "string")) {
      throw new TypeError("migration statements must be a non-empty string array");
    }
    const payload = await this.transport.request("/v1/sql/migrate", {
      body: { id: input.id, checksum: input.checksum, statements: input.statements },
      signal: options?.signal,
      retrySafe: true,
      exhaustedRequestRetryScope: "request",
    });
    if (!isRecord(payload) || typeof payload.applied !== "boolean") {
      throw protocolError("server migration response must contain applied and commit_cursor");
    }
    const commitCursor = requiredString(payload, "commit_cursor");
    acceptCursor(this.cursorState, commitCursor);
    return { applied: payload.applied, commitCursor };
  }

  async executeMultiple(sql: string, options?: OperationOptions): Promise<void> {
    if (typeof sql !== "string" || sql.length === 0) throw new TypeError("sql must be a non-empty string");
    const payload = await this.transport.request("/v1/sql/execute-multiple", {
      body: { sql, idempotency_key: newIdempotencyKey() },
      signal: options?.signal,
      retrySafe: true,
    });
    if (!isRecord(payload)) throw protocolError("server execute-multiple response must be an object");
    acceptCursor(this.cursorState, nullableString(payload, "commit_cursor"));
  }

  session(cursor: string | null = null): SqlSession {
    if (cursor !== null && typeof cursor !== "string") throw new TypeError("session cursor must be a string or null");
    return new Session(this.transport, { cursor });
  }

  getSessionCursor(): string | null {
    return this.cursorState.cursor;
  }

  resetSessionCursor(): void {
    this.cursorState.cursor = null;
  }

  async ping(options?: OperationOptions): Promise<void> {
    await this.transport.request("/version", {
      method: "GET",
      signal: options?.signal,
      authenticated: false,
      retrySafe: true,
      expectJson: false,
      exhaustedRequestRetryScope: "request",
    });
  }
}

class Client extends Session implements SqlClient {
  close(): void {
    this.transport.close();
  }
}

export function createSqlClient(options: ClientOptions): SqlClient {
  const transport = new Transport(options);
  return new Client(transport, { cursor: options.sessionCursor ?? null });
}
