import type { RetryScope, StatementResult, TransactionState } from "./types.ts";

export interface RindleSqlErrorOptions {
  code: string;
  message: string;
  sqliteCode?: number;
  retryScope?: RetryScope;
  transactionState?: TransactionState;
  status?: number;
  requestId?: string;
  statementIndex?: number;
  partialResults?: StatementResult[];
  cause?: unknown;
}

/** Stable error surfaced for both server envelopes and client-side contract refusals. */
export class RindleSqlError extends Error {
  readonly code: string;
  readonly sqliteCode: number | undefined;
  readonly retryScope: RetryScope;
  readonly transactionState: TransactionState | undefined;
  readonly status: number | undefined;
  readonly requestId: string | undefined;
  readonly statementIndex: number | undefined;
  readonly partialResults: StatementResult[] | undefined;

  constructor(options: RindleSqlErrorOptions) {
    super(options.message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "RindleSqlError";
    this.code = options.code;
    this.sqliteCode = options.sqliteCode;
    this.retryScope = options.retryScope ?? "never";
    this.transactionState = options.transactionState;
    this.status = options.status;
    this.requestId = options.requestId;
    this.statementIndex = options.statementIndex;
    this.partialResults = options.partialResults;
  }
}

export function isRindleSqlError(error: unknown): error is RindleSqlError {
  return error instanceof RindleSqlError;
}

export function valueUnsupported(message: string): RindleSqlError {
  return new RindleSqlError({ code: "VALUE_UNSUPPORTED", message, retryScope: "never" });
}

export function protocolError(message: string, cause?: unknown): RindleSqlError {
  return new RindleSqlError({ code: "PROTOCOL_ERROR", message, retryScope: "never", cause });
}
