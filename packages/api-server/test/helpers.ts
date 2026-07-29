// Shared test fakes for the api-server backend/scoped-mutator suites. NOT a `*.test.ts` file, so
// `node --test test/*.test.ts` never runs it directly — it is only imported. Kept here so a change to
// `PostgresPlugger` / `RindleDaemonClient` updates ONE fake, not a copy per test file.

import type { RindleDaemonClient } from "@rindle/daemon-client";

import type { PgQuery, PostgresPlugger } from "../src/index.ts";

/** Records every statement of every COMMITTED transaction, in order. Pass an `events` array to also
 *  append `tx-open`/`tx-commit` markers around each txn (so a test can prove outside-tx code ran
 *  before `BEGIN` and after-commit code ran after `COMMIT`); omit it and the plugger just records
 *  `txns`. Set `failWith` to make every `exec` throw (an infra failure). */
export class FakePlugger implements PostgresPlugger {
  txns: Array<Array<{ sql: string; params: unknown[] }>> = [];
  failWith?: Error;
  private readonly events?: string[];
  constructor(events?: string[]) {
    this.events = events;
  }

  async transaction<T>(fn: (q: PgQuery) => Promise<T>): Promise<T> {
    this.events?.push("tx-open");
    const stmts: Array<{ sql: string; params: unknown[] }> = [];
    const q: PgQuery = {
      exec: async (sql, params = []) => {
        if (this.failWith) throw this.failWith;
        stmts.push({ sql, params });
      },
      query: async () => [],
    };
    const out = await fn(q);
    this.txns.push(stmts);
    this.events?.push("tx-commit");
    return out;
  }
}

/** A daemon whose WRITE endpoints throw — proving the postgres backend never touches them. */
export function readOnlyDaemon(): RindleDaemonClient {
  const boom = (name: string) => () => {
    throw new Error(`daemon.${name} must not be called under postgresBackend`);
  };
  return {
    materialize: boom("materialize"),
    dematerialize: boom("dematerialize"),
    executeSqlTxn: boom("executeSqlTxn"),
    executeSqlRead: boom("executeSqlRead"),
    applyRowChangeTxn: boom("applyRowChangeTxn"),
    rejectMutation: boom("rejectMutation"),
    query: boom("query"),
    migrate: boom("migrate"),
  } as unknown as RindleDaemonClient;
}

/** True for the §2.3 lmid upsert statement (keyed on `_rindle_client_mutations` + `GREATEST`). */
export const isLmidUpsert = (sql: string): boolean =>
  sql.includes("_rindle_client_mutations") && sql.includes("GREATEST");

/** Flatten committed txns to their SQL strings, in order. */
export const flat = (txns: Array<Array<{ sql: string }>>): string[] => txns.flat().map((s) => s.sql);
