// The stream suites' shared durable half: a REAL SQLite (`node:sqlite`) behind the `ServerSql`
// surface, with fault injection. Used by both the example-based suite (`streams.test.ts`) and the
// property-based one (`streams.property.test.ts`) — the interesting claims are claims about SQL, so
// both run against the genuine article rather than a hand mock.

import { DatabaseSync } from "node:sqlite";

import { sqliteDialect, streamChunkTableDdl, assembleDurableText } from "../src/index.ts";
import type { MutationBackend, ServerSql, StreamTables } from "../src/index.ts";

export const TABLES: StreamTables = {
  message: "message",
  chunks: "message_chunk",
  columns: { cancel: "cancelRequested", error: "error" },
};

/** The mapping with the opt-in `host` column: the open write becomes a true CAS (§5.1). */
export const HOST_TABLES: StreamTables = {
  message: "message",
  chunks: "message_chunk",
  columns: { cancel: "cancelRequested", error: "error", host: "host" },
};

/** The app's own message table: `chatId`/`role` are the app's business, and the plane touches only
 *  the columns the mapping names. */
export const MESSAGE_DDL = `CREATE TABLE message (
  id TEXT PRIMARY KEY,
  chatId TEXT NOT NULL,
  role TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  seq INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'pending',
  cancelRequested INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  host TEXT
)`;

export class Store {
  readonly db = new DatabaseSync(":memory:");
  /** Statements the plane executed, in order (for asserting round-trip shape). */
  batches: string[][] = [];
  /** Consumed one per `batch`: an Error here fails THAT attempt before the statements run. */
  failures: Array<Error | undefined> = [];
  /** Fail the next `batch` AFTER its statements committed — a lost ack, the nastiest replay case:
   *  the plane retries a checkpoint the store already applied. */
  loseNextAck = false;
  /** Runs once, immediately BEFORE the next batch's statements — a rival landing in the exact
   *  window between the open probe and the open write. */
  nextBatchHook?: () => void;
  /** Fail this many upcoming `query` calls (the read-backs and probes), one each. */
  failNextQueries = 0;
  /** Artificial latency per batch, to exercise coalescing. Under mocked timers this is VIRTUAL
   *  latency: the batch stays in flight until the clock is ticked past it. */
  latencyMs = 0;

  constructor() {
    this.db.exec(MESSAGE_DDL);
    for (const ddl of streamChunkTableDdl(TABLES, sqliteDialect)) this.db.exec(ddl);
  }

  /** The app writes the message row itself — with its own columns — before any stream exists. */
  seedMessage(id: string, chatId = "c1"): void {
    this.db.prepare("INSERT INTO message (id, chatId, role) VALUES (?, ?, ?)").run(id, chatId, "assistant");
  }

  // `node:sqlite` hands back null-prototype rows; re-shape them so assertions compare plain objects.
  row(id: string): { body: string; seq: number; status: string; error: string | null } {
    const r = this.db.prepare("SELECT body, seq, status, error FROM message WHERE id = ?").get(id) as never as {
      body: string;
      seq: number;
      status: string;
      error: string | null;
    };
    return { body: r.body, seq: r.seq, status: r.status, error: r.error };
  }

  chunks(id: string): Array<{ seq: number; text: string }> {
    const rows = this.db
      .prepare('SELECT seq, text FROM message_chunk WHERE "streamId" = ? ORDER BY seq')
      .all(id) as never as Array<{ seq: number; text: string }>;
    return rows.map((r) => ({ seq: r.seq, text: r.text }));
  }

  /** What a client's IVM view assembles: the compacted body plus the chunks not yet folded in. */
  durableText(id: string): string {
    return assembleDurableText(this.row(id), this.chunks(id));
  }

  setCancel(id: string): void {
    this.db.prepare("UPDATE message SET cancelRequested = 1 WHERE id = ?").run(id);
  }

  /** The api-server's outside-transaction SQL surface, as every real backend provides it: `batch` is
   *  ONE transaction, `query` reads its own writes. */
  get sql(): ServerSql {
    const self = this;
    return {
      async execute(sql, params = []) {
        await self.sql.batch([{ sql, params: [...params] }]);
      },
      async batch(statements) {
        if (statements.length === 0) return;
        if (self.latencyMs) await new Promise((r) => setTimeout(r, self.latencyMs));
        self.batches.push(statements.map((s) => s.sql));
        const fail = self.failures.shift();
        if (fail) throw fail;
        const hook = self.nextBatchHook;
        self.nextBatchHook = undefined;
        hook?.();
        self.db.exec("BEGIN");
        try {
          for (const s of statements) self.db.prepare(s.sql).run(...((s.params ?? []) as never[]));
          self.db.exec("COMMIT");
        } catch (e) {
          self.db.exec("ROLLBACK");
          throw e;
        }
        if (self.loseNextAck) {
          self.loseNextAck = false;
          throw new Error("lost the ack");
        }
      },
      async query<Row = Record<string, unknown>>(sql: string, params: readonly unknown[] = []): Promise<Row[]> {
        if (self.failNextQueries > 0) {
          self.failNextQueries--;
          throw new Error("injected query failure");
        }
        return self.db.prepare(sql).all(...(params as never[])) as Row[];
      },
    };
  }

  get backend(): MutationBackend {
    return {
      dialect: sqliteDialect,
      outsideSql: this.sql,
      async runMutation() {
        throw new Error("no mutations in these tests — checkpoints are SYSTEM writes");
      },
      async reject() {
        return {};
      },
    };
  }
}
