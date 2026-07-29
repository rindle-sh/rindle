-- 0001_init — the app's two normalized tables.
--
-- This SQL is the SINGLE SOURCE OF TRUTH for the schema. It is applied to the daemon on demand through
-- the `/migrate` endpoint (run by `rindle migrate apply`, which `pnpm dev` drives for you), and the
-- @rindle/client TypeScript schema is GENERATED from the daemon's introspected result into
-- shared/schema.gen.ts — so the TS can't drift from the DDL.
--
-- To change the schema after this file has been applied, add a NEW ordered file in migrations/.
-- `pnpm dev` applies it and regenerates shared/schema.gen.ts automatically; `pnpm migrate` does the
-- same for a one-shot run.
--
-- Column kinds map to the client schema like so: TEXT → string(), INTEGER/REAL → number(), a column
-- declared BOOLEAN/BOOL → boolean(), JSON → json(). Column ORDER matters — the IVM engine reads it
-- back with PRAGMA table_info. `IF NOT EXISTS` keeps re-runs safe.

CREATE TABLE IF NOT EXISTS room (id TEXT NOT NULL, name TEXT NOT NULL, createdAt REAL NOT NULL, PRIMARY KEY (id));
-- The room list orders newest-first.
CREATE INDEX IF NOT EXISTS room_created ON room (createdAt DESC, id);

CREATE TABLE IF NOT EXISTS message (id TEXT NOT NULL, roomId TEXT NOT NULL, author TEXT NOT NULL, body TEXT NOT NULL, createdAt REAL NOT NULL, PRIMARY KEY (id));
-- Forward (a room's messages, oldest-first) AND the reverse the live count uses when a message changes.
CREATE INDEX IF NOT EXISTS message_room ON message (roomId, createdAt, id);
