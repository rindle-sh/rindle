-- 0001_init — the issue tracker's four normalized tables + their indices.
--
-- This is the schema the daemon used to receive as `createSql` on EVERY boot. It now lives here as
-- a migration applied ON DEMAND by `pnpm migrate` (via `rindle migrate apply`), so the daemon no
-- longer mutates schema at startup — it boots, then introspects whatever the file holds.
--
-- Column order matters: the IVM engine reads it back with PRAGMA table_info, so it must match
-- shared/app-def.ts. The indices keep the `sub` joins fast in BOTH directions — the forward fetch
-- (a parent's children) AND the reverse lookup (a changed child re-finding its parents).
--
-- DDL only (additive or destructive); `IF NOT EXISTS` keeps a re-run safe.
--
-- Every column is NOT NULL: this app never stores null (an unassigned issue uses the "unassigned"
-- sentinel string, not NULL), and declaring it makes `rindle schema gen` emit non-nullable types —
-- `string`, not `string | null` (design 206). A genuinely optional column would omit NOT NULL to
-- generate `.nullable()`; don't relax these without meaning it.

CREATE TABLE IF NOT EXISTS user (id TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id));

CREATE TABLE IF NOT EXISTS issue (id TEXT NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL, priority TEXT NOT NULL, ownerId TEXT NOT NULL, createdAt REAL NOT NULL, updatedAt REAL NOT NULL, PRIMARY KEY (id));
-- The paginated window: ORDER BY createdAt DESC, id — seek straight to the cursor.
CREATE INDEX IF NOT EXISTS issue_created ON issue (createdAt DESC, id);
-- The owner join's reverse direction: find an owner's issues / a changed user's issues.
CREATE INDEX IF NOT EXISTS issue_owner ON issue (ownerId);

CREATE TABLE IF NOT EXISTS tag (id TEXT NOT NULL, issueId TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id));
-- Forward: an issue's tags, alphabetized, via the covering (issueId, name) index.
CREATE INDEX IF NOT EXISTS tag_issue ON tag (issueId, name);
-- Reverse: every issue carrying a tag name (the search "tag" axis).
CREATE INDEX IF NOT EXISTS tag_name ON tag (name);

CREATE TABLE IF NOT EXISTS comment (id TEXT NOT NULL, issueId TEXT NOT NULL, authorId TEXT NOT NULL, body TEXT NOT NULL, createdAt REAL NOT NULL, PRIMARY KEY (id));
-- Forward: an issue's comments in chronological order (description first).
CREATE INDEX IF NOT EXISTS comment_issue ON comment (issueId, createdAt);
-- Reverse: a user's comments / the author join re-finding parents.
CREATE INDEX IF NOT EXISTS comment_author ON comment (authorId);
