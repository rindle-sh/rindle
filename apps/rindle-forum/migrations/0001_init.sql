-- 0001_init — the forum's five normalized tables + their indices.
--
-- This is the schema the daemon used to receive as `createSql` on EVERY boot (the old
-- shared/daemon-tables.ts, mirrored by hand into the infra table config). It now lives here as a
-- migration applied ON DEMAND through the `/migrate` endpoint (via `rindle migrate apply`). The ONE
-- topology (design 214) applies it in exactly one place — the `rindle-replicator` WRITE-MASTER (the
-- local pair's :7611 in dev; the Headwaters app hostname in production). The master mints one
-- co-transactional `ddl` change-log entry and fans it out; each follower relays the DDL and creates
-- the tables in lineage (MIGRATIONS-VIA-CHANGELOG-DESIGN.md §4–6). The master auto-registers the new
-- tables for capture (rederive_schemas), so no static table list is needed anywhere.
--
-- Column order matters: the IVM engine reads it back with PRAGMA table_info, so it must match
-- shared/app-def.ts. The indices keep the `sub` joins fast in BOTH directions — the forward fetch
-- (a parent's children, e.g. a thread's posts) AND the reverse lookup (a changed child re-finding
-- its parents).
--
-- DDL only (additive or destructive); `IF NOT EXISTS` keeps a re-run safe,
-- and lets the follower open a restored file that already holds the tables.

CREATE TABLE IF NOT EXISTS user (id TEXT NOT NULL, displayName TEXT NOT NULL, avatarUrl TEXT NOT NULL, PRIMARY KEY (id));

CREATE TABLE IF NOT EXISTS category (id TEXT NOT NULL, slug TEXT NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL, position REAL NOT NULL, PRIMARY KEY (id));
-- The category list orders by position.
CREATE INDEX IF NOT EXISTS category_position ON category (position, id);
-- Reverse of the slug lookup the /c/:slug route resolves.
CREATE INDEX IF NOT EXISTS category_slug ON category (slug);

CREATE TABLE IF NOT EXISTS thread (id TEXT NOT NULL, categoryId TEXT NOT NULL, authorId TEXT NOT NULL, title TEXT NOT NULL, createdAt REAL NOT NULL, lastPostAt REAL NOT NULL, locked REAL NOT NULL, pinned REAL NOT NULL, PRIMARY KEY (id));
-- A category's threads, most-recently-active first (pinned ones float via the ORDER BY).
CREATE INDEX IF NOT EXISTS thread_category_activity ON thread (categoryId, lastPostAt DESC, id);
-- The author join's reverse direction: a changed user re-finding its threads.
CREATE INDEX IF NOT EXISTS thread_author ON thread (authorId);

CREATE TABLE IF NOT EXISTS post (id TEXT NOT NULL, threadId TEXT NOT NULL, authorId TEXT NOT NULL, body TEXT NOT NULL, createdAt REAL NOT NULL, editedAt REAL NOT NULL, deleted REAL NOT NULL, PRIMARY KEY (id));
-- Forward: a thread's posts in chronological order (opening post first).
CREATE INDEX IF NOT EXISTS post_thread ON post (threadId, createdAt, id);
-- Reverse: a user's posts / the author join re-finding parents.
CREATE INDEX IF NOT EXISTS post_author ON post (authorId);

CREATE TABLE IF NOT EXISTS vote (id TEXT NOT NULL, postId TEXT NOT NULL, userId TEXT NOT NULL, createdAt REAL NOT NULL, PRIMARY KEY (id));
-- Forward: a post's votes (the score countAs) + the reverse re-find.
CREATE INDEX IF NOT EXISTS vote_post ON vote (postId);
CREATE INDEX IF NOT EXISTS vote_user ON vote (userId);
