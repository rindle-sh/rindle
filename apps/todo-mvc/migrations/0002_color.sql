-- NOT NULL needs a DEFAULT to satisfy existing rows at ADD COLUMN time. The following ordered
-- data-migration file recomputes existing rows without mixing DDL and DML in one durable run.
ALTER TABLE todo ADD COLUMN color REAL NOT NULL DEFAULT 0.58;
