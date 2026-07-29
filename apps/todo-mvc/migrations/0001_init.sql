CREATE TABLE IF NOT EXISTS todo_list (
  id TEXT NOT NULL,
  name TEXT NOT NULL,
  createdAt REAL NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS todo (
  id TEXT NOT NULL,
  listId TEXT NOT NULL,
  title TEXT NOT NULL,
  completed REAL NOT NULL,
  temperature REAL NOT NULL,
  createdAt REAL NOT NULL,
  updatedAt REAL NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS todo_list_order ON todo (listId, createdAt, id);
