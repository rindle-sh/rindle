INSERT OR IGNORE INTO todo_list (id, name, createdAt)
VALUES ('main', 'Today', 1710000000000);

INSERT OR IGNORE INTO todo (
  id,
  listId,
  title,
  completed,
  temperature,
  createdAt,
  updatedAt,
  color
)
VALUES
  ('todo-1', 'main', 'Read the fragment query', 0, 0, 1710000000000, 1710000000000, 0.12),
  ('todo-2', 'main', 'Move across one color field', 0, 0, 1710000001000, 1710000001000, 0.29),
  ('todo-3', 'main', 'Watch the row render counts', 0, 0, 1710000002000, 1710000002000, 0.46),
  ('todo-4', 'main', 'Toggle a completed todo', 1, 0, 1710000003000, 1710000003000, 0.63),
  ('todo-5', 'main', 'Add a new local-first task', 0, 0, 1710000004000, 1710000004000, 0.8);
