UPDATE todo
SET color = CASE
  WHEN temperature IS NULL THEN 0.58
  WHEN temperature < 0 THEN 0
  WHEN temperature > 100 THEN 1
  WHEN temperature > 1 THEN temperature / 100
  ELSE temperature
END;

UPDATE todo
SET title = 'Move across one color field'
WHERE title = 'Drag one temperature slider';
