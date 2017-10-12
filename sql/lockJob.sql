UPDATE ${queue}_jobs
  SET started_at = NOW()
WHERE id = (
  SELECT id
  FROM ${queue}_jobs
  WHERE (started_at IS NULL
    AND error_count <= ${errorThreshold})
  OR (repeat_every IS NOT NULL
    AND started_at + repeat_every < NOW())
  ORDER BY priority, id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *
