INSERT INTO ${queue}_jobs
  (args, priority, repeat_every) VALUES ($1::jsonb, $2, $3::interval)
  RETURNING *
