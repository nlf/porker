INSERT INTO ${queue}_jobs
  (args, repeat_every) VALUES ($1::jsonb, $2::interval)
  RETURNING *
