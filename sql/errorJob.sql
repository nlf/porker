UPDATE ${queue}_jobs
  SET error_count = error_count + 1,
      started_at = NULL
  WHERE id = $1
