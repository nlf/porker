'use strict';

exports.queries = function ({ queue, errorThreshold }) {

    const completeJob =
`DELETE FROM ${queue}
  WHERE id = $1`;

    const createTable =
`CREATE TABLE IF NOT EXISTS ${queue} (
  id serial PRIMARY KEY,
  priority integer NOT NULL DEFAULT 0,
  started_at timestamp with time zone,
  repeat_every interval,
  error_count integer NOT NULL DEFAULT 0,
  args jsonb
)`;

    const errorJob =
`UPDATE ${queue}
  SET error_count = error_count + 1,
      started_at = NULL
  WHERE id = $1`;

    const findRecurring =
`SELECT started_at + repeat_every AS next_run
FROM ${queue}
WHERE repeat_every IS NOT NULL
ORDER BY (started_at + repeat_every)
LIMIT 1`;

    const insertJob =
`INSERT INTO ${queue}
  (args, priority, repeat_every) VALUES ($1::jsonb, $2, $3::interval)
  RETURNING *`;

    const listenQueue =
`LISTEN ${queue}`;

    const lockJob =
`UPDATE ${queue}
  SET started_at = NOW()
WHERE id = (
  SELECT id
  FROM ${queue}
  WHERE (started_at IS NULL
    AND error_count <= ${errorThreshold})
  OR (repeat_every IS NOT NULL
    AND started_at + repeat_every < NOW())
  ORDER BY priority, id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *`;

    const notifyQueue =
`NOTIFY ${queue}`;

    return {
        completeJob,
        createTable,
        errorJob,
        findRecurring,
        insertJob,
        listenQueue,
        lockJob,
        notifyQueue
    };
};
