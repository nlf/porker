'use strict';

exports.queries = function ({ queue, errorThreshold }) {

    const completeJob =
`DELETE FROM "${queue}_jobs"
  WHERE id = $1`;

    const createTable =
`CREATE TABLE IF NOT EXISTS "${queue}_jobs" (
  id serial PRIMARY KEY,
  priority integer NOT NULL DEFAULT 0,
  started_at timestamp with time zone,
  repeat_every interval,
  error_count integer NOT NULL DEFAULT 0,
  args jsonb
)`;

    const dropTable =
`DROP TABLE IF EXISTS "${queue}_jobs" CASCADE`;

    const errorJob =
`UPDATE "${queue}_jobs"
  SET error_count = error_count + 1,
      started_at = NULL
  WHERE id = $1`;

    const findRecurring =
`SELECT started_at + repeat_every AS next_run
FROM "${queue}_jobs"
WHERE repeat_every IS NOT NULL
ORDER BY (started_at + repeat_every)
LIMIT 1`;

    const indexErrorCount =
`CREATE INDEX IF NOT EXISTS "${queue}_jobs_error_count_index" ON "${queue}_jobs" (error_count)`;

    const indexPriority =
`CREATE INDEX IF NOT EXISTS "${queue}_jobs_priority_index" ON "${queue}_jobs" (priority)`;

    const indexRepeatEvery =
`CREATE INDEX IF NOT EXISTS "${queue}_jobs_repeat_every_index" ON "${queue}_jobs" (repeat_every)`;

    const indexStartedAt =
`CREATE INDEX IF NOT EXISTS "${queue}_jobs_started_at_index" ON "${queue}_jobs" (started_at)`;

    const insertJob =
`INSERT INTO "${queue}_jobs"
  (args, priority, repeat_every) VALUES ($1::jsonb, $2, $3::interval)
  RETURNING *`;

    const listenQueue =
`LISTEN "${queue}_jobs"`;

    const lockJob =
`UPDATE "${queue}_jobs"
  SET started_at = NOW()
WHERE id = (
  SELECT id
  FROM "${queue}_jobs"
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
`NOTIFY "${queue}_jobs"`;

    return {
        completeJob,
        createTable,
        dropTable,
        errorJob,
        findRecurring,
        indexErrorCount,
        indexPriority,
        indexRepeatEvery,
        indexStartedAt,
        insertJob,
        listenQueue,
        lockJob,
        notifyQueue
    };
};
