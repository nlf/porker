'use strict';

exports.queries = function ({ concurrency, queue, retryDelay, errorThreshold }) {

    const completeJobs =
`DELETE FROM "${queue}_jobs"
  WHERE id = ANY ($1::int[])`;

    const createTable =
`CREATE TABLE IF NOT EXISTS "${queue}_jobs" (
  id serial PRIMARY KEY,
  priority integer NOT NULL DEFAULT 0,
  started_at timestamp with time zone,
  repeat_every interval,
  error_count integer NOT NULL DEFAULT 0,
  retry_at timestamp with time zone,
  args jsonb
);

CREATE INDEX IF NOT EXISTS "${queue}_jobs_priority_index" ON "${queue}_jobs" (priority);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_started_at_index" ON "${queue}_jobs" (started_at);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_repeat_every_index" ON "${queue}_jobs" (repeat_every);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_error_count_index" ON "${queue}_jobs" (error_count);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_retry_at_index" ON "${queue}_jobs" (retry_at);`;

    const dropTable =
`DROP TABLE IF EXISTS "${queue}_jobs" CASCADE`;

    const errorJobs =
`UPDATE "${queue}_jobs"
  SET error_count = error_count + 1,
      retry_at = NOW() + INTERVAL '${retryDelay}',
      started_at = NULL
  WHERE id = ANY ($1::int[])`;

    const findRecurring =
`SELECT started_at + repeat_every AS next_run
FROM "${queue}_jobs"
WHERE repeat_every IS NOT NULL
  AND error_count = 0
ORDER BY (started_at + repeat_every)
LIMIT 1`;

    const findRetry =
`SELECT retry_at AS next_run
FROM "${queue}_jobs"
WHERE retry_at IS NOT NULL
  AND error_count > 0
  AND error_count <= ${errorThreshold}
ORDER BY retry_at
LIMIT 1`;

    const insertJobs = (jobs) => {

        let counter = 2;
        const values = jobs.map((job) => {

            return `(\$${++counter}::jsonb, \$1, \$2::interval)`;
        });

        const query =
`INSERT INTO "${queue}_jobs"
  (args, priority, repeat_every) VALUES ${values.join(', ')}
  RETURNING id`;

        return query;
    };

    const listenQueue =
`LISTEN "${queue}_jobs"`;

    const lockJobs =
`UPDATE "${queue}_jobs"
  SET started_at = NOW()
WHERE id = ANY (
  SELECT id
  FROM "${queue}_jobs"
  WHERE error_count = 0
  AND (started_at IS NULL
    OR (repeat_every IS NOT NULL
      AND error_count = 0
      AND started_at + repeat_every < NOW()))
  ORDER BY priority, id
  LIMIT ${concurrency}
  FOR UPDATE SKIP LOCKED
)
RETURNING *`;

    const lockRetries =
`UPDATE "${queue}_jobs"
  SET started_at = NOW()
WHERE id = ANY (
  SELECT id
  FROM "${queue}_jobs"
  WHERE retry_at < NOW()
    AND error_count > 0
    AND error_count <= ${errorThreshold}
  ORDER BY priority, id
  LIMIT ${concurrency}
  FOR UPDATE SKIP LOCKED
)
RETURNING *`;

    const notifyQueue =
`NOTIFY "${queue}_jobs"`;

    const resetJobs =
`UPDATE "${queue}_jobs"
  SET retry_at = NULL,
      error_count = 0
  WHERE id = ANY ($1::int[])`;

    return {
        completeJobs,
        createTable,
        dropTable,
        errorJobs,
        findRecurring,
        findRetry,
        insertJobs,
        listenQueue,
        lockJobs,
        lockRetries,
        notifyQueue,
        resetJobs
    };
};
