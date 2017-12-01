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
  args jsonb
);

ALTER TABLE "${queue}_jobs" ADD COLUMN IF NOT EXISTS retry_at timestamp with time zone;

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

    const findNextRun =
`SELECT t.id, t.repeat_every, t.next_run, t.next_run + t.repeat_every as future_run
FROM (SELECT id, priority, error_count, started_at, repeat_every, COALESCE(started_at + repeat_every, NOW()) AS next_run FROM "${queue}_jobs") AS t
WHERE t.error_count = 0
  AND (t.started_at IS NULL
    OR t.repeat_every IS NOT NULL)
  AND t.next_run <= NOW() + interval '1 second'
ORDER BY t.next_run, t.priority DESC, t.id
LIMIT ${concurrency}
FOR UPDATE SKIP LOCKED`;

    const findRetries =
`SELECT id, retry_at
FROM "${queue}_jobs"
WHERE retry_at <= NOW() + interval '1 second'
  AND error_count > 0
  AND error_count <= ${errorThreshold}
ORDER BY retry_at, priority, id
LIMIT ${concurrency}
FOR UPDATE SKIP LOCKED`;

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

    const listenPublishes =
`LISTEN "${queue}_jobs_publish"`;

    const listenRetries =
`LISTEN "${queue}_jobs_retry"`;

    const lockJobs =
`UPDATE "${queue}_jobs"
  SET started_at = NOW()
WHERE id = ANY ($1::int[])
RETURNING *`;

    const lockRetries =
`UPDATE "${queue}_jobs"
  SET started_at = NOW()
WHERE id = ANY ($1::int[])
RETURNING *`;

    const notifyQueue =
`NOTIFY "${queue}_jobs_publish"`;

    const notifyRetryQueue =
`NOTIFY "${queue}_jobs_retry"`;

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
        findNextRun,
        findRetries,
        insertJobs,
        listenPublishes,
        listenRetries,
        lockJobs,
        lockRetries,
        notifyQueue,
        notifyRetryQueue,
        resetJobs
    };
};
