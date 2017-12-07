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

    const lockPendingJobs =
`UPDATE "${queue}_jobs"
  SET started_at = CASE
    WHEN started_at IS NULL THEN NOW()
    WHEN started_at IS NOT NULL AND repeat_every IS NOT NULL AND started_at + repeat_every <= NOW() THEN NOW()
    ELSE started_at
  END
  WHERE id = ANY (
    SELECT id FROM "${queue}_jobs"
      WHERE error_count = 0
        AND (started_at IS NULL
          OR repeat_every IS NOT NULL)
      ORDER BY (started_at + repeat_every), priority DESC, id
      LIMIT ${concurrency}
      FOR UPDATE SKIP LOCKED
  )
RETURNING *,
  CASE
    WHEN started_at = NOW() THEN true
    ELSE false
  END AS current,
  CASE
    WHEN started_at = NOW() THEN started_at
    WHEN repeat_every IS NOT NULL THEN started_at + repeat_every
  END AS next_run,
  CASE
    WHEN repeat_every IS NOT NULL AND started_at = NOW() THEN started_at + repeat_every
    WHEN repeat_every IS NOT NULL AND started_at != NOW() THEN started_at + repeat_every + repeat_every
    ELSE NULL
  END AS future_run;`;

    const lockPendingRetries =
`UPDATE "${queue}_jobs"
  SET started_at = NOW()
  WHERE id = ANY (
    SELECT id FROM "${queue}_jobs"
    WHERE error_count > 0
      AND error_count <= ${errorThreshold}
    ORDER BY retry_at, priority DESC, id
    LIMIT ${concurrency}
    FOR UPDATE SKIP LOCKED
  )
RETURNING *
`;

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
        insertJobs,
        listenPublishes,
        listenRetries,
        lockPendingJobs,
        lockPendingRetries,
        notifyQueue,
        notifyRetryQueue,
        resetJobs
    };
};
