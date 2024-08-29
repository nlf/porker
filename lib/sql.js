"use strict";
/** @import { PorkerOptions } from "./index.d.ts" */

/**
 * Return an object of queries with table names already populated
 * @param {PorkerOptions} options
 */
exports.queries = function ({ concurrency, retryDelay, maxRetries }) {
  const completeJobs
= `DELETE FROM "porker_jobs"
  WHERE id = ANY ($1::int[])`;

  const createTable
= `CREATE TABLE IF NOT EXISTS "porker_jobs" (
  id serial PRIMARY KEY,
  priority integer NOT NULL DEFAULT 0,
  started_at timestamp with time zone,
  repeat_every interval,
  error_count integer NOT NULL DEFAULT 0,
  args jsonb
);

ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS retry_at timestamp with time zone;

CREATE INDEX IF NOT EXISTS "porker_jobs_priority_index" ON "porker_jobs" (priority);
CREATE INDEX IF NOT EXISTS "porker_jobs_started_at_index" ON "porker_jobs" (started_at);
CREATE INDEX IF NOT EXISTS "porker_jobs_repeat_every_index" ON "porker_jobs" (repeat_every);
CREATE INDEX IF NOT EXISTS "porker_jobs_error_count_index" ON "porker_jobs" (error_count);
CREATE INDEX IF NOT EXISTS "porker_jobs_retry_at_index" ON "porker_jobs" (retry_at);`;

  const dropTable
= `DROP TABLE IF EXISTS "porker_jobs" CASCADE`;

  const errorJobs
= `UPDATE "porker_jobs"
  SET error_count = error_count + 1,
      retry_at = NOW() + INTERVAL '${retryDelay}',
      started_at = NULL
  WHERE id = ANY ($1::int[])`;

  const lockCurrentJobs
= `
UPDATE "porker_jobs"
  SET started_at = NOW()
  WHERE id = ANY (
    SELECT id FROM "porker_jobs"
      WHERE error_count = 0
      AND (started_at IS NULL
        OR (repeat_every IS NOT NULL
          AND COALESCE(started_at, TIMESTAMP '2000-01-01 00:00:00') + repeat_every <= NOW()))
      ORDER BY priority DESC, (started_at + repeat_every), id
      LIMIT ${concurrency}
      FOR UPDATE SKIP LOCKED
  )
RETURNING *;
`;

  const findFutureJob
= `
SELECT id, started_at + repeat_every AS next_run FROM "porker_jobs"
  WHERE error_count = 0
    AND repeat_every IS NOT NULL
    AND started_at + repeat_every >= NOW()
  ORDER BY priority DESC, (started_at + repeat_every), id
  LIMIT 1;
`;

  const lockPendingRetries
= `UPDATE "porker_jobs"
  SET started_at = NOW()
  WHERE id = ANY (
    SELECT id FROM "porker_jobs"
    WHERE error_count > 0
      AND error_count <= ${maxRetries}
    ORDER BY retry_at, priority DESC, id
    LIMIT ${concurrency}
    FOR UPDATE SKIP LOCKED
  )
RETURNING *
`;

  /** @param {unknown[]} jobs */
  const insertJobs = (jobs) => {
    let counter = 2;
    const values = jobs.map(() => {
      return `($${++counter}::jsonb, $1, $2::interval)`;
    });

    const query
= `INSERT INTO "porker_jobs"
  (args, priority, repeat_every) VALUES ${values.join(", ")}
  RETURNING id`;

    return query;
  };

  const listenPublishes
= `LISTEN "porker_jobs_publish"`;

  const listenRetries
= `LISTEN "porker_jobs_retry"`;

  const notifyQueue
= `NOTIFY "porker_jobs_publish"`;

  const notifyRetryQueue
= `NOTIFY "porker_jobs_retry"`;

  const resetJobs
= `UPDATE "porker_jobs"
  SET retry_at = NULL,
      error_count = 0
  WHERE id = ANY ($1::int[])`;

  return {
    completeJobs,
    createTable,
    dropTable,
    errorJobs,
    findFutureJob,
    insertJobs,
    listenPublishes,
    listenRetries,
    lockCurrentJobs,
    lockPendingRetries,
    notifyQueue,
    notifyRetryQueue,
    resetJobs,
  };
};
