"use strict";
/** @import { PorkerOptions } from "./index.d.ts" */

/**
 * Return an object of queries with table names already populated
 * @param {PorkerOptions} options
 */
exports.queries = function ({ concurrency, retryDelay, maxRetries }) {
  const cancelJobs
= `
UPDATE "porker_jobs"
  SET status = 'SUCCESS', updated_at = NOW()
  WHERE id = ANY ($1::uuid[]);
`;

  const completeJobs
= `
UPDATE "porker_jobs"
  SET status = CASE WHEN repeat_every IS NOT NULL THEN 'WAITING' ELSE 'SUCCESS' END,
      start_after = CASE WHEN repeat_every IS NOT NULL THEN NOW() + repeat_every ELSE start_after END,
      updated_at = NOW()
  WHERE id = ANY ($1::uuid[]);
`;

  const createTable
= `
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "porker_jobs" ();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS id uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS created_at timestamp with time zone NOT NULL DEFAULT NOW();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS updated_at timestamp with time zone NOT NULL DEFAULT NOW();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS event text NOT NULL;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 0;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'WAITING';
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS start_after timestamp with time zone NOT NULL DEFAULT NOW();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS repeat_every interval;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS args jsonb;

CREATE TABLE IF NOT EXISTS "porker_runs" ();
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS id uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4();
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS job_id uuid NOT NULL;
ALTER TABLE "porker_runs" ADD FOREIGN KEY (job_id) REFERENCES porker_jobs(id);
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS started_at timestamp with time zone NOT NULL;
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS finished_at timestamp with time zone NOT NULL;
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS status text NOT NULL;
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS result jsonb;
`;

  const dropTable
= `
DROP TABLE IF EXISTS "porker_jobs" CASCADE;
DROP TABLE IF EXISTS "porker_runs" CASCADE;
`;

  const errorJobs
= `
UPDATE "porker_jobs"
  SET status = CASE WHEN repeat_every IS NOT NULL THEN 'WAITING' ELSE 'ERROR' END,
      start_after = CASE WHEN repeat_every IS NOT NULL THEN NOW() + repeat_every ELSE NOW() + INTERVAL '${retryDelay}' END,
      updated_at = NOW()
  WHERE id = ANY ($1::uuid[]);
`;

const jobStatus = `
WITH __temp AS (
  SELECT job_id, to_json(porker_runs.*) AS run FROM "porker_runs" WHERE job_id = $1::uuid
)
SELECT porker_jobs.*, json_agg(__temp.run) AS runs
  FROM "porker_jobs"
  LEFT JOIN "__temp"
    ON porker_jobs.id = __temp.job_id
  WHERE porker_jobs.id = $1::uuid
  GROUP BY porker_jobs.id;
`;

  const lockAllJobs
= `
WITH error_counts AS (
  SELECT job_id, count(*) FROM "porker_runs" WHERE status = 'ERROR' GROUP BY job_id
)
UPDATE "porker_jobs"
  SET
    status = 'IN_PROGRESS',
    updated_at = NOW()
  FROM "porker_jobs" AS "porker_jobs_previous"
  WHERE porker_jobs.id = ANY (
    SELECT id FROM "porker_jobs"
      WHERE
        (status = 'WAITING' AND event = ANY ($1::text[]) AND start_after <= NOW())
      OR
        (status = 'ERROR' AND (SELECT count FROM error_counts WHERE job_id = id) <= ${maxRetries} AND event = ANY ($2::text[]) AND start_after <= NOW())
      ORDER BY priority DESC, start_after, created_at
      LIMIT ${concurrency}
      FOR UPDATE SKIP LOCKED
  )
RETURNING porker_jobs.*, porker_jobs_previous.status;
`;

  const lockPendingJobs
= `
UPDATE "porker_jobs"
  SET
    status = 'IN_PROGRESS',
    updated_at = NOW()
  FROM "porker_jobs" AS "porker_jobs_previous"
  WHERE porker_jobs.id = ANY (
    SELECT id FROM "porker_jobs"
    WHERE status = 'WAITING' AND event = ANY ($1::text[]) AND start_after <= NOW()
    ORDER BY priority DESC, start_after, created_at
    LIMIT ${concurrency}
    FOR UPDATE SKIP LOCKED
  )
RETURNING porker_jobs.*, porker_jobs_previous.status;
`;

  const lockRetryJobs
= `
WITH error_counts AS (
  SELECT job_id, count(*) FROM "porker_runs" WHERE status = 'ERROR' GROUP BY job_id
)
UPDATE "porker_jobs"
  SET
    status = 'IN_PROGRESS',
    updated_at = NOW()
  FROM "porker_jobs" AS "porker_jobs_previous"
  WHERE porker_jobs.id = ANY (
    SELECT id FROM "porker_jobs"
    WHERE status = 'ERROR' AND event = ANY ($1::text[]) AND (SELECT count FROM error_counts WHERE job_id = id) <= ${maxRetries}
    ORDER BY priority DESC, start_after, created_at
    LIMIT ${concurrency}
    FOR UPDATE SKIP LOCKED
  )
RETURNING porker_jobs.*, porker_jobs_previous.status;
`;

  const findFuturePendingJob = `
SELECT id, start_after FROM "porker_jobs"
  WHERE start_after > NOW()
    AND status = 'WAITING'
    AND event = ANY ($1::text[])
  ORDER BY priority DESC, start_after, created_at
  LIMIT 1;
`;

  const findFutureRetryJob = `
WITH error_counts AS (
  SELECT job_id, count(*) FROM "porker_runs" WHERE status = 'ERROR' GROUP BY job_id
)
SELECT id, start_after FROM "porker_jobs"
  WHERE start_after > NOW()
    AND status = 'ERROR'
    AND event = ANY ($1::text[])
    AND (SELECT count FROM error_counts WHERE job_id = id) <= ${maxRetries}
  ORDER BY priority DESC, start_after, created_at
  LIMIT 1;
`;

  const findFutureJob
= `
WITH error_counts AS (
  SELECT job_id, count(*) FROM "porker_runs" WHERE status = 'ERROR' GROUP BY job_id
)
SELECT id, start_after FROM "porker_jobs"
  WHERE start_after > NOW()
    AND (status = 'WAITING' AND event = ANY ($1::text[]))
    OR (status = 'ERROR' AND event = ANY ($2::text[]) AND (SELECT count FROM error_counts WHERE job_id = id) <= ${maxRetries})
  ORDER BY priority DESC, start_after, created_at
  LIMIT 1;
`;

  /**
   * @template {object} [T=object]
   * @param {T[]} jobs
   */
  const insertJobs = (jobs) => {
    let counter = 3;
    const values = jobs.map(() => {
      return `($${++counter}::jsonb, $1, $2, $3::interval)`;
    });

    const query
= `
INSERT INTO "porker_jobs"
  (args, event, priority, repeat_every) VALUES ${values.join(", ")}
  RETURNING id;
`;

    return query;
  };

  const listenPublishes
= `LISTEN "porker_jobs_publish"`;

  const notifyQueue
= `NOTIFY "porker_jobs_publish"`;

  const recordRun = `
INSERT INTO "porker_runs"
  (job_id, started_at, finished_at, status, result)
  VALUES ($1, $2::timestamp, $3::timestamp, $4, $5)
  RETURNING id;
`;

  return {
    cancelJobs,
    completeJobs,
    createTable,
    dropTable,
    errorJobs,
    findFutureJob,
    findFuturePendingJob,
    findFutureRetryJob,
    insertJobs,
    jobStatus,
    listenPublishes,
    lockAllJobs,
    lockPendingJobs,
    lockRetryJobs,
    notifyQueue,
    recordRun,
  };
};
