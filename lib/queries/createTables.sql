CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "porker_jobs" ();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS id uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS created_at timestamp with time zone NOT NULL DEFAULT NOW();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS updated_at timestamp with time zone NOT NULL DEFAULT NOW();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS channel text NOT NULL;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 0;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'WAITING';
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS start_after timestamp with time zone NOT NULL DEFAULT NOW();
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS repeat_every interval;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS max_retries integer NOT NULL DEFAULT 0;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS retry_delay interval;
ALTER TABLE "porker_jobs" ADD COLUMN IF NOT EXISTS args jsonb;

CREATE TABLE IF NOT EXISTS "porker_runs" ();
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS id uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4();
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS job_id uuid NOT NULL;
ALTER TABLE "porker_runs" ADD FOREIGN KEY (job_id) REFERENCES porker_jobs(id);
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS started_at timestamp with time zone NOT NULL;
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS finished_at timestamp with time zone NOT NULL;
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS status text NOT NULL;
ALTER TABLE "porker_runs" ADD COLUMN IF NOT EXISTS result jsonb;

