WITH error_counts AS (SELECT job_id, count(*) FROM __RUNS_TABLE__ WHERE status = 'ERROR' AND job_id = $1::uuid GROUP BY job_id)
UPDATE __JOBS_TABLE__
    SET status = 'ERROR',
        updated_at = statement_timestamp(),
        start_after = CASE
            WHEN max_retries > COALESCE((SELECT count FROM error_counts WHERE job_id = id), 0) THEN
                start_after
            ELSE
                statement_timestamp() + COALESCE(retry_delay, '0'::interval)
            END
    WHERE id = $1::uuid;
