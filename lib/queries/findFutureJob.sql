WITH error_counts AS (
    SELECT
        job_id, count(*)
    FROM
        "porker_runs"
    WHERE
        status = 'ERROR'
    GROUP BY
        job_id
)
SELECT
    id, start_after
FROM
    "porker_jobs"
WHERE
    status = ANY ('{"WAITING", "ERROR"}')
    AND start_after >= NOW()
    AND CASE
        WHEN status = 'WAITING' THEN
            channel = ANY ($1::text[])
        ELSE
            channel = ANY ($2::text[])
            AND max_retries >= (SELECT count FROM error_counts WHERE job_id = id)
        END
ORDER BY
    priority DESC,
    start_after,
    created_at
LIMIT 1;
