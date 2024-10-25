WITH error_counts AS (
    SELECT
        job_id, count(*)
    FROM
        __RUNS_TABLE__
    WHERE
        status = 'ERROR'
    GROUP BY
        job_id
)
UPDATE __JOBS_TABLE__
SET
    status = 'IN_PROGRESS',
    updated_at = NOW()
FROM
    __JOBS_TABLE__ AS jobs_old
WHERE
    __JOBS_TABLE__.id = ANY (
        SELECT
            id
        FROM
            __JOBS_TABLE__
        WHERE
            status = ANY ('{"WAITING","ERROR"}')
            AND start_after <= NOW()
            AND CASE
                WHEN status = 'WAITING' THEN
                    channel = ANY ($1::text[])
                WHEN status = 'ERROR' THEN
                    channel = ANY ($2::text[])
                    AND max_retries >= (SELECT count FROM error_counts WHERE job_id = id)
                END
        ORDER BY
            priority DESC,
            start_after,
            created_at
        LIMIT
            $3::integer
        FOR UPDATE SKIP LOCKED
    )
    AND __JOBS_TABLE__.id = jobs_old.id
RETURNING __JOBS_TABLE__.*, jobs_old.status AS status;
