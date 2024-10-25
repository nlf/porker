WITH run_aggs AS (
    SELECT
        job_id, to_json(__RUNS_TABLE__.*) AS run
    FROM
        __RUNS_TABLE__
    WHERE
        job_id = $1::uuid
)
SELECT
    __JOBS_TABLE__.*, json_agg(run_aggs.run) AS runs
FROM
    __JOBS_TABLE__
LEFT JOIN
    "run_aggs" ON __JOBS_TABLE__.id = run_aggs.job_id
WHERE
    __JOBS_TABLE__.id = $1::uuid
GROUP BY
    __JOBS_TABLE__.id;
