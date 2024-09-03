WITH run_aggs AS (
    SELECT
        job_id, to_json(porker_runs.*) AS run
    FROM
        "porker_runs"
    WHERE
        job_id = $1::uuid
)
SELECT
    porker_jobs.*, json_agg(run_aggs.run) AS runs
FROM
    "porker_jobs"
LEFT JOIN
    "run_aggs" ON porker_jobs.id = run_aggs.job_id
WHERE
    porker_jobs.id = $1::uuid
GROUP BY
    porker_jobs.id;
