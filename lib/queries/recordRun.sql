INSERT INTO __RUNS_TABLE__
    (job_id, started_at, finished_at, status, result)
        VALUES ($1::uuid, $2::timestamp, $3::timestamp, $4::text, $5::jsonb);
