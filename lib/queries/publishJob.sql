INSERT INTO "porker_jobs"
    (channel, priority, repeat_every, max_retries, retry_delay, args)
    VALUES ($1::text, $2::integer, $3::interval, $4::integer, $5::interval, $6::jsonb)
RETURNING id;
