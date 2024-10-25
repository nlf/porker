INSERT INTO __JOBS_TABLE__
    (channel, priority, max_retries, retry_delay, args)
    VALUES ($1::text, $2::integer, $3::integer, $4::interval, $5::jsonb)
RETURNING id;
