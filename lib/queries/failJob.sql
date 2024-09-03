UPDATE "porker_jobs"
    SET status = 'ERROR',
        start_after = NOW() + COALESCE(retry_delay, '0'::interval),
        updated_at = NOW()
    WHERE id = $1::uuid;
