UPDATE "porker_jobs"
    SET status = 'SUCCESS',
        updated_at = NOW()
    WHERE id = $1::uuid;
