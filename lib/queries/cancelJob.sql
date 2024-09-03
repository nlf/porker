UPDATE "porker_jobs"
    SET status = 'CANCELLED',
        updated_at = NOW()
    WHERE id = $1::uuid;
