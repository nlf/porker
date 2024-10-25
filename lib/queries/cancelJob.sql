UPDATE __JOBS_TABLE__
    SET status = 'CANCELLED',
        updated_at = NOW()
    WHERE id = $1::uuid;
