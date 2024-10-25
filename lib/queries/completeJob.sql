UPDATE __JOBS_TABLE__
    SET status = 'SUCCESS',
        updated_at = statement_timestamp()
    WHERE id = $1::uuid;
