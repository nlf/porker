SELECT started_at + repeat_every AS next_run
FROM ${queue}_jobs
WHERE repeat_every IS NOT NULL
ORDER BY (started_at + repeat_every)
LIMIT 1
