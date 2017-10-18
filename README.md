## Porker

A super simple job queue for Node.js (7.6 and higher, uses async functions) built on top of Postgres (9.6 and higher since we use `SELECT FOR UPDATE SKIP LOCKED` and `ADD COLUMN IF NOT EXISTS`)

### Usage


#### `new Porker({ connection, queue, errorThreshold, retryDelay, timeout, concurrency })`

Creates and returns a new worker.

- `connection`: (optional) Postgres connection details, passed through to [new Client](https://node-postgres.com/api/client#new-client-config-object-) in [pg](https://node-postgres.com/)
- `queue`: *required* Name of the job queue, will be represented as a table with a name in the form of `${queue}_jobs`
- `errorThreshold`: (optional) Maximum `error_count` a job can have to be received by the retry handler if one is specified, defaults to `1`
- `retryDelay`: (optional) Interval to wait between failure and retry attempt, specified as a Postgres interval string, defaults to `'5 minutes'`
- `timeout`: (optional) Maximum time a worker function can run before a job will be flagged as an error and returned to the queue, defaults to `15000` (15 seconds)
- `concurrency`: (optional) How many jobs to process simultaneously, defaults to `1`

#### `porker.connect()`

AsyncFunction that connects to the database. This is unnecessary to call manually as all methods that require a connection will call it themselves.

#### `porker.create()`

AsyncFunction that creates the table representing the current queue. The table schema is as follows (replacing `${queue}` with the name of your queue):

```sql
CREATE TABLE IF NOT EXISTS "${queue}_jobs" (
  id serial PRIMARY KEY,
  priority integer NOT NULL DEFAULT 0,
  started_at timestamp with time zone,
  repeat_every interval,
  error_count integer NOT NULL DEFAULT 0,
  retry_at timestamp with time zone,
  args jsonb
);

CREATE INDEX IF NOT EXISTS "${queue}_jobs_error_count_index" ON "${queue}_jobs" (error_count);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_priority_index" ON "${queue}_jobs" (priority);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_repeat_every_index" ON "${queue}_jobs" (repeat_every);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_retry_at_index" ON "${queue}_jobs" (retry_at);
CREATE INDEX IF NOT EXISTS "${queue}_jobs_started_at_index" ON "${queue}_jobs" (started_at);
```

#### `porker.drop()`

AsyncFunction that drops the table representing the current queue. THIS WILL DESTROY DATA.

#### `porker.subscribe(fn)`

AsyncFunction that adds a subscription to the current queue.

The parameter, `fn` should be an AsyncFunction that accepts a single parameter which will be an object representing the full row from the database for the current job. If the function throws, the job's `error_count` column will be incremented and it will be returned to the queue. If the function runs successfully, the row will be deleted.

#### `porker.retry(fn)`

AsyncFunction that adds a retry handler to the current queue.

The parameter, `fn` should be an AsyncFunction that accepts a single parameter which will be an object representing the full row from the database for the current job. Jobs passed to this worker will have failed at least once, but not more than `errorThreshold` times. A job that succeeds this handler will be cleared from the database as though it succeeded the first time. If a recurring job is delivered to the retry handler and succeeds there, the `error_count` will be reset to `0`.

#### `porker.publish(jobs, { priority, repeat })`

AsyncFunction that publishes a new job or jobs to the queue and returns an array of ids. The first parameter may be a single object to publish one job or an array of objects to publish multiple jobs, however each job must be serializable as a `jsonb` column. The second parameter is entirely optional and is used for passing additional options.

- `priority`: (optional) A priority for the current job, higher priorities will be processed sooner. Defaults to `0`
- `repeat`: (optional) For creating a recurring job, can be specified as any [Postgres interval](https://www.postgresql.org/docs/9.5/static/datatype-datetime.html) See section 8.5.4. Omitting this option creates a standard non-recurring job.

#### `porker.unpublish(ids)`

AsyncFunction that unpublishes a job or jobs. May pass either a single job id or an array of job ids. This is mostly useful for canceling recurring jobs.

#### `porker.end()`

AsyncFunction that stops the job loop and disconnects from the database.
