## Porker

A super simple job queue for Node.js (7.6 and higher, uses async functions) built on top of Postgres (9.5 and higher since we use `SELECT FOR UPDATE SKIP LOCKED`)

### Usage


#### `new Porker({ host, port, user, password, database, queue, errorThreshold, timeout })`

Creates and returns a new worker.

- `host`: (optional) Postgres host to connect to, defaults to `localhost` or the value of `PGHOST`
- `port`: (optional) Port to connect to on the postgres server, defaults to `5432` or the value of `PGPORT`
- `user`: (optional) Username to connect to postgres with, defaults to current logged in user or value of `PGUSER`
- `password`: (optional) Password for authentication to postgres, defaults to nothing or the value of `PGPASSWORD`
- `database`: *required* Name of the database to store job queues in, must be specified here or via `PGDATABASE`
- `queue`: *required* Name of the job queue, will be represented as a table with a name in the form of `${queue}_jobs`
- `errorThreshold`: (optional) Maximum `error_count` a job can have to be received by this worker, defaults to `0`
- `timeout`: (optional) Maximum time a worker function can run before a job will be flagged as an error and returned to the queue, defaults to `15000` (15 seconds)

#### `porker.connect()`

AsyncFunction that connects to the database. This is unnecessary to call manually as all methods that require it will call it themselves.

#### `porker.create()`

AsyncFunction that creates the table representing the current queue. The table schema is as follows:

```sql
CREATE TABLE IF NOT EXISTS ${queue}_jobs (
  id serial PRIMARY KEY,
  priority integer NOT NULL DEFAULT 0,
  started_at timestamp with time zone,
  repeat_every interval,
  error_count integer NOT NULL DEFAULT 0,
  args jsonb
);
```

#### `porker.subscribe(fn)`

AsyncFunction that adds a subscription to the current queue.

The parameter, `fn` should be an AsyncFunction that accepts a single parameter which will be an object representing the full row from the database for the current job. If the function throws, the job's `error_count` column will be incremented and it will be returned to the queue. If the function runs successfully, the row will be deleted.

#### `porker.publish(job, { priority, repeat })`

AsyncFunction that publishes a new job to the queue and returns it. The first parameter must be serializable as a `jsonb` column. The second parameter is entirely optional and is used for passing additional options.

- `priority`: (optional) A priority for the current job, higher priorities will be processed sooner. Defaults to `0`
- `repeat`: (optional) For creating a recurring job, can be specified as any [Postgres interval](https://www.postgresql.org/docs/9.5/static/datatype-datetime.html) See section 8.5.4. Omitting this option creates a standard non-recurring job.

#### `porker.unpublish(job || id)`

AsyncFunction that unpublishes a job. May pass either an object with an `id` property, or the `id` itself. This is mostly useful for canceling a recurring job.

#### `porker.end()`

AsyncFunction that stops the job loop and disconnects from the database.
