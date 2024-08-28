"use strict";

const { once } = require("node:events");
const t = require("tap");
const Porker = require("../");
const { Deferred } = require("../lib/util");
/** @import { Job } from "../" */

const Http = require("http");
const Pg = require("pg");

const Util = require("util");

const get = Util.promisify(Http.get);
const timeout = Util.promisify(setTimeout);

const connection = process.env.PORKER_CONNECTION || { database: "porker-test", user: "porker-test", password: "porker-test" };
const db = new Pg.Client(connection);

t.test("Porker", (t) => {
  t.before(async () => {
    await db.connect();
  });

  t.after(async () => {
    await db.end();
  });

  t.afterEach(async () => {
    await db.query("BEGIN");
    let res = await db.query(`SELECT 'DROP TABLE IF EXISTS ' || quote_ident(table_schema) || '.' || quote_ident(table_name) || ' CASCADE;' AS drop_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND NOT table_schema ~ '^(information_schema|pg_.*)$'`);
    for (const row of res.rows) {
      await db.query(row.drop_table);
    }

    res = await db.query(`SELECT 'DROP SEQUENCE IF EXISTS ' || quote_ident(relname) || ' CASCADE;' AS drop_sequence FROM pg_statio_user_sequences`);
    for (const row of res.rows) {
      await db.query(row.drop_sequence);
    }

    await db.query("COMMIT");
  });

  t.test("accepts strings for connection settings", (t) => {
    t.doesNotThrow(() => {
      new Porker({ connection: "postgres://localhost/porker_test_suite", queue: "test" });
    });

    t.end();
  });

  t.test("throws when no queue is specified", (t) => {
    t.throws(() => {
      new Porker();
    }, "Missing required parameter: queue");

    t.end();
  });

  t.test("throws when a subscriber is added twice", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe(async () => {});
    await t.rejects(worker.subscribe(async () => {}), "A subscriber has already been added to this queue");
  });

  t.test("throws when retrier is added twice", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.retry(async () => {});
    await t.rejects(worker.retry(async () => {}), "A retry handler has already been added to this queue");
  });

  t.test("does not throw when queues have a dash", async (t) => {
    const worker = new Porker({ connection, queue: "test-queue" });
    t.teardown(async () => {
      await worker.end();
    });

    await t.resolves(worker.create());
  });

  t.test("can create its own table", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_jobs'");
    t.equal(res.rowCount, 7);

    const rows = res.rows.reduce((acc, row) => [...acc, row.column_name], []);
    t.strictSame(rows, ["id", "priority", "started_at", "repeat_every", "error_count", "args", "retry_at"]);
  });

  t.test("can drop its own table", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    let res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_jobs'");
    t.equal(res.rowCount, 7);

    const rows = res.rows.reduce((acc, row) => [...acc, row.column_name], []);
    t.strictSame(rows, ["id", "priority", "started_at", "repeat_every", "error_count", "args", "retry_at"]);

    await worker.drop();
    res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_jobs'");
    t.equal(res.rowCount, 0);
  });

  t.test("can end without a client connection", async (t) => {
    let worker = new Porker({ connection, queue: "test" });

    await worker.create();
    await worker.end();

    worker = new Porker({ connection, queue: "test" });
    await worker.subscribe(() => {});

    await t.resolves(worker.end());
  });

  t.test("can handle a single job", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish({ some: "data" });

    const listener = new Deferred();

    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle a failing job", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const [id] = await worker.publish({ some: "data" });

    const listener = new Deferred();

    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 1);
    t.equal(res.rows[0].id, id);
    t.equal(res.rows[0].error_count, 1);
    t.ok(res.rows[0].retry_at > new Date());
  });

  t.test("can retry a failed job", async (t) => {
    const worker = new Porker({ connection, queue: "test", retryDelay: "10 milliseconds" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const drainedRetries = new Promise((resolve) => {
      worker.once("drainRetries", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    await worker.retry((job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.error_count, 1);
      retrier.resolve(true);
    });

    await worker.publish({ some: "data" });

    await Promise.all([
      listener.promise,
      retrier.promise,
      drained,
      drainedRetries,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can retry a failed job with a delay", async (t) => {
    const worker = new Porker({ connection, queue: "test", retryDelay: "150 milliseconds" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const drainedRetries = new Promise((resolve) => {
      worker.once("drainRetries", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    await worker.retry((job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.error_count, 1);
      retrier.resolve(true);
    });

    await worker.publish({ some: "data" });

    await Promise.all([
      listener.promise,
      retrier.promise,
      drained,
      drainedRetries,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle two jobs", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish({ some: "data" });
    await worker.publish({ some: "data" });

    const listener = new Deferred();

    let count = 0;
    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      if (++count === 2) {
        listener.resolve(true);
      }
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can retry two failed jobs", async (t) => {
    const worker = new Porker({ connection, queue: "test", retryDelay: "1 millisecond" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const drainedRetries = new Promise((resolve) => {
      worker.once("drainRetries", resolve);
    });

    await worker.publish({ some: "data" });
    await worker.publish({ some: "data" });

    const listener = new Deferred();

    let listenerCount = 0;
    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      if (++listenerCount === 2) {
        listener.resolve(true);
      }

      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    let retrierCount = 0;
    await worker.retry((job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.error_count, 1);
      if (++retrierCount === 2) {
        retrier.resolve(true);
      }
    });

    await Promise.all([
      listener.promise,
      drained,
      retrier.promise,
      drainedRetries,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can bulk publish jobs", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish([{ some: "data" }, { some: "data" }]);

    const listener = new Deferred();

    let count = 0;
    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      if (++count === 2) {
        listener.resolve(true);
      }
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle a publish after a subscription", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
    });

    await worker.publish({ some: "data" });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle two publishes after a subscription", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();
    const eventOne = new Deferred();
    const eventTwo = new Deferred();
    const events = [eventOne, eventTwo];

    await worker.subscribe((job) => {
      t.strictSame(job.args, { some: "data" });
      if (events.length) {
        const event = /** @type {Deferred} */ (events.shift());
        event.resolve(true);
      }
    });

    await worker.publish({ some: "data" });
    await eventOne.promise;
    await once(worker, "drain");

    await worker.publish({ some: "data" });
    await eventTwo.promise;
    await once(worker, "drain");

    const res = await db.query("SELECT * from test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can timeout a job", async (t) => {
    const worker = new Porker({ connection, queue: "test", timeout: 1 });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe(async (job) => {
      t.strictSame(job.args, { some: "data" });
      await timeout(10);
      listener.resolve(true);
    });

    await worker.publish({ some: "data" });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 1);
    const row = Object.assign({}, res.rows[0]);
    t.hasStrict(row, { error_count: 1, args: { some: "data" } });
  });

  t.test("can create a recurring job", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      let count = 0;
      worker.on("drain", () => {
        if (++count === 2) {
          resolve(true);
        }
      });
    });

    const listener = new Deferred();

    let count = 0;
    await worker.subscribe((job) => {
      t.strictSame(job.args, { timer: "data" });
      t.not(job.repeat_every, null);
      if (++count === 2) {
        listener.resolve(true);
      }
    });

    await worker.publish({ timer: "data" }, { repeat: "100 milliseconds" });
    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 1);
    const row = Object.assign({}, res.rows[0]);
    t.hasStrict(row, { error_count: 0, args: { timer: "data" } });
    t.not(row.repeat_every, null);
  });

  t.test("can retry a failed recurring job and reset it", async (t) => {
    const worker = new Porker({ connection, queue: "test", retryDelay: "10 milliseconds" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      let count = 0;
      // should fire twice
      worker.on("drain", () => {
        if (++count === 2) {
          resolve(true);
        }
      });
    });

    const drainedRetries = new Promise((resolve) => {
      // should fire once
      worker.once("drainRetries", resolve);
    });

    const listener = new Deferred();

    let count = 0;
    // should fire twice
    await worker.subscribe((job) => {
      t.strictSame(job.args, { timer: "data" });
      t.not(job.repeat_every, null);
      if (++count === 1) {
        throw new Error("Uh oh");
      }

      if (count === 2) {
        return listener.resolve(true);
      }
    });

    const retrier = new Deferred();

    // should fire once
    await worker.retry((job) => {
      t.strictSame(job.args, { timer: "data" });
      t.not(job.repeat_every, null);
      retrier.resolve(true);
    });

    await worker.publish({ timer: "data" }, { repeat: "100 milliseconds" });

    await Promise.all([
      listener.promise,
      drained,
      retrier.promise,
      drainedRetries,
    ]);

    const res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 1);
    const row = Object.assign({}, res.rows[0]);
    // error_count will be 0 because we fail once setting it to 1, retry setting it back to 0, then run a second time keeping the 0
    t.hasStrict(row, { error_count: 0, args: { timer: "data" } });
    t.not(row.repeat_every, null);
  });

  t.test("can unpublish a job", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const [job] = await worker.publish({ some: "data" });

    let res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 1);
    t.equal(res.rows[0].id, job);

    await worker.unpublish(job);
    res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can bulk unpublish jobs", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const jobs = await worker.publish([{ some: "data" }, { some: "data" }]);
    t.equal(jobs.length, 2);

    let res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 2);
    t.equal(res.rows[0].id, jobs[0]);
    t.equal(res.rows[1].id, jobs[1]);

    await worker.unpublish(jobs);
    res = await db.query("SELECT * FROM test_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("returns 200 on healthcheck when connected", async (t) => {
    const worker = new Porker({ connection, queue: "test", healthcheckPort: 4500 });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();
    await worker.subscribe(() => {});

    // why this throws even for a 200, i have no idea
    try {
      await get(`http://localhost:${worker.healthcheckPort}`);
      t.fail("this should not be reachable");
    } catch (err) {
      t.equal(/** @type {Error & { statusCode: number }} */ (err).statusCode, 200);
    }
  });

  t.test("does not listen for healthchecks by default", async (t) => {
    const worker = new Porker({ connection, queue: "test" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();
    await worker.subscribe(() => {});

    t.equal(worker.healthcheckPort, null);
  });

  t.end();
});
