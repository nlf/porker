"use strict";

const { once } = require("node:events");
const { setTimeout } = require("node:timers/promises");
const t = require("tap");
const { Porker } = require("../");
const { Deferred } = require("../lib/util");

const Pg = require("pg");

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
      new Porker({ connection: "postgres://localhost/porker-test" });
    });

    t.end();
  });

  t.test("throws when a subscriber is added twice", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("event", async () => {});
    await t.rejects(worker.subscribe("event", async () => {}), /A subscriber for this event has already been added to this queue/);
  });

  t.test("throws when retrier is added twice", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.retry("event", async () => {});
    await t.rejects(worker.retry("event", async () => {}), /A retry handler for this event has already been added to this queue/);
  });

  t.test("can create its own table", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'porker_jobs'");
    t.equal(res.rowCount, 8);

    const rows = res.rows.reduce((acc, row) => [...acc, row.column_name], []);
    t.strictSame(rows, ["id", "priority", "started_at", "repeat_every", "error_count", "args", "retry_at", "event"]);
  });

  t.test("can drop its own table", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    let res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'porker_jobs'");
    t.equal(res.rowCount, 8);

    const rows = res.rows.reduce((acc, row) => [...acc, row.column_name], []);
    t.strictSame(rows, ["id", "priority", "started_at", "repeat_every", "error_count", "args", "retry_at", "event"]);

    await worker.drop();
    res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'porker_jobs'");
    t.equal(res.rowCount, 0);
  });

  t.test("can end without a client connection", async (t) => {
    let worker = new Porker({ connection });

    await worker.create();
    await worker.end();

    worker = new Porker({ connection });
    await worker.subscribe("event", () => {});

    await t.resolves(worker.end());
  });

  t.test("can handle a single job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish("event", { some: "data" });

    const listener = new Deferred();

    await worker.subscribe("event", (job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle a failing job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const [id] = await worker.publish("myevent", { some: "data" });

    const listener = new Deferred();

    await worker.subscribe("myevent", (job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 1);
    t.equal(res.rows[0].id, id);
    t.equal(res.rows[0].error_count, 1);
    t.ok(res.rows[0].retry_at > new Date());
  });

  t.test("can retry a failed job", async (t) => {
    const worker = new Porker({ connection, retryDelay: "10 milliseconds" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe("retry_failed", (job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    await worker.retry("retry_failed", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.error_count, 1);
      retrier.resolve(true);
    });

    await worker.publish("retry_failed", { some: "data" });

    await Promise.all([
      listener.promise,
      retrier.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can retry a failed job when a worker is only a retrier", async (t) => {
    const worker = new Porker({ connection, retryDelay: "10 milliseconds" });
    const retryWorker = new Porker({ connection, retryDelay: "10 milliseconds" });
    t.teardown(async () => {
      await worker.end();
      await retryWorker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe("retrier", (job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    const retryDrained = once(retryWorker, "drain");

    await retryWorker.retry("retrier", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.error_count, 1);
      retrier.resolve(true);
    });

    await worker.publish("retrier", { some: "data" });

    await Promise.all([
      listener.promise,
      retrier.promise,
      drained,
      retryDrained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can retry a failed job with a delay", async (t) => {
    const worker = new Porker({ connection, retryDelay: "150 milliseconds" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe("delay", (job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    await worker.retry("delay", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.error_count, 1);
      retrier.resolve(true);
    });

    await worker.publish("delay", { some: "data" });

    await Promise.all([
      listener.promise,
      retrier.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle two jobs", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish("event", { some: "data" });
    await worker.publish("event", { some: "data" });

    const listener = new Deferred();

    let count = 0;
    await worker.subscribe("event", (job) => {
      t.strictSame(job.args, { some: "data" });
      if (++count === 2) {
        listener.resolve(true);
      }
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can retry two failed jobs", async (t) => {
    const worker = new Porker({ connection, retryDelay: "1 millisecond" });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish("fail", { some: "data" });
    await worker.publish("fail", { some: "data" });

    const listener = new Deferred();

    let listenerCount = 0;
    await worker.subscribe("fail", (job) => {
      t.strictSame(job.args, { some: "data" });
      if (++listenerCount === 2) {
        listener.resolve(true);
      }

      throw new Error("Uh oh");
    });

    const retrier = new Deferred();

    let retrierCount = 0;
    await worker.retry("fail", (job) => {
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
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can bulk publish jobs", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    await worker.publish("batch", [{ some: "data" }, { some: "data" }]);

    const listener = new Deferred();

    let count = 0;
    await worker.subscribe("batch", (job) => {
      t.strictSame(job.args, { some: "data" });
      if (++count === 2) {
        listener.resolve(true);
      }
    });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle a publish after a subscription", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe("event", (job) => {
      t.strictSame(job.args, { some: "data" });
      listener.resolve(true);
    });

    await worker.publish("event", { some: "data" });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can handle two publishes after a subscription", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();
    const eventOne = new Deferred();
    const eventTwo = new Deferred();
    const events = [eventOne, eventTwo];

    await worker.subscribe("event", (job) => {
      t.strictSame(job.args, { some: "data" });
      if (events.length) {
        const event = /** @type {Deferred} */ (events.shift());
        event.resolve(true);
      }
    });

    await worker.publish("event", { some: "data" });
    await eventOne.promise;
    await once(worker, "drain");

    await worker.publish("event", { some: "data" });
    await eventTwo.promise;
    await once(worker, "drain");

    const res = await db.query("SELECT * from porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can timeout a job", async (t) => {
    const worker = new Porker({ connection, timeout: 1 });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const drained = new Promise((resolve) => {
      worker.once("drain", resolve);
    });

    const listener = new Deferred();

    await worker.subscribe("timeout", async (job) => {
      t.strictSame(job.args, { some: "data" });
      await setTimeout(10);
      listener.resolve(true);
    });

    await worker.publish("timeout", { some: "data" });

    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 1);
    const row = Object.assign({}, res.rows[0]);
    t.hasStrict(row, { error_count: 1, args: { some: "data" } });
  });

  t.test("can create a recurring job", async (t) => {
    const worker = new Porker({ connection });
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
    await worker.subscribe("recurring", (job) => {
      t.strictSame(job.args, { timer: "data" });
      t.not(job.repeat_every, null);
      if (++count === 2) {
        listener.resolve(true);
      }
    });

    await worker.publish("recurring", { timer: "data" }, { repeat: "100 milliseconds" });
    await Promise.all([
      listener.promise,
      drained,
    ]);

    const res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 1);
    const row = Object.assign({}, res.rows[0]);
    t.hasStrict(row, { error_count: 0, args: { timer: "data" } });
    t.not(row.repeat_every, null);
  });

  t.test("can retry a failed recurring job and reset it", async (t) => {
    const worker = new Porker({ connection, retryDelay: "10 milliseconds" });
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

    const listener = new Deferred();

    let count = 0;
    // should fire twice
    await worker.subscribe("fail", (job) => {
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
    await worker.retry("fail", (job) => {
      t.strictSame(job.args, { timer: "data" });
      t.not(job.repeat_every, null);
      retrier.resolve(true);
    });

    await worker.publish("fail", { timer: "data" }, { repeat: "100 milliseconds" });

    await Promise.all([
      listener.promise,
      drained,
      retrier.promise,
    ]);

    const res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 1);
    const row = Object.assign({}, res.rows[0]);
    // error_count will be 0 because we fail once setting it to 1, retry setting it back to 0, then run a second time keeping the 0
    t.hasStrict(row, { error_count: 0, args: { timer: "data" } });
    t.not(row.repeat_every, null);
  });

  t.test("can unpublish a job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const [job] = await worker.publish("nope", { some: "data" });

    let res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 1);
    t.equal(res.rows[0].id, job);

    await worker.unpublish(job);
    res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.test("can bulk unpublish jobs", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const jobs = await worker.publish("nope", [{ some: "data" }, { some: "data" }]);
    t.equal(jobs.length, 2);

    let res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 2);
    t.equal(res.rows[0].id, jobs[0]);
    t.equal(res.rows[1].id, jobs[1]);

    await worker.unpublish(jobs);
    res = await db.query("SELECT * FROM porker_jobs");
    t.equal(res.rowCount, 0);
  });

  t.end();
});
