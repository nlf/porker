"use strict";

const { once } = require("node:events");
const { setTimeout } = require("node:timers/promises");
const t = require("tap");
const { Porker } = require("../");

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
    t.ok(res.rowCount && res.rowCount > 0);
  });

  t.test("can drop its own table", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    let res = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'porker_jobs'");
    t.ok(res.rowCount && res.rowCount > 8);

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

    await worker.subscribe("event", (job) => {
      t.strictSame(job.args, { some: "data" });
    });

    const id = await worker.publish("event", { some: "data" });
    await once(worker, "drain");

    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      event: "event",
      args: { some: "data" },
      status: "SUCCESS",
    });
    t.equal(status.runs?.length, 1);
    t.hasStrict(status.runs?.[0], {
      job_id: id,
      status: "SUCCESS",
      result: null,
    });
  });

  t.test("can handle a failing job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("myevent", (job) => {
      t.strictSame(job.args, { some: "data" });
      throw new Error("Uh oh");
    });

    const id = await worker.publish("myevent", { some: "data" });
    await once(worker, "drain");

    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      event: "myevent",
      args: { some: "data" },
      status: "ERROR",
    });
    t.equal(status.runs?.length, 1);
    t.hasStrict(status.runs?.[0], {
      job_id: id,
      status: "ERROR",
      result: { error: { message: "Uh oh" } },
    });
  });

  t.test("can retry a failed job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();
    await worker.subscribe("retry_failed", (job) => {
      t.strictSame(job.args, { some: "data" });
      throw new Error("Uh oh");
    });

    const id = await worker.publish("retry_failed", { some: "data" }, { retryDelay: "10 milliseconds" });
    await once(worker, "drain");

    await worker.retry("retry_failed", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.status, "ERROR");
      return { result: "data" };
    });
    await once(worker, "drain");

    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      event: "retry_failed",
      status: "SUCCESS",
      args: { some: "data" },
    });
    t.equal(status.runs?.length, 2);
    t.hasStrict(status.runs?.[0], {
      job_id: id,
      status: "ERROR",
      result: { error: { message: "Uh oh" } },
    });
    t.hasStrict(status.runs?.[1], {
      job_id: id,
      status: "SUCCESS",
      result: { result: "data" },
    });
  });

  t.test("can retry a failed job when a worker is only a retrier", async (t) => {
    const worker = new Porker({ connection });
    const retryWorker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
      await retryWorker.end();
    });

    await worker.create();

    await worker.subscribe("retrier", (job) => {
      t.strictSame(job.args, { some: "data" });
      throw new Error("Uh oh");
    });

    const id = await worker.publish("retrier", { some: "data" }, { retryDelay: "10 milliseconds" });
    await once(worker, "drain");

    await retryWorker.retry("retrier", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.status, "ERROR");
    });
    await once(retryWorker, "drain");

    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      event: "retrier",
      args: { some: "data" },
      status: "SUCCESS",
    });
    t.equal(status.runs?.length, 2);
    t.hasStrict(status.runs?.[0], {
      job_id: id,
      status: "ERROR",
      result: { error: { message: "Uh oh" } },
    });
    t.hasStrict(status.runs?.[1], {
      job_id: id,
      status: "SUCCESS",
      result: null,
    });
  });

  t.test("can retry a failed job with a delay", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("delay", (job) => {
      t.strictSame(job.args, { some: "data" });
      throw new Error("Uh oh");
    });

    const id = await worker.publish("delay", { some: "data" }, { retryDelay: "150 milliseconds" });
    await once(worker, "drain");

    await worker.retry("delay", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.status, "ERROR");
    });
    await once(worker, "drain");

    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      event: "delay",
      args: { some: "data" },
      status: "SUCCESS",
    });
    t.equal(status.runs?.length, 2);
    t.hasStrict(status.runs?.[0], {
      job_id: id,
      status: "ERROR",
      result: { error: { message: "Uh oh" } },
    });
    t.hasStrict(status.runs?.[1], {
      job_id: id,
      status: "SUCCESS",
      result: null,
    });

    const runOneStart = status.runs?.[0].started_at;
    const runTwoStart = status.runs?.[1].started_at;
    const delay = (runOneStart && runTwoStart) && runTwoStart?.getTime() - runOneStart?.getTime();
    t.ok(delay && delay >= 148, `${delay} is above 148 (150 with fuzz)`);
  });

  t.test("can handle two jobs", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("event", (job) => {
      t.strictSame(job.args, { some: "data" });
    });

    const idOne = await worker.publish("event", { some: "data" });
    const idTwo = await worker.publish("event", { some: "data" });
    await once(worker, "drain");

    const statusOne = await worker.status(idOne);
    t.equal(statusOne.status, "SUCCESS");

    const statusTwo = await worker.status(idTwo);
    t.equal(statusTwo.status, "SUCCESS");
  });

  t.test("can retry two failed jobs", async (t) => {
    const retryDelay = "1 millisecond";
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("fail", (job) => {
      t.strictSame(job.args, { some: "data" });
      throw new Error("Uh oh");
    });

    const idOne = await worker.publish("fail", { some: "data" }, { retryDelay });
    const idTwo = await worker.publish("fail", { some: "data" }, { retryDelay });
    await once(worker, "drain");

    await worker.retry("fail", (job) => {
      t.strictSame(job.args, { some: "data" });
      t.equal(job.status, "ERROR");
    });
    await once(worker, "drain");

    const statusOne = await worker.status(idOne);
    t.equal(statusOne.status, "SUCCESS");

    const statusTwo = await worker.status(idTwo);
    t.equal(statusTwo.status, "SUCCESS");
  });

  t.test("can bulk publish jobs", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("batch", (job) => {
      t.strictSame(job.args, { some: "data" });
    });

    const ids = await worker.publish("batch", [{ some: "data" }, { some: "data" }]);
    await once(worker, "drain");

    t.equal(ids.length, 2);
    for (const id of ids) {
      const status = await worker.status(id);
      t.equal(status.status, "SUCCESS");
    }
  });

  t.test("can timeout a job", async (t) => {
    const worker = new Porker({ connection, timeout: 1 });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("timeout", async (job) => {
      t.strictSame(job.args, { some: "data" });
      await setTimeout(10);
    });

    const id = await worker.publish("timeout", { some: "data" });
    await once(worker, "drain");

    const status = await worker.status(id);
    t.equal(status.status, "ERROR");
  });

  t.test("can create a recurring job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    await worker.subscribe("recurring", (job) => {
      t.strictSame(job.args, { timer: "data" });
      t.not(job.repeat_every, null);
    });

    const id = await worker.publish("recurring", { timer: "data" }, { repeat: "100 milliseconds" });
    await once(worker, "drain");
    await once(worker, "drain");

    await worker.unpublish(id);

    const status = await worker.status(id);
    t.equal(status.status, 'SUCCESS');
    t.equal(status.runs?.length, 2);
    t.equal(status.runs?.[0].status, "SUCCESS");
    t.equal(status.runs?.[1].status, "SUCCESS");
  });

  t.test("can unpublish a job", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const id = await worker.publish("nope", { some: "data" });

    const beforeStatus = await worker.status(id);
    t.equal(beforeStatus.status, "WAITING");

    await worker.unpublish(id);

    const afterStatus = await worker.status(id);
    t.equal(afterStatus.status, "SUCCESS");
  });

  t.test("can bulk unpublish jobs", async (t) => {
    const worker = new Porker({ connection });
    t.teardown(async () => {
      await worker.end();
    });

    await worker.create();

    const jobs = await worker.publish("nope", [{ some: "data" }, { some: "data" }]);
    t.equal(jobs.length, 2);

    const beforeStatuses = await Promise.all([
      worker.status(jobs[0]),
      worker.status(jobs[1]),
    ]);

    t.strictSame(beforeStatuses.map((job) => job.status), ["WAITING", "WAITING"]);

    await worker.unpublish(jobs);

    const afterStatuses = await Promise.all([
      worker.status(jobs[0]),
      worker.status(jobs[1]),
    ]);

    t.strictSame(afterStatuses.map((job) => job.status), ["SUCCESS", "SUCCESS"]);
  });

  t.end();
});
