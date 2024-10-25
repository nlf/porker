"use strict";
/** @import { Job } from "../" */

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("simple job", async (t) => {
  const { db, worker, channel, args, tables } = await bootstrap(t, { maxRetries: 0 });

  /** @type {string} */
  let id;
  await t.test("can publish a job", async (t) => {
    id = await worker.publish(channel, args);
    t.type(id, "string", "should return a string id");

    const res = await db.query(`SELECT * FROM ${tables.jobs} WHERE id = $1::uuid`, [id]);
    t.equal(res.rowCount, 1, "should have created one row");

    const row = /** @type {Job} */ (res.rows[0]);
    t.hasStrict(row, {
      id,
      channel,
      priority: 0,
      status: "WAITING",
      max_retries: 0,
    });

    t.type(row.created_at, Date);
    t.type(row.updated_at, Date);
    t.type(row.start_after, Date);
    t.equal(row.retry_delay?.toPostgres(), "5 minutes");
    t.strictSame(row.args, args);

    return id;
  });

  await t.test("can lookup status on a WAITING job", async (t) => {
    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      channel,
      status: "WAITING",
    });

    t.equal(status.runs.length, 0);
  });

  await t.test("can handle a job", async (t) => {
    worker.subscribe(channel, (job) => {
      t.equal(job.status, "WAITING");
      t.strictSame(job.args, args);
      return { ...args, result: "OK" };
    });

    const status = await worker.wait(id);
    t.hasStrict(status, {
      id,
      channel,
      status: "SUCCESS",
    });

    t.equal(status.runs.length, 1);
    const run = status.runs[0];
    t.hasStrict(run, {
      job_id: id,
      status: "SUCCESS",
      result: { ...args, result: "OK" },
    });

    t.type(run.id, "string");
    t.type(run.started_at, Date);
    t.type(run.finished_at, Date);
  });

  await t.test("wait and status return the same value after completion", async (t) => {
    const wait = await worker.wait(id);
    const status = await worker.status(id);

    t.strictSame(wait, status);
  });
});
