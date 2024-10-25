"use strict";

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("cancel job", async (t) => {
  const { worker, channel, args } = await bootstrap(t, { maxRetries: 0 });

  const id = await worker.publish(channel, args);

  await t.test("initial status is WAITING", async (t) => {
    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      channel,
      status: "WAITING",
    });

    t.equal(status.runs.length, 0);
  });

  await worker.cancel(id);

  await t.test("new status is CANCELLED", async (t) => {
    const status = await worker.status(id);
    t.hasStrict(status, {
      id,
      channel,
      status: "CANCELLED",
    });

    t.equal(status.runs.length, 0);
  });

  await t.test("can wait for a cancelled job", async (t) => {
    const wait = await worker.wait(id);
    t.hasStrict(wait, {
      id,
      channel,
      status: "CANCELLED",
    });

    t.equal(wait.runs.length, 0);
  });
});
