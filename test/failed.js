"use strict";

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("failed job", async (t) => {
  const { worker, channel, args } = await bootstrap(t, { maxRetries: 0 });

  worker.subscribe(channel, () => {
    throw new Error("Uh oh");
  });

  const id = await worker.publish(channel, args);
  const status = await worker.wait(id);

  t.hasStrict(status, {
    id,
    channel,
    args,
    status: "ERROR",
  });
  t.equal(status.runs.length, 1);
  const run = status.runs[0];
  t.hasStrict(run, {
    job_id: id,
    status: "ERROR",
    result: { error: { message: "Uh oh" } },
  });

  await t.test("wait and status return the same result after error", async (t) => {
    const status = await worker.status(id);
    const wait = await worker.wait(id);
    t.strictSame(status, wait);
  });

  await t.test("wait with exhaustRetries returns the same as wait when done", async (t) => {
    const wait = await worker.wait(id);
    const waitWithRetries = await worker.wait(id, { exhaustRetries: true });
    t.strictSame(wait, waitWithRetries);
  });
});
