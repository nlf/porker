"use strict";

const t = require("tap");
const { setTimeout } = require("node:timers/promises");
const { bootstrap } = require("./shared");

t.test("timed out job", async (t) => {
  const { worker, channel, args } = await bootstrap(t, { maxRetries: 0, timeout: 150 });

  worker.subscribe(channel, async () => {
    await setTimeout(250);
    return { should_have_failed: true };
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
    result: { error: { message: `Job "${id}" timed out`, code: "E_TIMEOUT" } },
  });
});
