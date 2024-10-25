"use strict";

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("failed job with non-string error", async (t) => {
  const { worker, channel, args } = await bootstrap(t, { maxRetries: 0 });

  worker.subscribe(channel, () => {
    throw "whoops";
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
    result: { error: "whoops" },
  });
});
