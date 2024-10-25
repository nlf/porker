"use strict";

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("retry job", async (t) => {
  const { worker, channel, args } = await bootstrap(t, { maxRetries: 1, retryDelay: "100 milliseconds" });

  const id = await worker.publish(channel, args);

  worker.subscribe(channel, () => {
    throw new Error("failed first run");
  });

  const firstStatus = await worker.wait(id);
  t.hasStrict(firstStatus, {
    id,
    channel,
    status: "ERROR",
  });
  t.equal(firstStatus.runs.length, 1);

  worker.retry(channel, () => {
    return { ...args, result: "OK" };
  });

  const secondStatus = await worker.wait(id, { exhaustRetries: true });
  t.hasStrict(secondStatus, {
    id,
    channel,
    status: "SUCCESS",
  });
  t.equal(secondStatus.runs.length, 2);

  const firstRun = secondStatus.runs[0];
  t.hasStrict(firstRun, {
    job_id: id,
    status: "ERROR",
    result: { error: { message: "failed first run" } },
  });

  const secondRun = secondStatus.runs[1];
  t.hasStrict(secondRun, {
    job_id: id,
    status: "SUCCESS",
    result: { ...args, result: "OK" },
  });
});
