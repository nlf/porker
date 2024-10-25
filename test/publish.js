"use strict";

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("publish nuances", async (t) => {
  const { worker, channel, args } = await bootstrap(t, { maxRetries: 0, retryDelay: "10 milliseconds" });

  await t.test("respects worker level defaults", async (t) => {
    const id = await worker.publish(channel, args);
    const status = await worker.status(id);
    t.hasStrict(status, {
      max_retries: 0,
      retry_delay: { milliseconds: 10 },
    });
  });

  await t.test("allows overriding at publish time", async (t) => {
    const id = await worker.publish(channel, args, { maxRetries: 1, retryDelay: "50 milliseconds" });
    const status = await worker.status(id);
    t.hasStrict(status, {
      max_retries: 1,
      retry_delay: { milliseconds: 50 },
    });
  });
});
