"use strict";

const t = require("tap");
const { bootstrap } = require("./shared");

t.test("errors", async (t) => {
  const { worker, channel } = await bootstrap(t);

  await t.test("adding a subscriber twice errors", async (t) => {
    worker.subscribe(channel, () => {});
    t.throws(() => worker.subscribe(channel, () => {}), /already been added/);
  });

  await t.test("adding a retrier twice errors", async (t) => {
    worker.retry(channel, () => {});
    t.throws(() => worker.retry(channel, () => {}), /already been added/);
  });

  await t.test("getting status for a non-existant job errors", async (t) => {
    await t.rejects(worker.status("0256E2E4-E015-44E8-9DDA-2D4A6A7A9464"), /not found/);
  });
});
