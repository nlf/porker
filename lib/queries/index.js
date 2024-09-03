const { readFileSync } = require("node:fs");
const { join } = require("node:path");

/**
 * Read a file from this directory into a string
 * @param {string} path
 * @returns {string}
 */
const read = (path) => readFileSync(join(__dirname, path), { encoding: "utf8" });

module.exports = {
  createTables: read("./createTables.sql"),
  dropTables: read("./dropTables.sql"),

  listen: read("./listen.sql"),
  notify: read("./notify.sql"),

  cancelJob: read("./cancelJob.sql"),
  completeJob: read("./completeJob.sql"),
  failJob: read("./failJob.sql"),
  findFutureJob: read("./findFutureJob.sql"),
  lockJobs: read("./lockJobs.sql"),
  publishJob: read("./publishJob.sql"),
  getJobStatus: read("./getJobStatus.sql"),

  recordRun: read("./recordRun.sql"),
};
