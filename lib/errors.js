"use strict";
/** @import { Job } from "./" */

class TimeoutError extends Error {
  /** @param {Job} job */
  constructor(job) {
    super(`Job "${job.id}" timed out`);
    this.code = "E_TIMEOUT";
  }
}

module.exports = {
  TimeoutError,
};
