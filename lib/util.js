"use strict";
/** @import { Job, UserSubscriber } from "./" */

const { TimeoutError } = require("./errors");

/**
 * Simple replacement for Promise.defer()
 * @template {object} [T=object]
 */
class Deferred {
  /** @param {T} [meta] */
  constructor(meta = /** @type {T} */ ({})) {
    this.promise = new Promise((resolve, reject) => {
      /** @param {unknown} value */
      this.resolve = (value) => {
        this.finished = true;
        return resolve(value);
      };

      /** @param {unknown} reason */
      this.reject = (reason) => {
        this.finished = true;
        return reject(reason);
      };
    });

    this.finished = false;
    this.meta = meta;
  }
}

/**
 * Run a job handler, wrapped with a timeout
 * @param {UserSubscriber} fn
 * @param {number} timeout
 * @param {Job} job
 * @returns {Promise<unknown>}
 */
async function runWithTimeout(fn, timeout, job) {
  const deferredTimeout = new Deferred();

  const timer = setTimeout(() => {
    deferredTimeout.reject(new TimeoutError(job));
  }, timeout);

  return await Promise.race([
    (async () => {
      try {
        return await fn(job);
      } finally {
        clearTimeout(timer);
      }
    })(),
    deferredTimeout.promise,
  ]);
}

module.exports = {
  runWithTimeout,
  Deferred,
};
