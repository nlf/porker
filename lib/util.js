"use strict";
/** @import { Job, UserSubscriber, PorkerSubscriber } from "./" */

/**
 * Simple replacement for Promise.defer()
 */
class Deferred {
  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

/**
 * @typedef {object} GatherResultsOptions
 * @property {PorkerSubscriber} [run]
 * @property {PorkerSubscriber} [retry]
 * @property {Job[]} jobs
 */

/**
 * Given a function and a set of jobs, run the function for each job and return a batch of results
 * @param {GatherResultsOptions} options
 * @returns {Promise<{ passed: number[]; failed: number[]; reset: number[]; }>}
 */
async function gatherResults({ run, retry, jobs }) {
  // first, run the fn in parallel for every job
  const results = await Promise.all(jobs.map(async (job) => {
    return {
      id: job.id,
      repeat: job.repeat_every,
      failed: job.error_count === 0
        ? await /** @type {PorkerSubscriber} */ (run)({ ...job })
        : await /** @type {PorkerSubscriber} */ (retry)({ ...job }),
    };
  }));

  return results.reduce((result, run) => {
    if (run.failed) {
      result.failed.push(run.id);
    } else if (run.repeat) {
      result.reset.push(run.id);
    } else {
      result.passed.push(run.id);
    }

    return result;
  }, {
    passed: /** @type {number[]} */ ([]),
    failed: /** @type {number[]} */ ([]),
    reset: /** @type {number[]} */ ([]),
  });
}

/**
 * Wrap a subscriber function such that it has timeouts and returns a boolean indicating success
 * @param {UserSubscriber} fn
 * @param {number} timeout
 * @returns {PorkerSubscriber}
 */
function wrapSubscriber(fn, timeout) {
  return async (job) => {
    const deferred = new Deferred();
    const timer = setTimeout(() => deferred.resolve(true), timeout);

    const wrapped = async () => {
      let result;
      try {
        await fn(job);
        result = false;
      } catch (/* eslint-disable-line no-unused-vars */ err) {
        result = true;
      } finally {
        clearTimeout(timer);
      }

      return result;
    };

    return await Promise.race([
      wrapped(),
      deferred.promise,
    ]);
  };
}

module.exports = {
  Deferred,
  gatherResults,
  wrapSubscriber,
};
