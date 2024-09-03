"use strict";

/**
 * @import { PoolClient } from "pg"
 * @import { Job, PorkerOptions, PorkerSettings, PublishOptions, Status, UserSubscriber } from "./"
 */

const { EventEmitter } = require("node:events");

const Pg = require("pg");

const Sql = require("./sql");
const {
  Deferred,
} = require("./util");

class Porker extends EventEmitter {
  /** @type {PorkerSettings} */
  settings;

  #queries;
  #client;

  /** @type {Record<string, UserSubscriber>} */
  #retriers = {};

  /** @type {Record<string, UserSubscriber>} */
  #subscribers = {};
  /** @type {PoolClient | undefined} */
  #subscriptionListener;

  #stopped = false;
  #working = new Deferred();

  /** @type {NodeJS.Timeout | undefined} */
  #workTimer = undefined;

  /**
   * Create a new Porker
   * @param {PorkerOptions} options
   */
  constructor({ connection = {}, maxRetries = 1, retryDelay = "5 minutes", timeout = 15000, concurrency = 1 } = {}) {
    super();

    const connectionSettings = typeof connection === "string" ? { connectionString: connection } : connection;
    connectionSettings.min ??= 3;

    this.settings = {
      connection: connectionSettings,
      maxRetries,
      retryDelay,
      timeout,
      concurrency,
    };

    this.#working.resolve(true);
    this.#queries = Sql.queries(this.settings);

    this.#client = new Pg.Pool(connectionSettings);
  }

  /**
   * Create the underlying tables that power porker
   * @returns {Promise<void>}
   */
  async create() {
    const client = await this.#client.connect();
    await client.query(this.#queries.createTable);
    return client.release();
  }

  /**
   * Drop the tables
   * @returns {Promise<void>}
   */
  async drop() {
    const client = await this.#client.connect();
    await client.query(this.#queries.dropTable);
    return client.release();
  }

  /**
   * @template {object} [T=object]
   * @overload
   * @param {string} event
   * @param {T} jobs
   * @param {PublishOptions} [options]
   * @returns {Promise<string>}
   */

  /**
   * @template {object} [T=object]
   * @overload
   * @param {string} event
   * @param {T[]} jobs
   * @param {PublishOptions} [options]
   * @returns {Promise<string[]>}
   */
   
  /**
   * Publish one or more jobs, options are applied to all jobs provided. Returns an array of job IDs.
   * @template {object} [T=object]
   * @param {string} event
   * @param {T | T[]} jobs
   * @param {PublishOptions} [options]
   * @returns {Promise<string | string[]>}
   */
  async publish(event, jobs, { priority = 0, repeat = null } = {}) {
    const client = await this.#client.connect();

    const list = Array.isArray(jobs) ? jobs : [jobs];
    await client.query("BEGIN");
    const res = await client.query(this.#queries.insertJobs(list), [event, priority, repeat, ...list]);
    await client.query(this.#queries.notifyQueue);
    await client.query("COMMIT");
    client.release();

    const ids = res.rows.map((row) => row.id);
    if (!Array.isArray(jobs)) {
      return ids[0];
    }

    return ids;
  }

  /**
   * Unpublish one or more jobs, this is mostly useful for recurring jobs
   * @param {string | string[]} jobs
   * @returns {Promise<void>}
   */
  async unpublish(jobs) {
    const client = await this.#client.connect();
    await client.query(this.#queries.cancelJobs, [Array.isArray(jobs) ? jobs : [jobs]]);
    return client.release();
  }

  /**
   * Get the status, including all previous runs, of a job
   * @param {string} id
   * @returns {Promise<Job>}
   */
  async status(id) {
    const client = await this.#client.connect();
    const res = await client.query(this.#queries.jobStatus, [id]);
    client.release();

    if (res.rowCount === 0) {
      throw new Error("Not found");
    }

    const status = /** @type {Job} */ (res.rows[0]);
    status.runs ??= [];
    if (status.runs.length === 1 && status.runs[0] === null) {
      status.runs.pop();
    }

    for (const run of status.runs) {
      run.started_at = new Date(run.started_at);
      run.finished_at = new Date(run.finished_at);
    }

    return status;
  }

  /**
   * Add a retry handler to this porker instance
   * @param {string} event
   * @param {UserSubscriber} fn
   * @returns {Promise<void>}
   */
  async retry(event, fn) {
    if (event in this.#retriers) {
      throw new Error("A retry handler for this event has already been added to this queue");
    }

    this.#retriers[event] = fn;
    this.#work();
  }

  /**
   * Add a subscriber to this porker instance
   * @param {string} event
   * @param {UserSubscriber} fn
   * @returns {Promise<void>}
   */
  async subscribe(event, fn) {
    if (event in this.#subscribers) {
      throw new Error("A subscriber for this event has already been added to this queue");
    }

    this.#subscribers[event] = fn;
    this.#work();
  }

  /**
   * Handle a received job
   * @param {Job} job
   * @param {PoolClient} client
   * @returns {Promise<boolean>}
   */
  async #handle(job, client) {
    const handler = job.status === "WAITING"
      ? this.#subscribers[job.event]
      : this.#retriers[job.event];

    const started_at = new Date();
    let failed = false;

    const timeout = new Deferred();
    const timer = setTimeout(() => {
      failed = true;
      timeout.reject(new Error("Timed out"));
    }, this.settings.timeout);

    const wrapped = async () => {
      try {
        return await handler(job);
      } finally {
        clearTimeout(timer);
      }
    };

    const result = await Promise.race([
      wrapped(),
      timeout.promise,
    ]).catch((err) => {
      failed = true;
      if (!(err instanceof Error)) {
        return { error: err };
      }

      return {
        error: {
          message: err.message,
          ...("code" in err ? { code: err.code } : {}),
        },
      };
    });

    await client.query(this.#queries.recordRun, [job.id, started_at, new Date(), failed ? 'ERROR' : 'SUCCESS', result]);

    return failed;
  }

  /**
   * Find job(s) based on concurrency and process them
   */
  async #work() {
    await this.#working.promise;
    this.#working = new Deferred();

    if (!this.#subscriptionListener) {
      this.#subscriptionListener = await this.#client.connect();
      this.#subscriptionListener.on("notification", () => {
        this.#work();
      });

      await this.#subscriptionListener.query(this.#queries.listenPublishes);
    }

    const client = await this.#client.connect();

    let didWork = false;
    while (!this.#stopped) {
      await client.query("BEGIN");
      const subscriberEvents = Object.keys(this.#subscribers);
      const hasSubscriber = subscriberEvents.length > 0;

      const retrierEvents = Object.keys(this.#retriers);
      const hasRetrier = retrierEvents.length > 0;

      const payload = [];
      let currentJobQuery;
      let futureJobQuery;
      if (hasSubscriber && hasRetrier) {
        currentJobQuery = this.#queries.lockAllJobs;
        futureJobQuery = this.#queries.findFutureJob;
        payload.push(subscriberEvents);
        payload.push(retrierEvents);
      } else if (hasSubscriber && !hasRetrier) {
        currentJobQuery = this.#queries.lockPendingJobs;
        futureJobQuery = this.#queries.findFuturePendingJob;
        payload.push(subscriberEvents);
      } else {
        currentJobQuery = this.#queries.lockRetryJobs;
        futureJobQuery = this.#queries.findFutureRetryJob;
        payload.push(retrierEvents);
      }

      const { rows: currentJobs } = await client.query(currentJobQuery, payload);
      const futureJob = await client.query(futureJobQuery, payload);
      if (futureJob.rows.length) {
        clearTimeout(this.#workTimer);
        this.#workTimer = setTimeout(() => {
          this.#work();
        }, futureJob.rows[0].next_run - Date.now());
      }

      if (!currentJobs.length) {
        await client.query("ROLLBACK");
        break;
      }

      didWork = true;

      const jobResults = await Promise.all(currentJobs.map(async (job) => {
        return {
          id: job.id,
          repeat: job.repeat_every,
          failed: await this.#handle(job, client),
        };
      }));

      const results = jobResults.reduce((result, job) => {
        if (job.failed) {
          result.failed.push(job.id);
        } else {
          result.passed.push(job.id);
        }
        return result;
      }, {
        passed: /** @type {number[]} */ ([]),
        failed: /** @type {number[]} */ ([]),
      });

      await client.query(this.#queries.completeJobs, [results.passed]);
      await client.query(this.#queries.errorJobs, [results.failed]);

      // If we had errors, notify the retry queue
      if (results.failed.length) {
        await client.query(this.#queries.notifyQueue);
      }

      await client.query("COMMIT");
    }

    if (didWork) {
      this.emit("drain");
    }

    client.release();
    this.#working.resolve(true);
  }

  /**
   * Disconnect all clients and stop processing events
   * @returns {Promise<void>}
   */
  async end() {
    this.#stopped = true;
    await this.#working.promise;
    clearTimeout(this.#workTimer);

    if (this.#subscriptionListener) {
      this.#subscriptionListener.release();
    }

    await this.#client.end();

    this.emit("end");
  }
}

module.exports = {
  Porker,
};
