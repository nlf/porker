"use strict";

/**
 * @import { PoolClient } from "pg"
 * @import { Job, PorkerOptions, PorkerSettings, PublishOptions, UserSubscriber } from "./"
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
   * Publish one or more jobs, options are applied to all jobs provided. Returns an array of job IDs.
   * @template {object} [T=object]
   * @param {string} event
   * @param {T | T[]} jobs
   * @param {PublishOptions} options
   * @returns {Promise<number[]>}
   */
  async publish(event, jobs, { priority = 0, repeat = null } = {}) {
    const client = await this.#client.connect();

    const list = Array.isArray(jobs) ? jobs : [jobs];
    await client.query("BEGIN");
    const res = await client.query(this.#queries.insertJobs(list), [event, priority, repeat, ...list]);
    await client.query(this.#queries.notifyQueue);
    await client.query("COMMIT");
    client.release();

    return res.rows.map((row) => {
      return row.id;
    });
  }

  /**
   * Unpublish one or more jobs, this is mostly useful for recurring jobs
   * @param {number | number[]} jobs
   * @returns {Promise<void>}
   */
  async unpublish(jobs) {
    const client = await this.#client.connect();
    await client.query(this.#queries.completeJobs, [Array.isArray(jobs) ? jobs : [jobs]]);
    return client.release();
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
   * @returns {Promise<boolean>}
   */
  async #handle(job) {
    const handler = job.error_count === 0
      ? this.#subscribers[job.event]
      : this.#retriers[job.event];

    const timeout = new Deferred();
    const timer = setTimeout(() => timeout.resolve(true), this.settings.timeout);
    const wrapped = async () => {
      let result;
      try {
        await handler(job);
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
      timeout.promise,
    ]);
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

      const query = (hasSubscriber && hasRetrier)
        ? this.#queries.lockAllJobs
        : (hasSubscriber && !hasRetrier)
            ? this.#queries.lockPendingJobs
            : this.#queries.lockRetryJobs;

      const payload = (hasSubscriber && hasRetrier)
        ? [subscriberEvents, retrierEvents]
        : (hasSubscriber && !hasRetrier)
            ? [subscriberEvents]
            : [retrierEvents];

      const { rows: currentJobs } = await client.query(query, payload);
      const futureJob = await client.query(this.#queries.findFutureJob);
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
          failed: await this.#handle(job),
        };
      }));

      const results = jobResults.reduce((result, job) => {
        if (job.failed) {
          result.failed.push(job.id);
        } else if (job.repeat) {
          result.reset.push(job.id);
        } else {
          result.passed.push(job.id);
        }
        return result;
      }, {
        passed: /** @type {number[]} */ ([]),
        failed: /** @type {number[]} */ ([]),
        reset: /** @type {number[]} */ ([]),
      });

      await client.query(this.#queries.completeJobs, [results.passed]);
      await client.query(this.#queries.errorJobs, [results.failed]);
      await client.query(this.#queries.resetJobs, [results.reset]);

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
