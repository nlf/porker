"use strict";

/**
 * @import { PoolClient } from "pg"
 * @import { PorkerOptions, PorkerSettings, PublishOptions, UserSubscriber, PorkerSubscriber } from "./"
 */

const { EventEmitter } = require("node:events");

const Pg = require("pg");

const Sql = require("./sql");
const {
  gatherResults,
  wrapSubscriber,
  Deferred,
} = require("./util");

class Porker extends EventEmitter {
  /** @type {PorkerSettings} */
  settings;

  #queries;
  #client;

  /** @type {PorkerSubscriber | undefined} */
  #retrier;

  /** @type {PorkerSubscriber | undefined} */
  #subscriber;
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
   * @param {T | T[]} jobs
   * @param {PublishOptions} options
   * @returns {Promise<number[]>}
   */
  async publish(jobs, { priority = 0, repeat = null } = {}) {
    const client = await this.#client.connect();

    const list = Array.isArray(jobs) ? jobs : [jobs];
    await client.query("BEGIN");
    const res = await client.query(this.#queries.insertJobs(list), [priority, repeat, ...list]);
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
   * Add a subscriber to this porker instance
   * @param {UserSubscriber} fn
   * @returns {Promise<void>}
   */
  async subscribe(fn) {
    if (this.#subscriber) {
      throw new Error("A subscriber has already been added to this queue");
    }

    this.#subscriber = wrapSubscriber(fn, this.settings.timeout);

    this.#work();
  }

  /**
   * Find job(s) based on concurrency and process them
   */
  async #work() {
    await this.#working.promise;
    this.#working = new Deferred();

    /* c8 ignore next 3 - no need to test, this won't run if neither is set */
    if (!this.#subscriber && !this.#retrier) {
      return;
    }

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
      const query = (this.#subscriber && this.#retrier)
        ? this.#queries.lockAllJobs
        : (this.#subscriber && !this.#retrier)
            ? this.#queries.lockPendingJobs
            : this.#queries.lockRetryJobs;

      const { rows: currentJobs } = await client.query(query);
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

      const results = await gatherResults({
        run: this.#subscriber,
        retry: this.#retrier,
        jobs: currentJobs,
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
   * Add a retry handler to this porker instance
   * @param {UserSubscriber} fn
   * @returns {Promise<void>}
   */
  async retry(fn) {
    if (this.#retrier) {
      throw new Error("A retry handler has already been added to this queue");
    }

    this.#retrier = wrapSubscriber(fn, this.settings.timeout);
    this.#work();
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
