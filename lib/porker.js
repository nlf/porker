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
} = require("./util");

class Porker extends EventEmitter {
  /** @type {PorkerSettings} */
  settings;

  #queries;
  #client;

  /** @type {PorkerSubscriber | undefined} */
  #retrier;
  /** @type {PoolClient | undefined} */
  #retryListener;
  #retryWorker;

  /** @type {PorkerSubscriber | undefined} */
  #subscriber;
  /** @type {PoolClient | undefined} */
  #subscriptionListener;
  #subscriptionWorker;

  #stopped = false;

  /** @type {NodeJS.Timeout | undefined} */
  #workTimer = undefined;
  /** @type {NodeJS.Timeout | undefined} */
  #retryTimer = undefined;

  /**
   * Create a new Porker
   * @param {PorkerOptions} options
   */
  constructor({ connection, maxRetries = 1, retryDelay = "5 minutes", timeout = 15000, concurrency = 1 } = {}) {
    super();

    const connectionSettings = typeof connection === "string" ? { connectionString: connection } : connection;
    this.settings = {
      connection: connectionSettings,
      maxRetries,
      retryDelay,
      timeout,
      concurrency,
    };

    this.#queries = Sql.queries(this.settings);

    this.#client = new Pg.Pool(connectionSettings);
    this.#retryWorker = new Pg.Pool({ max: 2, ...connectionSettings });
    this.#subscriptionWorker = new Pg.Pool({ max: 2, ...connectionSettings });
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
  async publish(jobs, { priority = 0, repeat } = {}) {
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

    this.#subscriptionListener = await this.#subscriptionWorker.connect();
    this.#subscriptionListener.on("notification", () => {
      this.#work();
    });

    await this.#subscriptionListener.query(this.#queries.listenPublishes);

    this.emit("subscriberReady");
    this.#work();
  }

  /**
   * Find job(s) based on concurrency and process them
   */
  async #work() {
    /* c8 ignore next 3 - this method only runs if a subscriber is set */
    if (!this.#subscriber) {
      return;
    }

    const client = await this.#subscriptionWorker.connect();

    let didWork = false;
    while (!this.#stopped) {
      await client.query("BEGIN");
      const { rows: currentJobs } = await client.query(this.#queries.lockCurrentJobs);
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

      const results = await gatherResults(this.#subscriber, currentJobs.map((job) => Object.assign({}, job)));
      await client.query(this.#queries.completeJobs, [results.passed]);
      await client.query(this.#queries.errorJobs, [results.failed]);

      // If we had errors, notify the retry queue
      if (results.failed.length) {
        await client.query(this.#queries.notifyRetryQueue);
      }

      await client.query("COMMIT");
    }

    if (didWork) {
      this.emit("drain");
    }

    client.release();
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

    this.#retryListener = await this.#retryWorker.connect();
    this.#retryListener.on("notification", () => {
      this.#retry();
    });

    await this.#retryListener.query(this.#queries.listenRetries);

    this.emit("retrierReady");
    this.#retry();
  }

  /**
   * Find job(s) that need to be retried and process them
   */
  async #retry() {
    /* c8 ignore next 3 - this internal method only runs if a retrier is set */
    if (!this.#retrier) {
      return;
    }

    const client = await this.#retryWorker.connect();

    let didWork = false;
    while (!this.#stopped) {
      await client.query("BEGIN");
      const pendingJobs = await client.query(this.#queries.lockPendingRetries);

      if (pendingJobs.rowCount === 0) {
        await client.query("ROLLBACK");
        break;
      }

      const currentJobs = [];
      const futureJobs = [];
      for (const job of pendingJobs.rows) {
        if (job.retry_at <= Date.now()) {
          currentJobs.push(job);
        } else {
          futureJobs.push(job);
        }
      }

      if (futureJobs.length) {
        clearTimeout(this.#retryTimer);
        this.#retryTimer = setTimeout(() => {
          this.#retry();
        }, futureJobs[0].retry_at - Date.now());
      }

      if (!currentJobs.length) {
        await client.query("ROLLBACK");
        break;
      }

      didWork = true;
      const results = await gatherResults(this.#retrier, currentJobs.map((row) => Object.assign({}, row)));

      await client.query(this.#queries.completeJobs, [results.passed]);
      await client.query(this.#queries.resetJobs, [results.reset]);
      await client.query(this.#queries.errorJobs, [results.failed]);
      await client.query("COMMIT");
    }

    if (didWork) {
      this.emit("drainRetries");
    }

    client.release();
  }

  /**
   * Disconnect all clients and stop processing events
   * @returns {Promise<void>}
   */
  async end() {
    this.#stopped = true;
    clearTimeout(this.#workTimer);
    clearTimeout(this.#retryTimer);

    await this.#client.end();

    if (this.#subscriptionListener) {
      this.#subscriptionListener.release();
    }

    await this.#subscriptionWorker.end();

    if (this.#retryListener) {
      this.#retryListener.release();
    }

    await this.#retryWorker.end();

    this.emit("end");
  }
}

module.exports = {
  Porker,
};
