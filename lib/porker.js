"use strict";

/**
 * @import { PoolClient } from "pg"
 * @import { Job, PorkerOptions, PorkerSettings, PublishOptions, UserSubscriber } from "./"
 */

const { EventEmitter } = require("node:events");

const Pg = require("pg");

const Queries = require("./queries");

const {
  Deferred,
} = require("./util");

class Porker extends EventEmitter {
  /** @type {PorkerSettings} */
  settings;

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
  constructor({ connection = {}, retryRecurring = false, maxRetries = 1, retryDelay = "5 minutes", timeout = 15000, concurrency = 1 } = {}) {
    super();

    const connectionSettings = typeof connection === "string" ? { connectionString: connection } : connection;
    connectionSettings.min ??= 3;

    this.settings = {
      connection: connectionSettings,
      retryRecurring,
      maxRetries,
      retryDelay,
      timeout,
      concurrency,
    };

    this.#working.resolve(true);

    this.#client = new Pg.Pool(connectionSettings);
  }

  /**
   * Create the underlying tables that power porker
   * @returns {Promise<void>}
   */
  async create() {
    const client = await this.#client.connect();
    await client.query(Queries.createTables);
    return client.release();
  }

  /**
   * Drop the tables
   * @returns {Promise<void>}
   */
  async drop() {
    const client = await this.#client.connect();
    await client.query(Queries.dropTables);
    return client.release();
  }

  /**
   * Publish one or more jobs, options are applied to all jobs provided. Returns an array of job IDs.
   * @template {object} [T=object]
   * @param {string} channel
   * @param {T} args
   * @param {PublishOptions} [options]
   * @returns {Promise<string>}
   */
  async publish(channel, args, { priority = 0, repeat = null, maxRetries, retryDelay } = {}) {
    const realRetryDelay = retryDelay ?? this.settings.retryDelay;
    const realMaxRetries = maxRetries ?? (repeat
      ? this.settings.retryRecurring
        ? this.settings.maxRetries
        : 0
      : this.settings.maxRetries);

    const client = await this.#client.connect();

    await client.query("BEGIN");
    const res = await client.query(Queries.publishJob, [channel, priority, repeat, realMaxRetries, realRetryDelay, args]);
    await client.query(Queries.notify);
    await client.query("COMMIT");
    client.release();

    const ids = res.rows.map((row) => row.id);
    return ids[0];
  }

  /**
   * Cancel a job
   * @param {string} id
   * @returns {Promise<void>}
   */
  async cancel(id) {
    const client = await this.#client.connect();
    await client.query(Queries.cancelJob, [id]);
    return client.release();
  }

  /**
   * Get the status, including all previous runs, of a job
   * @param {string} id
   * @returns {Promise<Job>}
   */
  async status(id) {
    const client = await this.#client.connect();
    const res = await client.query(Queries.getJobStatus, [id]);
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
   * @param {string} channel
   * @param {UserSubscriber} fn
   * @returns {Promise<void>}
   */
  async retry(channel, fn) {
    if (channel in this.#retriers) {
      throw new Error("A retry handler for this channel has already been added to this queue");
    }

    this.#retriers[channel] = fn;
    this.#work();
  }

  /**
   * Add a subscriber to this porker instance
   * @param {string} channel
   * @param {UserSubscriber} fn
   * @returns {Promise<void>}
   */
  async subscribe(channel, fn) {
    if (channel in this.#subscribers) {
      throw new Error("A subscriber for this channel has already been added to this queue");
    }

    this.#subscribers[channel] = fn;
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
      ? this.#subscribers[job.channel]
      : this.#retriers[job.channel];

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

    await client.query(Queries.recordRun, [job.id, started_at, new Date(), failed ? "ERROR" : "SUCCESS", result]);
    await client.query(failed ? Queries.failJob : Queries.completeJob, [job.id]);

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

      await this.#subscriptionListener.query(Queries.listen);
    }

    const client = await this.#client.connect();

    let didWork = false;
    while (!this.#stopped) {
      await client.query("BEGIN");
      const subscriberEvents = Object.keys(this.#subscribers);
      const retrierEvents = Object.keys(this.#retriers);

      const { rows: currentJobs } = await client.query(Queries.lockJobs, [subscriberEvents, retrierEvents, this.settings.concurrency]);
      const futureJob = await client.query(Queries.findFutureJob, [subscriberEvents, retrierEvents]);

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

      if (jobResults.some((job) => job.failed)) {
        await client.query(Queries.notify);
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
