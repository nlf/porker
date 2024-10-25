"use strict";

const { EventEmitter } = require("node:events");

const { QueryClient } = require("./queries");
const {
  Deferred,
  runWithTimeout,
} = require("./util");

/**
 * @import { Job, JobWithRuns, PorkerOptions, PorkerSettings, PublishOptions, Status, UserSubscriber, WaitOptions } from "./"
 * @import { TransactionQueryClient } from "./queries"
 */

class Porker extends EventEmitter {
  #client;
  /** @type {PorkerSettings} */
  settings;
  /** @type {Set<Deferred<{ id: string; exhaustRetries: boolean; checkAfter?: Date }>>} */
  #waiters = new Set();
  /** @type {Record<string, UserSubscriber>} */
  #retriers = {};
  /** @type {Record<string, UserSubscriber>} */
  #subscribers = {};
  #stopped = false;
  #working = new Deferred();
  /** @type {NodeJS.Timeout | undefined} */
  #workTimer = undefined;

  /**
   * @param {PorkerOptions} options
   */
  constructor({ connection = {}, maxRetries = 1, retryDelay = "5 minutes", timeout = 15000, concurrency = 1, tables } = {}) {
    super();

    const connectionSettings = typeof connection === "string" ? { connectionString: connection } : connection;

    this.settings = {
      connection: connectionSettings,
      tables: tables ?? { jobs: "porker_jobs", runs: "porker_runs" },
      maxRetries,
      retryDelay,
      timeout,
      concurrency,
    };

    this.#client = new QueryClient(this.settings);
    this.#client.on("notification", () => {
      this.#working.promise = this.#working.promise.then(() => this.#work());
    });

    this.#working.resolve(true);
  }

  /**
   * Create the underlying tables that power porker
   * @returns {Promise<void>}
   */
  async create() {
    return await this.#client.createTables();
  }

  /**
   * Drop the tables
   * @returns {Promise<void>}
   */
  async drop() {
    return await this.#client.dropTables();
  }

  /**
   * Publish one or more jobs, options are applied to all jobs provided. Returns an array of job IDs.
   * @template {object} [T=object]
   * @param {string} channel
   * @param {T} args
   * @param {PublishOptions} [options]
   * @returns {Promise<string>}
   */
  async publish(channel, args, { priority = 0, maxRetries, retryDelay } = {}) {
    const realRetryDelay = retryDelay ?? this.settings.retryDelay;
    const realMaxRetries = maxRetries ?? this.settings.maxRetries;

    const id = await this.#client.publishJob(channel, args, {
      priority,
      maxRetries: realMaxRetries,
      retryDelay: realRetryDelay,
    });

    await this.#client.notify();
    return id;
  }

  /**
   * Cancel a job
   * @param {string} id
   * @returns {Promise<void>}
   */
  async cancel(id) {
    return await this.#client.cancelJob(id);
  }

  /**
   * Get the status, including all previous runs, of a job
   * @param {string} id
   * @returns {Promise<JobWithRuns>}
   */
  async status(id) {
    return await this.#client.getJobStatus(id);
  }

  /**
   * Wait for a job to finish, return its status when it does
   * @param {string} id
   * @param {WaitOptions} options
   * @returns {Promise<JobWithRuns>}
   */
  async wait(id, { exhaustRetries = false } = {}) {
    const deferred = new Deferred({ id, exhaustRetries });
    this.#waiters.add(deferred);
    this.#checkWaiters();
    return deferred.promise;
  }

  /**
   * Add a retry handler to this porker instance
   * @param {string} channel
   * @param {UserSubscriber} fn
   * @returns {void}
   */
  retry(channel, fn) {
    if (channel in this.#retriers) {
      throw new Error("A retry handler for this channel has already been added");
    }

    this.#retriers[channel] = fn;
    this.#working.promise = this.#working.promise.then(() => this.#work());
  }

  /**
   * Add a subscriber to this porker instance
   * @param {string} channel
   * @param {UserSubscriber} fn
   * @returns {void}
   */
  subscribe(channel, fn) {
    if (channel in this.#subscribers) {
      throw new Error("A subscriber for this channel has already been added");
    }

    this.#subscribers[channel] = fn;
    this.#working.promise = this.#working.promise.then(() => this.#work());
  }

  /**
   * Handle a received job
   * @param {Job} job
   * @param {TransactionQueryClient} client
   * @returns {Promise<boolean>}
   */
  async #handle(job, client) {
    const handler = job.status === "WAITING"
      ? this.#subscribers[job.channel]
      : this.#retriers[job.channel];

    const started_at = new Date();

    /** @type {Status} */
    let runStatus;
    let runResult;
    try {
      runResult = await runWithTimeout(handler, this.settings.timeout, job);
      runStatus = "SUCCESS";
    } catch (err) {
      runResult = err instanceof Error
        ? {
            error: {
              message: err.message,
              ...("code" in err ? { code: err.code } : {}),
            },
          }
        : { error: err };
      runStatus = "ERROR";
    }

    await client.recordRun(job.id, started_at, runStatus, runResult);
    if (runStatus === "ERROR") {
      await client.failJob(job.id);
    } else {
      await client.completeJob(job.id);
    }

    return runStatus === "ERROR";
  }

  async #checkWaiters() {
    for (const waiter of this.#waiters.values()) {
      if (!waiter.meta.checkAfter || waiter.meta.checkAfter <= new Date()) {
        const status = await this.status(waiter.meta.id);
        if (status.status === "SUCCESS" || status.status === "CANCELLED") {
          this.#waiters.delete(waiter);
          waiter.resolve(status);
          continue;
        }

        if (status.status === "ERROR") {
          if (!waiter.meta.exhaustRetries) {
            this.#waiters.delete(waiter);
            waiter.resolve(status);
            continue;
          }

          const errorCount = status.runs.filter((run) => run.status === "ERROR").length;
          if (status.max_retries < errorCount) {
            this.#waiters.delete(waiter);
            waiter.resolve(status);
            continue;
          }
        }

        waiter.meta.checkAfter = status.start_after;
      }
    }
  }

  /**
   * Find job(s) based on concurrency and process them
   */
  async #work() {
    if (!this.#working.finished) {
      return;
    }

    this.#working = new Deferred();

    await this.#client.listen();

    let finished = false;
    let didWork = false;
    let shouldNotify = false;

    while (!finished && !this.#stopped) {
      await this.#client.withTransaction(async (client) => {
        const subscriberEvents = Object.keys(this.#subscribers);
        const retrierEvents = Object.keys(this.#retriers);

        const currentJobs = await client.lockJobs(subscriberEvents, retrierEvents, this.settings.concurrency);
        const futureJob = await client.findFutureJob(subscriberEvents, retrierEvents);

        const currentJobIds = currentJobs.map((job) => job.id);

        let nextWait = futureJob ? futureJob.start_after.getTime() - Date.now() : null;
        for (const waiter of this.#waiters.values()) {
          if (currentJobIds.includes(waiter.meta.id)) {
            continue;
          }

          const thisWait = waiter.meta.checkAfter ? waiter.meta.checkAfter.getTime() - Date.now() : 0;
          if (!nextWait || thisWait < nextWait) {
            nextWait = thisWait;
          }
        }

        if (nextWait !== null) {
          clearTimeout(this.#workTimer);
          this.#workTimer = setTimeout(() => {
            this.#working.promise = this.#working.promise.then(() => this.#work());
          }, nextWait < 0 ? 0 : nextWait);
        }

        if (!currentJobs.length) {
          finished = true;
          return;
        }

        didWork = true;

        const jobResults = await Promise.all(currentJobs.map(/** @param {Job} job */ async (job) => {
          return {
            id: job.id,
            failed: await this.#handle(job, client),
          };
        }));

        if (jobResults.some((job) => job.failed)) {
          shouldNotify = true;
        }
      });
    }

    if (didWork) {
      this.emit("drain");
    }

    if (shouldNotify) {
      await this.#client.notify();
    }

    await this.#checkWaiters();

    setTimeout(() => {
      this.#working.resolve(true);
    });
  }

  /**
   * Disconnect all clients and stop processing events
   * @returns {Promise<void>}
   */
  async end() {
    this.#stopped = true;
    await this.#working.promise;

    clearTimeout(this.#workTimer);

    await this.#client.end();

    this.emit("end");
  }
}

module.exports = {
  Porker,
};
