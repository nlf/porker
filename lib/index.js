"use strict";

const EventEmitter = require("events").EventEmitter;

const Http = require("http");
const Pg = require("pg");
const Util = require("util");

const Sql = require("./sql");
const { Deferred } = require("./util");

const internals = {};
internals.timeout = Util.promisify(setTimeout);

/** @typedef {{
  id: number;
  priority: number;
  started_at: Date | null;
  repeat_every: string | null;
  error_count: number;
  args: object;
}} Job */

// we wrap the user supplied function to guarantee
// that it does not throw and returns a boolean
// representing if it failed (or timed out)
/**
 * @param {Function} fn
 * @param {number} timeout
 * @returns {(job: Job) => Promise<unknown>}
 */
internals.wrapSubscriber = (fn, timeout) => {
  /** @param {Job} job */
  return (job) => {
    const deferred = new Deferred();
    const timer = setTimeout(deferred.resolve, timeout);

    return Promise.race([
      (async () => {
        let failed = false;
        try {
          await fn(job);
        } catch (/* eslint-disable-line no-unused-vars */ err) {
          failed = true;
        } finally {
          clearTimeout(timer);
        }

        return failed;
      })(),
      (async () => {
        await deferred.promise;
        return true;
      })(),
    ]);
  };
};

/**
 * @param {Function} fn
 * @param {Job[]} jobs
 */
internals.gatherResults = async (fn, jobs) => {
  // run subscribe fn in parallel for each row
  const results = await Promise.all(jobs.map(async (job) => {
    return {
      id: job.id,
      repeat: job.repeat_every,
      failed: await fn(Object.assign({}, job)),
    };
  }));

  // gather results so we can do bulk actions
  const result = results.reduce((acc, result) => {
    if (result.failed) {
      acc.failed.push(result.id);
    } else if (result.repeat) {
      acc.reset.push(result.id);
    } else {
      acc.passed.push(result.id);
    }

    return acc;
  }, {
    passed: /** @type {number[]} */ ([]),
    reset: /** @type {number[]} */ ([]),
    failed: /** @type {number[]} */ ([]),
  });

  return result;
};

/** @typedef {{
  connection?: string | object;
  queue: string;
  errorThreshold?: number;
  retryDelay?: string;
  timeout?: number;
  concurrency?: number;
  healthcheckPort?: number | null;
}} PorkerOptions */

class Porker extends EventEmitter {
  #healthcheck;
  #queries;

  #client;

  /** @type {undefined | ((job: Job) => Promise<unknown>)} */
  #retrier = undefined;
  /** @type {Pg.PoolClient | undefined} */
  #retryListener;
  #retryWorker;

  /** @type {undefined | ((job: Job) => Promise<unknown>)} */
  #subscriber;
  /** @type {Pg.PoolClient | undefined} */
  #subscriptionListener;
  #subscriptionWorker;

  #stopped = false;

  /** @type {NodeJS.Timeout | undefined} */
  #workTimer = undefined;
  /** @type {NodeJS.Timeout | undefined} */
  #retryTimer = undefined;

  /** @param {PorkerOptions} options */
  constructor({ connection, queue, errorThreshold = 1, retryDelay = "5 minutes", timeout = 15000, concurrency = 1, healthcheckPort = null } = /** @type {PorkerOptions} */ ({})) {
    super();

    const connectionSettings = typeof connection === "string" ? { connectionString: connection } : connection;

    if (!queue) {
      throw new Error("Missing required parameter: queue");
    }

    this.queue = queue;
    this.errorThreshold = errorThreshold;
    this.retryDelay = retryDelay;
    this.timeout = timeout;
    this.concurrency = concurrency;
    this.healthcheckPort = healthcheckPort;

    this.#healthcheck = Http.createServer((req, res) => {
      res.writeHead(200);
      return res.end();
    });

    this.#queries = Sql.queries(this);

    this.#client = new Pg.Pool(connectionSettings);
    this.#retryWorker = new Pg.Pool({ max: 2, ...connectionSettings });
    this.#subscriptionWorker = new Pg.Pool({ max: 2, ...connectionSettings });

    if (this.healthcheckPort) {
      this.#healthcheck.listen(this.healthcheckPort);
    }
  }

  async create() {
    const client = await this.#client.connect();
    await client.query(this.#queries.createTable);
    return client.release();
  }

  async drop() {
    const client = await this.#client.connect();
    await client.query(this.#queries.dropTable);
    return client.release();
  }

  /**
   * @param {unknown | unknown[]} jobs
   * @param {{ priority?: number; repeat?: string }} options
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
   * @param {string | string[]} jobs
   */
  async unpublish(jobs) {
    const client = await this.#client.connect();
    await client.query(this.#queries.completeJobs, [Array.isArray(jobs) ? jobs : [jobs]]);
    return client.release();
  }

  /** @param {(job: Job) => unknown | Promise<unknown>} fn */
  async subscribe(fn) {
    if (this.#subscriber) {
      throw new Error("A subscriber has already been added to this queue");
    }

    this.#subscriber = internals.wrapSubscriber(fn, this.timeout);

    this.#subscriptionListener = await this.#subscriptionWorker.connect();
    this.#subscriptionListener.on("notification", () => {
      this.#work();
    });

    await this.#subscriptionListener.query(this.#queries.listenPublishes);

    this.emit("subscriberReady");
    this.#work();
  }

  async #work() {
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

      const results = await internals.gatherResults(/** @type {Function} */ (this.#subscriber), currentJobs.map((job) => Object.assign({}, job)));
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

  /** @param {(job: Job) => unknown | Promise<unknown>} fn */
  async retry(fn) {
    if (this.#retrier) {
      throw new Error("A retry handler has already been added to this queue");
    }

    this.#retrier = internals.wrapSubscriber(fn, this.timeout);

    this.#retryListener = await this.#retryWorker.connect();
    this.#retryListener.on("notification", () => {
      this.#retry();
    });

    await this.#retryListener.query(this.#queries.listenRetries);

    this.emit("retrierReady");
    this.#retry();
  }

  async #retry() {
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
      const results = await internals.gatherResults(/** @type {Function} */ (this.#retrier), currentJobs.map((row) => Object.assign({}, row)));

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

  async end() {
    this.#stopped = true;
    clearTimeout(this.#workTimer);
    clearTimeout(this.#retryTimer);

    if (this.healthcheckPort) {
      await new Promise((resolve) => {
        this.#healthcheck.close(resolve);
      });
    }

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

module.exports = Porker;
