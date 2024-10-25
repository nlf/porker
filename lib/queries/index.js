"use strict";

const { EventEmitter } = require("node:events");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const { escapeIdentifier, Client, Pool } = require("pg");

const { Deferred } = require("../util");

/** @import { Job, JobWithRuns, PorkerSettings, PublishOptions, ScheduledJob, Status, TableDefinitions } from "../" */
/** @import { PoolClient, QueryResult } from "pg" */

const queries = {
  createTables: read("./createTables.sql"),
  dropTables: read("./dropTables.sql"),

  listen: read("./listen.sql"),
  unlisten: read("./unlisten.sql"),
  notify: read("./notify.sql"),

  cancelJob: read("./cancelJob.sql"),
  completeJob: read("./completeJob.sql"),
  failJob: read("./failJob.sql"),
  findFutureJob: read("./findFutureJob.sql"),
  lockJobs: read("./lockJobs.sql"),
  publishJob: read("./publishJob.sql"),
  getJobStatus: read("./getJobStatus.sql"),

  recordRun: read("./recordRun.sql"),
};

/**
 * Read a file from this directory into a string
 * @param {string} path
 * @returns {string}
 */
function read(path) {
  return readFileSync(join(__dirname, path), { encoding: "utf8" });
}

/**
 * Replaces placeholder values in all queries and returns the set
 * @param {TableDefinitions} tables
 * @returns {typeof queries}
 */
function prepare(tables) {
  const jobsTable = escapeIdentifier(tables.jobs);
  const runsTable = escapeIdentifier(tables.runs);

  const prepared = { ...queries };

  for (const [key, value] of Object.entries(prepared)) {
    const preparedValue = value
      .replaceAll("__JOBS_TABLE__", jobsTable)
      .replaceAll("__RUNS_TABLE__", runsTable);

    prepared[/** @type {keyof queries} */ (key)] = preparedValue;
  }

  return prepared;
};

class QueryClient extends EventEmitter {
  #connection;
  #client;
  #queries;
  /** @type {PoolClient | undefined} */
  #listener;
  #listening = false;
  #transaction = Promise.resolve();

  /** @param {PorkerSettings} options */
  constructor(options) {
    super();
    this.#connection = options.connection;
    this.#client = new Pool(this.#connection);
    this.#queries = prepare(options.tables);
  }

  /**
   * Run a query through a pool connection
   * @param {string} query
   * @param {unknown[]} values
   * @returns {Promise<QueryResult<unknown>>}
   */
  async #exec(query, values = []) {
    const client = await this.#client.connect();
    const res = await client.query(query, values);
    client.release();
    return res;
  }

  /**
   * End the client
   * @returns {Promise<void>}
   */
  async end() {
    await this.#transaction;

    if (this.#listener) {
      this.#listener.release(true);
    }

    await this.#client.end();
  }

  /**
   * Create the tables needed for this query client
   * @returns {Promise<void>}
   */
  async createTables() {
    const client = new Client(this.#connection);
    await client.connect();
    await client.query(this.#queries.createTables);
    await client.end();
  }

  /**
   * Drop tables created by this query client
   * @returns {Promise<void>}
   */
  async dropTables() {
    const client = new Client(this.#connection);
    await client.connect();
    await client.query(this.#queries.dropTables);
    await client.end();
  }

  /**
   * Starts listening for notifications from postgres
   * @returns {Promise<void>}
   */
  async listen() {
    if (this.#listening) {
      return;
    }

    this.#listening = true;
    this.#listener = await this.#client.connect();
    this.#listener.on("notification", () => {
      this.emit("notification");
    });

    await this.#listener.query(this.#queries.listen);
  }

  /**
   * Triggers a notification on the listener
   * @returns {Promise<void>}
   */
  async notify() {
    await this.#exec(this.#queries.notify);
  }

  /**
   * Marks a job as cancelled
   * @param {Job["id"]} id
   * @returns {Promise<void>}
   */
  async cancelJob(id) {
    await this.#exec(this.#queries.cancelJob, [id]);
  }

  /**
   * Publish a job to the relevant channel
   * @template {object} [T=object]
   * @param {string} channel
   * @param {T} args
   * @param {PublishOptions} options
   * @returns {Promise<string>}
   */
  async publishJob(channel, args, options) {
    const res = await this.#exec(this.#queries.publishJob, [channel, options.priority, options.maxRetries, options.retryDelay, args]);
    const row = /** @type {{ id: string; }} */ (res.rows[0]);
    return row.id;
  }

  /**
   * Get a job's status
   * @param {string} id
   * @returns {Promise<JobWithRuns>}
   */
  async getJobStatus(id) {
    const res = await this.#exec(this.#queries.getJobStatus, [id]);
    if (res.rowCount === 0) {
      throw new Error(`Job "${id}" not found`);
    }

    const row = /** @type {JobWithRuns} */ (res.rows[0]);
    if (row.runs.length === 1 && row.runs[0] === null) {
      row.runs.pop();
    }

    // since runs are aggregated as JSON and JSON doesn't know how to
    // decode a string into a date, we have to do it ourselves here
    for (const run of row.runs) {
      run.started_at = new Date(run.started_at);
      run.finished_at = new Date(run.finished_at);
    }

    return row;
  }

  /**
   * Wrap a function in a transaction
   * @param {(client: TransactionQueryClient) => Promise<unknown>} fn
   * @returns {Promise<void>}
   */
  async withTransaction(fn) {
    await this.#transaction;

    const deferred = new Deferred();
    this.#transaction = deferred.promise;

    const client = await this.#client.connect();
    await client.query("BEGIN");

    const transactionClient = new TransactionQueryClient(client, this.#queries);

    let error;
    let response = "COMMIT";

    try {
      await fn(transactionClient);
    } catch (err) {
      response = "ROLLBACK";
      error = err;
    }

    await client.query(response);
    client.release();

    if (error) {
      deferred.reject(error);
    } else {
      deferred.resolve(true);
    }
  }
}

class TransactionQueryClient {
  #client;
  #queries;

  /**
   * @param {PoolClient} client
   * @param {typeof queries} queries
   */
  constructor(client, queries) {
    this.#client = client;
    this.#queries = queries;
  }

  /**
   * Marks a job as having completed
   * @param {Job["id"]} id
   * @returns {Promise<void>}
   */
  async completeJob(id) {
    await this.#client.query(this.#queries.completeJob, [id]);
  }

  /**
   * Marks a job as having failed
   * @param {Job["id"]} id
   * @returns {Promise<void>}
   */
  async failJob(id) {
    await this.#client.query(this.#queries.failJob, [id]);
  }

  /**
   * Record an attempt in the runs table
   * @param {Job["id"]} id
   * @param {Date} started_at
   * @param {Status} status
   * @param {unknown} result
   * @returns {Promise<void>}
   */
  async recordRun(id, started_at, status, result) {
    await this.#client.query(this.#queries.recordRun, [id, started_at, new Date(), status, result]);
  }

  /**
   * Find an appropriate job scheduled to start in the future
   * @param {string[]} subscriberEvents
   * @param {string[]} retrierEvents
   * @returns {Promise<ScheduledJob | null>}
   */
  async findFutureJob(subscriberEvents, retrierEvents) {
    const res = await this.#client.query(this.#queries.findFutureJob, [subscriberEvents, retrierEvents]);
    if (res.rowCount === 0) {
      return null;
    }

    const row = /** @type {ScheduledJob} */ (res.rows[0]);
    return row;
  }

  /**
   * Find and lock jobs up to the specified concurrency limit
   * @param {string[]} subscriberEvents
   * @param {string[]} retrierEvents
   * @param {number} concurrency
   * @returns {Promise<Job[]>}
   */
  async lockJobs(subscriberEvents, retrierEvents, concurrency) {
    const res = await this.#client.query(this.#queries.lockJobs, [subscriberEvents, retrierEvents, concurrency]);
    return res.rows;
  }
}

module.exports = {
  QueryClient,
  TransactionQueryClient,
  prepare,
};
