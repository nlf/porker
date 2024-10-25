"use strict";
/** @import { PoolConfig } from "pg" */
/** @import { IPostgresInterval as PostgresInterval } from "postgres-interval" */

/**
 * @typedef {object} PorkerOptions
 * @property {string | PoolConfig} [connection]
 * @property {TableDefinitions} [tables]
 * @property {number} [maxRetries]
 * @property {string} [retryDelay]
 * @property {number} [timeout]
 * @property {number} [concurrency]
 */

/**
 * @typedef {object} TableDefinitions
 * @property {string} jobs
 * @property {string} runs
 */

/**
 * @typedef {object} PorkerSettings
 * @property {PoolConfig} connection
 * @property {TableDefinitions} tables
 * @property {number} maxRetries
 * @property {string} retryDelay
 * @property {number} timeout
 * @property {number} concurrency
 */

/**
 * @typedef {"WAITING" | "IN_PROGRESS" | "SUCCESS" | "ERROR" | "CANCELLED"} Status
 */

/**
 * @typedef {object} ScheduledJob
 * @property {string} id
 * @property {Date} start_after
 */

/**
 * @template {object} [T=object]
 * @typedef {object} Job
 * @property {string} id
 * @property {Date} created_at
 * @property {Date} updated_at
 * @property {string} channel
 * @property {number} priority
 * @property {Status} status
 * @property {Date} start_after
 * @property {number} max_retries
 * @property {PostgresInterval | null} retry_delay
 * @property {T} args
 */

/**
 * @template {object} [T=object]
 * @typedef {object} JobWithRuns
 * @property {string} id
 * @property {Date} created_at
 * @property {Date} updated_at
 * @property {string} channel
 * @property {number} priority
 * @property {Status} status
 * @property {Date} start_after
 * @property {number} max_retries
 * @property {PostgresInterval | null} retry_delay
 * @property {T} args
 * @property {Run[]} runs
 */

/**
 * @template {object} [T=object]
 * @typedef {object} Run
 * @property {string} id
 * @property {string} job_id
 * @property {Date} started_at
 * @property {Date} finished_at
 * @property {Status} status
 * @property {T} result
 */

/**
 * @template {unknown} [T=unknown]
 * @callback UserSubscriber
 * @param {Job} job
 * @returns {T | Promise<T>}
 */

/**
 * @callback PorkerSubscriber
 * @param {Job} job
 * @returns {Promise<boolean>}
 */

/**
 * @typedef {object} PublishOptions
 * @property {number} [priority]
 * @property {number} [maxRetries]
 * @property {string | null} [retryDelay]
 */

/**
 * @typedef {object} WaitOptions
 * @property {boolean} [exhaustRetries]
 */

const { Porker } = require("./porker");

module.exports = {
  Porker,
};
