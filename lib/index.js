"use strict";
/** @import { PoolConfig } from "pg" */

/**
 * @typedef {object} PorkerOptions
 * @property {string | PoolConfig} [connection]
 * @property {boolean} [retryRecurring]
 * @property {number} [maxRetries]
 * @property {string} [retryDelay]
 * @property {number} [timeout]
 * @property {number} [concurrency]
 */

/**
 * @typedef {object} PorkerSettings
 * @property {string | PoolConfig} [connection]
 * @property {boolean} retryRecurring
 * @property {number} maxRetries
 * @property {string} retryDelay
 * @property {number} timeout
 * @property {number} concurrency
 */

/**
 * @typedef {"WAITING" | "IN_PROGRESS" | "SUCCESS" | "ERROR"} Status
 */

/**
 * @template {object} [T=object]
 * @typedef {object} Job
 * @property {string} id
 * @property {Date} created_at
 * @property {Date} updated_at
 * @property {string} event
 * @property {number} priority
 * @property {Status} status
 * @property {Date} start_after
 * @property {string | null} repeat_every
 * @property {number} max_retries
 * @property {string | null} retry_delay
 * @property {T} args
 * @property {Run[]} [runs]
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
 * @property {string | null} [repeat]
 * @property {number} [maxRetries]
 * @property {string | null} [retryDelay]
 */

const { Porker } = require("./porker");

module.exports = {
  Porker,
};
