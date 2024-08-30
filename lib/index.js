"use strict";
/** @import { PoolConfig } from "pg" */

/**
 * @typedef {object} PorkerOptions
 * @property {string | PoolConfig} [connection]
 * @property {number} [maxRetries]
 * @property {string} [retryDelay]
 * @property {number} [timeout]
 * @property {number} [concurrency]
 */

/**
 * @typedef {object} PorkerSettings
 * @property {string | PoolConfig} [connection]
 * @property {number} maxRetries
 * @property {string} retryDelay
 * @property {number} timeout
 * @property {number} concurrency
 */

/**
 * @template {object} [T=object]
 * @typedef {object} Job
 * @property {number} id
 * @property {string} event
 * @property {number} priority
 * @property {Date | null} started_at
 * @property {string | null} repeat_every
 * @property {number} error_count
 * @property {T} args
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
 */

const { Porker } = require("./porker");

module.exports = {
  Porker,
};
