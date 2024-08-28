'use strict';

// Postgres connections
exports.client = Symbol('client');

exports.worker = Symbol('worker');

exports.workerListener = Symbol('workerListener');

exports.retryWorker = Symbol('retryWorker');

exports.retryListener = Symbol('retryListener');

// Subscription methods
exports.subscriber = Symbol('subscriber');

exports.retrier = Symbol('retrier');

// Healthcheck
exports.healthcheck = Symbol('healthcheck');
