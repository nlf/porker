'use strict';

// Postgres connections
exports.client = Symbol('client');

exports.worker = Symbol('worker');

exports.workerListener = Symbol('workerListener');

exports.retryWorker = Symbol('retryWorker');

exports.retryListener = Symbol('retryListener');
