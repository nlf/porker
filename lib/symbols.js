'use strict';

// Mutexes
exports.connectMutex = Symbol('connectMutex');
exports.workerMutex = Symbol('workerMutex');
exports.retryWorkerMutex = Symbol('retryWorkerMutex');

// Postgres connections
exports.client = Symbol('client');
exports.worker = Symbol('worker');
exports.retryWorker = Symbol('retryWorker');

// Subscription methods
exports.subscriber = Symbol('subscriber');
exports.retrier = Symbol('retrier');

// Healthcheck
exports.healthcheck = Symbol('healthcheck');

// Private methods
exports.work = Symbol('work');
exports.scheduleWork = Symbol('scheduleWork');
exports.retry = Symbol('retry');
exports.scheduleRetry = Symbol('scheduleRetry');

// Private flags + properties
exports.stopped = Symbol('stopped');
exports.workTimer = Symbol('workTimer');
exports.retryTimer = Symbol('retryTimer');
exports.queries = Symbol('queries');
