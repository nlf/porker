'use strict';

// Mutexes
exports.connectMutex = Symbol('connectMutex');
exports.workerMutex = Symbol('workerMutex');
exports.retryWorkerMutex = Symbol('retryWorkerMutex');
exports.subscriberMutex = Symbol('subscriberMutex');
exports.subscriberResolver = Symbol('subscriberResolver');
exports.retrierMutex = Symbol('retrierMutex');
exports.retrierResolver = Symbol('retrierResolver');

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
exports.retry = Symbol('retry');

// Private flags + properties
exports.workTimer = Symbol('workTimer');
exports.retryTimer = Symbol('retryTimer');
exports.queries = Symbol('queries');
exports.connection = Symbol('connection');
exports.stopped = Symbol('stopped');
