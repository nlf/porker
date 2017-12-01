'use strict';

const EventEmitter = require('events').EventEmitter;

const Http = require('http');
const Pg = require('pg');
const Util = require('util');

const Sql = require('./sql');
const Symbols = require('./symbols');

const internals = {};
internals.timeout = Util.promisify(setTimeout);

// we wrap the user supplied function to guarantee
// that it does not throw and returns a boolean
// representing if it failed (or timed out)
internals.wrapSubscriber = (fn, timeout) => {

    return (job) => {

        return Promise.race([
            (async () => {

                try {
                    await fn(job);
                    return false;
                }
                catch (err) {
                    return true;
                }
            })(),
            (async () => {

                await internals.timeout(timeout);
                return true;
            })()
        ]);
    };
};

internals.gatherResults = async (fn, jobs) => {

    // run subscribe fn in parallel for each row
    const results = await Promise.all(jobs.map(async (job) => {

        return {
            id: job.id,
            repeat: job.repeat_every,
            failed: await fn(Object.assign({}, job))
        };
    }));

    // gather results so we can do bulk actions
    return results.reduce((acc, result) => {

        if (result.failed) {
            acc.failed.push(result.id);
        }
        else if (result.repeat) {
            acc.reset.push(result.id);
        }
        else {
            acc.passed.push(result.id);
        }

        return acc;
    }, { passed: [], reset: [], failed: [] });
};

class Porker extends EventEmitter {
    constructor({ connection, queue, errorThreshold = 1, retryDelay = '5 minutes', timeout = 15000, concurrency = 1, healthcheckPort = null } = {}) {

        super();

        if (!queue) {
            throw new Error('Missing required parameter: queue');
        }

        this.queue = queue;
        this.errorThreshold = errorThreshold;
        this.retryDelay = retryDelay;
        this.timeout = timeout;
        this.concurrency = concurrency;
        this.healthcheckPort = healthcheckPort;

        Object.defineProperties(this, {
            [Symbols.client]: {
                value: new Pg.Client(connection),
                writable: true
            },
            [Symbols.worker]: {
                value: new Pg.Client(connection),
                writable: true
            },
            [Symbols.workerMutex]: {
                value: Promise.resolve(),
                writable: true
            },
            [Symbols.subscriberMutex]: {
                value: Promise.resolve(),
                writable: true
            },
            [Symbols.subscriberResolver]: {
                value: () => {},
                writable: true
            },
            [Symbols.retryWorker]: {
                value: new Pg.Client(connection),
                writable: true
            },
            [Symbols.retryWorkerMutex]: {
                value: Promise.resolve(),
                writable: true
            },
            [Symbols.retrierMutex]: {
                value: Promise.resolve(),
                writable: true
            },
            [Symbols.retrierResolver]: {
                value: () => {},
                writable: true
            },
            [Symbols.queries]: {
                value: Sql.queries(this)
            },
            [Symbols.healthcheck]: {
                value: Http.createServer((req, res) => {

                    // $lab:coverage:off$
                    const connected = this[Symbols.client].readyForQuery &&
                                      (this[Symbols.subscriber] ? this[Symbols.worker].readyForQuery : true) &&
                                      (this[Symbols.retrier] ? this[Symbols.retryWorker].readyForQuery : true);
                    // $lab:coverage:on$

                    res.writeHead(connected ? 200 : 400);
                    return res.end();
                })
            }
        });

        if (this.healthcheckPort) {
            this[Symbols.healthcheck].listen(this.healthcheckPort);
        }
    }

    connect() {

        if (!this[Symbols.connectMutex]) {
            Object.defineProperty(this, Symbols.connectMutex, {
                value: new Promise(async (resolve) => {

                    this[Symbols.client].once('connect', () => {

                        this.emit('ready');
                        return resolve();
                    });

                    await this[Symbols.client].connect();
                })
            });
        }

        return this[Symbols.connectMutex];
    }

    async create() {

        await this.connect();

        return this[Symbols.client].query(this[Symbols.queries].createTable);
    }

    async drop() {

        await this.connect();

        return this[Symbols.client].query(this[Symbols.queries].dropTable);
    }

    async publish(jobs, { priority = 0, repeat } = {}) {

        await this.connect();

        const list = [].concat(jobs);
        const res = await this[Symbols.client].query(this[Symbols.queries].insertJobs(list), [priority, repeat, ...list]);
        await this[Symbols.client].query(this[Symbols.queries].notifyQueue);

        return res.rows.map((row) => {

            return row.id;
        });
    }

    async unpublish(jobs) {

        await this.connect();

        return this[Symbols.client].query(this[Symbols.queries].completeJobs, [[].concat(jobs)]);
    }

    async subscribe(fn) {

        if (this[Symbols.subscriber]) {
            throw new Error('A subscriber has already been added to this queue');
        }

        Object.defineProperty(this, Symbols.subscriber, {
            value: internals.wrapSubscriber(fn, this.timeout)
        });

        await new Promise(async (resolve) => {

            this[Symbols.worker].once('connect', () => {

                return resolve();
            });

            await this[Symbols.worker].connect();
        });

        await this[Symbols.worker].query(this[Symbols.queries].listenPublishes);
        this[Symbols.subscriberMutex] = new Promise((resolve) => {

            this[Symbols.subscriberResolver] = resolve;
        });

        this[Symbols.worker].on('notification', (msg) => {

            this[Symbols.work]();
        });

        this.emit('subscriberReady');
        this[Symbols.work]();
    }

    async [Symbols.work]() {

        await this[Symbols.workerMutex];
        this[Symbols.workerMutex] = (async () => {

            this[Symbols.subscriberMutex] = new Promise((resolve) => {

                this[Symbols.subscriberResolver] = resolve;
            });

            let didWork = false;
            while (!this[Symbols.stopped]) {
                await this[Symbols.worker].query('BEGIN');
                const nextJobs = await this[Symbols.worker].query(this[Symbols.queries].findNextRun);

                if (nextJobs.rowCount === 0) {
                    await this[Symbols.worker].query('ROLLBACK');
                    break;
                }

                // Delay before next job should be run
                const timeout = nextJobs.rows[0].next_run - Date.now();
                if (timeout > 0) {
                    await this[Symbols.worker].query('ROLLBACK');
                    this[Symbols.workTimer] = setTimeout(() => {

                        this[Symbols.work]();
                    }, timeout);
                    break;
                }

                // We have a recurring job, so go ahead and schedule that
                const future = nextJobs.rows.find((row) => row.future_run);
                if (future) {
                    this[Symbols.workTimer] = setTimeout(() => {

                        this[Symbols.work]();
                    }, future.future_run - Date.now());
                }

                didWork = true;
                const jobs = await this[Symbols.worker].query(this[Symbols.queries].lockJobs, [nextJobs.rows.map((row) => row.id)]);
                const results = await internals.gatherResults(this[Symbols.subscriber], jobs.rows.map((row) => Object.assign({}, row)));

                await this[Symbols.worker].query(this[Symbols.queries].completeJobs, [results.passed]);
                await this[Symbols.worker].query(this[Symbols.queries].errorJobs, [results.failed]);
                await this[Symbols.worker].query('COMMIT');

                // If we had errors, notify the retry queue
                if (results.failed.length) {
                    await this[Symbols.worker].query(this[Symbols.queries].notifyRetryQueue);
                }
            }

            if (didWork) {
                this.emit('drain');
            }

            this[Symbols.subscriberResolver]();
        })();
    }

    async retry(fn) {

        if (this[Symbols.retrier]) {
            throw new Error('A retry handler has already been added to this queue');
        }

        Object.defineProperty(this, Symbols.retrier, {
            value: internals.wrapSubscriber(fn, this.timeout)
        });

        await new Promise(async (resolve) => {

            this[Symbols.retryWorker].once('connect', () => {

                return resolve();
            });

            await this[Symbols.retryWorker].connect();
        });

        await this[Symbols.retryWorker].query(this[Symbols.queries].listenRetries);
        this[Symbols.retrierMutex] = new Promise((resolve) => {

            this[Symbols.retrierResolver] = resolve;
        });

        this[Symbols.retryWorker].on('notification', (msg) => {

            this[Symbols.retry]();
        });

        this.emit('retrierReady');
        this[Symbols.retry]();
    }

    async [Symbols.retry]() {

        await this[Symbols.retryWorkerMutex];
        this[Symbols.retryWorkerMutex] = (async () => {

            this[Symbols.retrierMutex] = new Promise((resolve) => {

                this[Symbols.retrierResolver] = resolve;
            });

            let didWork = false;
            while (!this[Symbols.stopped]) {
                await this[Symbols.retryWorker].query('BEGIN');
                const nextJobs = await this[Symbols.retryWorker].query(this[Symbols.queries].findRetries);

                if (nextJobs.rowCount === 0) {
                    await this[Symbols.retryWorker].query('ROLLBACK');
                    break;
                }

                const timeout = nextJobs.rows[0].retry_at - Date.now();
                if (timeout > 0) {
                    await this[Symbols.retryWorker].query('ROLLBACK');
                    this[Symbols.retryWorkTimer] = setTimeout(() => {

                        this[Symbols.retry]();
                    }, timeout);
                    break;
                }

                didWork = true;
                const jobs = await this[Symbols.retryWorker].query(this[Symbols.queries].lockRetries, [nextJobs.rows.map((row) => row.id)]);
                const results = await internals.gatherResults(this[Symbols.retrier], jobs.rows.map((row) => Object.assign({}, row)));

                await this[Symbols.retryWorker].query(this[Symbols.queries].completeJobs, [results.passed]);
                await this[Symbols.retryWorker].query(this[Symbols.queries].resetJobs, [results.reset]);
                await this[Symbols.retryWorker].query(this[Symbols.queries].errorJobs, [results.failed]);
                await this[Symbols.retryWorker].query('COMMIT');
            }

            if (didWork) {
                this.emit('drainRetries');
            }

            this[Symbols.retrierResolver]();
        })();
    }

    async end() {

        this[Symbols.stopped] = true;

        if (this.healthcheckPort) {
            await new Promise((resolve) => {

                this[Symbols.healthcheck].close(resolve);
            });
        }

        if (this[Symbols.client]._connected) {
            await this[Symbols.client].end();
        }

        if (this[Symbols.worker]._connected) {
            await this[Symbols.subscriberMutex];
            await this[Symbols.workerMutex];
            clearTimeout(this[Symbols.workTimer]);

            await this[Symbols.worker].end();
        }

        if (this[Symbols.retryWorker]._connected) {
            await this[Symbols.retrierMutex];
            await this[Symbols.retryWorkerMutex];
            clearTimeout(this[Symbols.retryWorkTimer]);

            await this[Symbols.retryWorker].end();
        }

        this.emit('end');
    }
}

module.exports = Porker;
