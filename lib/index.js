'use strict';

const EventEmitter = require('events').EventEmitter;

const Http = require('http');
const Pg = require('pg');
const Util = require('util');

const AsyncMutex = require('./mutex');
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
                value: new Pg.Client(connection)
            },
            [Symbols.worker]: {
                value: new Pg.Client(connection)
            },
            [Symbols.workerMutex]: {
                value: new AsyncMutex()
            },
            [Symbols.retryWorker]: {
                value: new Pg.Client(connection)
            },
            [Symbols.retryWorkerMutex]: {
                value: new AsyncMutex()
            },
            [Symbols.queries]: {
                value: Sql.queries(this)
            },
            [Symbols.healthcheck]: {
                value: Http.createServer((req, res) => {

                    // $lab:coverage:off$
                    const connected = this[Symbols.client].readyForQuery &&
                                      this[Symbols.worker].readyForQuery &&
                                      this[Symbols.retryWorker].readyForQuery;
                    // $lab:coverage:on$

                    res.writeHead(connected ? 200 : 400);
                    return res.end();
                })
            },
            [Symbols.stopped]: {
                value: false,
                writable: true
            }
        });
        this[Symbols.workerMutex].release();
        this[Symbols.retryWorkerMutex].release();

        if (this.healthcheckPort) {
            this[Symbols.healthcheck].listen(this.healthcheckPort);
        }

        this[Symbols.worker].on('notification', (msg) => {

            if (msg.payload === 'retry') {
                this[Symbols.scheduleRetry]();
            }
            else {
                this[Symbols.scheduleWork]();
            }
        });
    }

    connect() {

        if (!this[Symbols.connectMutex]) {
            Object.defineProperty(this, Symbols.connectMutex, {
                value: new Promise(async (resolve) => {

                    const connection = new AsyncMutex();
                    this[Symbols.client].once('connect', () => {

                        connection.release();
                    });

                    this[Symbols.worker].once('connect', () => {

                        connection.release();
                    });

                    this[Symbols.retryWorker].once('connect', () => {

                        connection.release();
                    });

                    await Promise.all([
                        this[Symbols.client].connect(),
                        this[Symbols.worker].connect(),
                        this[Symbols.retryWorker].connect(),
                        connection.acquire(),
                        connection.acquire(),
                        connection.acquire()
                    ]);

                    await this[Symbols.worker].query(this[Symbols.queries].listenQueue);
                    this.emit('ready');
                    return resolve();
                })
            });
        }

        return this[Symbols.connectMutex];
    }

    async create() {

        await this.connect();

        await this[Symbols.client].query(this[Symbols.queries].createTable);
    }

    async drop() {

        await this.connect();

        await this[Symbols.client].query(this[Symbols.queries].dropTable);
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

    async [Symbols.work]() {

        await this[Symbols.workerMutex].acquire();

        let runCount = 0;

        while (!this[Symbols.stopped]) {
            await this[Symbols.worker].query('BEGIN');
            const res = await this[Symbols.worker].query(this[Symbols.queries].lockJobs);

            if (res.rowCount === 0) {
                await this[Symbols.worker].query('ROLLBACK');
                break;
            }

            const results = await internals.gatherResults(this[Symbols.subscriber], res.rows.map((row) => Object.assign({}, row)));

            await this[Symbols.worker].query(this[Symbols.queries].completeJobs, [results.passed]);
            await this[Symbols.worker].query(this[Symbols.queries].errorJobs, [results.failed]);
            if (results.failed.length) {
                await this[Symbols.worker].query(this[Symbols.queries].notifyRetryQueue);
            }
            await this[Symbols.worker].query('COMMIT');
            ++runCount;
        }

        await this[Symbols.workerMutex].release();
        await this[Symbols.scheduleWork]();
        if (runCount) {
            this.emit('drain');
        }
    }

    async [Symbols.retry]() {

        await this[Symbols.retryWorkerMutex].acquire();

        let runCount = 0;

        while (!this[Symbols.stopped]) {
            await this[Symbols.retryWorker].query('BEGIN');
            const res = await this[Symbols.retryWorker].query(this[Symbols.queries].lockRetries);

            if (res.rowCount === 0) {
                await this[Symbols.retryWorker].query('ROLLBACK');
                break;
            }

            const results = await internals.gatherResults(this[Symbols.retrier], res.rows.map((row) => Object.assign({}, row)));

            await this[Symbols.retryWorker].query(this[Symbols.queries].completeJobs, [results.passed]);
            await this[Symbols.retryWorker].query(this[Symbols.queries].resetJobs, [results.reset]);
            await this[Symbols.retryWorker].query(this[Symbols.queries].errorJobs, [results.failed]);
            await this[Symbols.retryWorker].query('COMMIT');
            ++runCount;
        }

        await this[Symbols.retryWorkerMutex].release();
        await this[Symbols.scheduleRetry]();
        if (runCount) {
            this.emit('drainRetries');
        }
    }

    unpublish(jobs) {

        return this[Symbols.client].query(this[Symbols.queries].completeJobs, [[].concat(jobs)]);
    }

    async [Symbols.scheduleWork]() {

        clearTimeout(this[Symbols.workTimer]);

        if (this[Symbols.stopped]) {
            return;
        }

        await this[Symbols.workerMutex].acquire();
        const res = await this[Symbols.worker].query(this[Symbols.queries].findRecurring);
        await this[Symbols.workerMutex].release();

        if (res.rowCount) {
            this[Symbols.workTimer] = setTimeout(() => {

                this[Symbols.work]();
            }, res.rows[0].next_run - Date.now());
        }
    }

    async [Symbols.scheduleRetry]() {

        clearTimeout(this[Symbols.retryTimer]);

        if (this[Symbols.stopped]) {
            return;
        }

        await this[Symbols.retryWorkerMutex].acquire();
        const res = await this[Symbols.retryWorker].query(this[Symbols.queries].findRetry);
        await this[Symbols.retryWorkerMutex].release();

        if (res.rowCount) {
            this[Symbols.retryTimer] = setTimeout(() => {

                this[Symbols.retry]();
            }, res.rows[0].next_run - Date.now());
        }
    }

    async subscribe(fn) {

        if (this[Symbols.subscriber]) {
            throw new Error('A subscriber has already been added to this queue');
        }

        await this.connect();

        Object.defineProperty(this, Symbols.subscriber, {
            value: internals.wrapSubscriber(fn, this.timeout)
        });
        this[Symbols.work]();
    }

    async retry(fn) {

        if (this[Symbols.retrier]) {
            throw new Error('A retry handler has already been added to this queue');
        }

        await this.connect();

        Object.defineProperty(this, Symbols.retrier, {
            value: internals.wrapSubscriber(fn, this.timeout)
        });
        this[Symbols.retry]();
    }

    async end() {

        this[Symbols.stopped] = true;
        clearTimeout(this[Symbols.workTimer]);
        clearTimeout(this[Symbols.retryTimer]);

        await this[Symbols.workerMutex].acquire();
        await this[Symbols.retryWorkerMutex].acquire();

        await Promise.all([
            this[Symbols.client].end(),
            this[Symbols.worker].end(),
            this[Symbols.retryWorker].end()
        ]);

        if (this.healthcheckPort) {
            this[Symbols.healthcheck].close();
        }

        this.emit('end');
    }
}

module.exports = Porker;
