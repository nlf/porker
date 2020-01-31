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

        const connectionSettings = typeof connection === 'string' ? { connectionString: connection } : connection;

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
                value: new Pg.Pool(connectionSettings)
            },
            [Symbols.worker]: {
                value: new Pg.Pool(Object.assign({ max: 2 }, connectionSettings))
            },
            [Symbols.retryWorker]: {
                value: new Pg.Pool(Object.assign({ max: 2 }, connectionSettings))
            },
            [Symbols.queries]: {
                value: Sql.queries(this)
            },
            [Symbols.healthcheck]: {
                value: Http.createServer((req, res) => {

                    // $lab:coverage:off$
                    const connected = (this[Symbols.subscriber] ? this[Symbols.worker].totalCount > 0 : true) &&
                                      (this[Symbols.retrier] ? this[Symbols.retryWorker].totalCount > 0 : true);
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

    async create() {

        const client = await this[Symbols.client].connect();
        await client.query(this[Symbols.queries].createTable);
        client.release();
    }

    async drop() {

        const client = await this[Symbols.client].connect();
        await this[Symbols.client].query(this[Symbols.queries].dropTable);
        client.release();
    }

    async publish(jobs, { priority = 0, repeat } = {}) {

        const client = await this[Symbols.client].connect();

        const list = [].concat(jobs);
        await client.query('BEGIN');
        const res = await client.query(this[Symbols.queries].insertJobs(list), [priority, repeat, ...list]);
        await client.query(this[Symbols.queries].notifyQueue);
        await client.query('COMMIT');
        client.release();

        return res.rows.map((row) => {

            return row.id;
        });
    }

    async unpublish(jobs) {

        const client = await this[Symbols.client].connect();
        await client.query(this[Symbols.queries].completeJobs, [[].concat(jobs)]);
        client.release();
    }

    async subscribe(fn) {

        if (this[Symbols.subscriber]) {
            throw new Error('A subscriber has already been added to this queue');
        }

        Object.defineProperties(this, {
            [Symbols.subscriber]: {
                value: internals.wrapSubscriber(fn, this.timeout)
            },
            [Symbols.workerListener]: {
                value: await this[Symbols.worker].connect()
            }
        });

        this[Symbols.workerListener].on('notification', () => {

            this[Symbols.work]();
        });

        await this[Symbols.workerListener].query(this[Symbols.queries].listenPublishes);

        this.emit('subscriberReady');
        this[Symbols.work]();
    }

    async [Symbols.work]() {

        const client = await this[Symbols.worker].connect();

        let didWork = false;
        while (!this[Symbols.stopped]) {
            await client.query('BEGIN');
            const { rows: currentJobs } = await client.query(this[Symbols.queries].lockCurrentJobs);
            const futureJob = await client.query(this[Symbols.queries].findFutureJob);
            if (futureJob.rows.length) {
                clearTimeout(this[Symbols.workTimer]);
                this[Symbols.workTimer] = setTimeout(() => {

                    this[Symbols.work]();
                }, futureJob.rows[0].next_run - Date.now());
            }


            if (!currentJobs.length) {
                await client.query('ROLLBACK');
                break;
            }

            didWork = true;

            const results = await internals.gatherResults(this[Symbols.subscriber], currentJobs.map((job) => Object.assign({}, job)));
            await client.query(this[Symbols.queries].completeJobs, [results.passed]);
            await client.query(this[Symbols.queries].errorJobs, [results.failed]);

            // If we had errors, notify the retry queue
            if (results.failed.length) {
                await client.query(this[Symbols.queries].notifyRetryQueue);
            }

            await client.query('COMMIT');
        }

        if (didWork) {
            this.emit('drain');
        }

        client.release();
    }

    async retry(fn) {

        if (this[Symbols.retrier]) {
            throw new Error('A retry handler has already been added to this queue');
        }

        Object.defineProperties(this, {
            [Symbols.retrier]: {
                value: internals.wrapSubscriber(fn, this.timeout)
            },
            [Symbols.retryListener]: {
                value: await this[Symbols.retryWorker].connect()
            }
        });

        this[Symbols.retryListener].on('notification', (msg) => {

            this[Symbols.retry]();
        });

        await this[Symbols.retryListener].query(this[Symbols.queries].listenRetries);

        this.emit('retrierReady');
        this[Symbols.retry]();
    }

    async [Symbols.retry]() {

        const client = await this[Symbols.retryWorker].connect();

        let didWork = false;
        while (!this[Symbols.stopped]) {
            await client.query('BEGIN');
            const pendingJobs = await client.query(this[Symbols.queries].lockPendingRetries);

            if (pendingJobs.rowCount === 0) {
                await client.query('ROLLBACK');
                break;
            }

            const currentJobs = [];
            const futureJobs = [];
            for (const job of pendingJobs.rows) {
                if (job.retry_at <= Date.now()) {
                    currentJobs.push(job);
                }
                else {
                    futureJobs.push(job);
                }
            }

            if (futureJobs.length) {
                clearTimeout(this[Symbols.retryWorkTimer]);
                this[Symbols.retryWorkTimer] = setTimeout(() => {

                    this[Symbols.retry]();
                }, futureJobs[0].retry_at - Date.now());
            }

            if (!currentJobs.length) {
                await client.query('ROLLBACK');
                break;
            }

            didWork = true;
            const results = await internals.gatherResults(this[Symbols.retrier], currentJobs.map((row) => Object.assign({}, row)));

            await client.query(this[Symbols.queries].completeJobs, [results.passed]);
            await client.query(this[Symbols.queries].resetJobs, [results.reset]);
            await client.query(this[Symbols.queries].errorJobs, [results.failed]);
            await client.query('COMMIT');
        }

        if (didWork) {
            this.emit('drainRetries');
        }

        client.release();
    }

    async end() {

        this[Symbols.stopped] = true;
        clearTimeout(this[Symbols.workTimer]);
        clearTimeout(this[Symbols.retryWorkTimer]);

        if (this.healthcheckPort) {
            await new Promise((resolve) => {

                this[Symbols.healthcheck].close(resolve);
            });
        }

        await this[Symbols.client].end();

        if (this[Symbols.workerListener]) {
            this[Symbols.workerListener].release();
        }
        await this[Symbols.worker].end();

        if (this[Symbols.retryListener]) {
            this[Symbols.retryListener].release();
        }
        await this[Symbols.retryWorker].end();

        this.emit('end');
    }
}

module.exports = Porker;
