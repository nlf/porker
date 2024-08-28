'use strict';

const EventEmitter = require('events').EventEmitter;

const Http = require('http');
const Pg = require('pg');
const Util = require('util');

const Sql = require('./sql');

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
    #healthcheck;
    #queries;

    #client;

    #retrier;
    #retryListener;
    #retryWorker;

    #subscriber;
    #subscriptionListener;
    #subscriptionWorker;

    #stopped = false;

    #workTimer = null;
    #retryTimer = null;

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

        this.#healthcheck = Http.createServer((req, res) => {

            res.writeHead(200);
            return res.end();
        });

        this.#queries = Sql.queries(this);

        this.#client = new Pg.Pool(connectionSettings);
        this.#retryWorker = new Pg.Pool({ max: 2, ...connectionSettings });
        this.#subscriptionWorker = new Pg.Pool({ max: 2, ...connectionSettings });

        if (this.healthcheckPort) {
            this.#healthcheck.listen(this.healthcheckPort);
        }
    }

    async create() {

        const client = await this.#client.connect();
        await client.query(this.#queries.createTable);
        return client.release();
    }

    async drop() {

        const client = await this.#client.connect();
        await client.query(this.#queries.dropTable);
        return client.release();
    }

    async publish(jobs, { priority = 0, repeat } = {}) {

        const client = await this.#client.connect();

        const list = [].concat(jobs);
        await client.query('BEGIN');
        const res = await client.query(this.#queries.insertJobs(list), [priority, repeat, ...list]);
        await client.query(this.#queries.notifyQueue);
        await client.query('COMMIT');
        client.release();

        return res.rows.map((row) => {

            return row.id;
        });
    }

    async unpublish(jobs) {

        const client = await this.#client.connect();
        await client.query(this.#queries.completeJobs, [[].concat(jobs)]);
        return client.release();
    }

    async subscribe(fn) {

        if (this.#subscriber) {
            throw new Error('A subscriber has already been added to this queue');
        }

        this.#subscriber = internals.wrapSubscriber(fn, this.timeout);

        this.#subscriptionListener = await this.#subscriptionWorker.connect();
        this.#subscriptionListener.on('notification', () => {

            this.#work();
        });

        await this.#subscriptionListener.query(this.#queries.listenPublishes);

        this.emit('subscriberReady');
        this.#work();
    }

    async #work() {

        const client = await this.#subscriptionWorker.connect();

        let didWork = false;
        while (!this.#stopped) {
            await client.query('BEGIN');
            const { rows: currentJobs } = await client.query(this.#queries.lockCurrentJobs);
            const futureJob = await client.query(this.#queries.findFutureJob);
            if (futureJob.rows.length) {
                clearTimeout(this.#workTimer);
                this.#workTimer = setTimeout(() => {

                    this.#work();
                }, futureJob.rows[0].next_run - Date.now());
            }


            if (!currentJobs.length) {
                await client.query('ROLLBACK');
                break;
            }

            didWork = true;

            const results = await internals.gatherResults(this.#subscriber, currentJobs.map((job) => Object.assign({}, job)));
            await client.query(this.#queries.completeJobs, [results.passed]);
            await client.query(this.#queries.errorJobs, [results.failed]);

            // If we had errors, notify the retry queue
            if (results.failed.length) {
                await client.query(this.#queries.notifyRetryQueue);
            }

            await client.query('COMMIT');
        }

        if (didWork) {
            this.emit('drain');
        }

        client.release();
    }

    async retry(fn) {

        if (this.#retrier) {
            throw new Error('A retry handler has already been added to this queue');
        }

        this.#retrier = internals.wrapSubscriber(fn, this.timeout);

        this.#retryListener = await this.#retryWorker.connect();
        this.#retryListener.on('notification', (msg) => {

            this.#retry();
        });

        await this.#retryListener.query(this.#queries.listenRetries);

        this.emit('retrierReady');
        this.#retry();
    }

    async #retry() {

        const client = await this.#retryWorker.connect();

        let didWork = false;
        while (!this.#stopped) {
            await client.query('BEGIN');
            const pendingJobs = await client.query(this.#queries.lockPendingRetries);

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
                clearTimeout(this.#retryTimer);
                this.#retryTimer = setTimeout(() => {

                    this.#retry();
                }, futureJobs[0].retry_at - Date.now());
            }

            if (!currentJobs.length) {
                await client.query('ROLLBACK');
                break;
            }

            didWork = true;
            const results = await internals.gatherResults(this.#retrier, currentJobs.map((row) => Object.assign({}, row)));

            await client.query(this.#queries.completeJobs, [results.passed]);
            await client.query(this.#queries.resetJobs, [results.reset]);
            await client.query(this.#queries.errorJobs, [results.failed]);
            await client.query('COMMIT');
        }

        if (didWork) {
            this.emit('drainRetries');
        }

        client.release();
    }

    async end() {

        this.#stopped = true;
        clearTimeout(this.#workTimer);
        clearTimeout(this.#retryTimer);

        if (this.healthcheckPort) {
            await new Promise((resolve) => {

                this.#healthcheck.close(resolve);
            });
        }

        await this.#client.end();

        if (this.#subscriptionListener) {
            this.#subscriptionListener.release();
        }

        await this.#subscriptionWorker.end();

        if (this.#retryListener) {
            this.#retryListener.release();
        }

        await this.#retryWorker.end();

        this.emit('end');
    }
}

module.exports = Porker;
