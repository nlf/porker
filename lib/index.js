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

        this[Symbols.working] = new AsyncMutex();
        this[Symbols.working].release();

        Object.defineProperty(this, Symbols.sql, {
            value: Sql.queries(this)
        });

        Object.defineProperty(this, Symbols.pg, {
            value: new Pg.Client(connection)
        });

        if (this.healthcheckPort) {
            Object.defineProperty(this, Symbols.healthcheck, {
                value: Http.createServer(this[Symbols.healthcheckHandler].bind(this))
            });

            this[Symbols.healthcheck].listen(this.healthcheckPort);
        }

        this[Symbols.pg].on('notification', (msg) => {

            this[Symbols.loop]();
        });
    }

    [Symbols.healthcheckHandler](req, res) {

        res.writeHead(this[Symbols.pg]._connected ? 200 : 400);
        return res.end();
    }

    async connect() {

        if (this[Symbols.connection]) {
            return await this[Symbols.connection];
        }

        this[Symbols.connection] = new Promise(async (resolve) => {

            this[Symbols.pg].once('connect', () => {

                return resolve();
            });

            await this[Symbols.pg].connect();
            await this[Symbols.pg].query(this[Symbols.sql].listenQueue);
            this.emit('ready');
        });
    }

    async create() {

        await this.connect();

        await this[Symbols.pg].query(this[Symbols.sql].createTable);
    }

    async drop() {

        await this.connect();

        await this[Symbols.pg].query(this[Symbols.sql].dropTable);
    }

    async publish(jobs, { priority = 0, repeat } = {}) {

        await this.connect();

        const list = [].concat(jobs);
        const res = await this[Symbols.pg].query(this[Symbols.sql].insertJobs(list), [priority, repeat, ...list]);
        await this[Symbols.pg].query(this[Symbols.sql].notifyQueue);

        return res.rows.map((row) => {

            return row.id;
        });
    }

    async [Symbols.loop]() {

        if (!this[Symbols.fn]) {
            return;
        }

        await this[Symbols.working].acquire();

        let runCount = 0;

        while (!this[Symbols.stopped]) {
            await this[Symbols.pg].query('BEGIN');
            const res = await this[Symbols.pg].query(this[Symbols.sql].lockJobs);

            if (res.rowCount === 0) {
                await this[Symbols.pg].query('ROLLBACK');
                break;
            }

            const results = await internals.gatherResults(this[Symbols.fn], res.rows.map((row) => Object.assign({}, row)));

            await this[Symbols.pg].query(this[Symbols.sql].completeJobs, [results.passed]);
            await this[Symbols.pg].query(this[Symbols.sql].errorJobs, [results.failed]);
            await this[Symbols.pg].query('COMMIT');
            ++runCount;
        }

        await this[Symbols.repeat]();
        if (runCount) {
            this.emit('drain');
        }
        await this[Symbols.working].release();
    }

    async [Symbols.retryLoop]() {

        await this[Symbols.working].acquire();

        let runCount = 0;

        while (!this[Symbols.stopped]) {
            await this[Symbols.pg].query('BEGIN');
            const res = await this[Symbols.pg].query(this[Symbols.sql].lockRetries);

            if (res.rowCount === 0) {
                await this[Symbols.pg].query('ROLLBACK');
                break;
            }

            const results = await internals.gatherResults(this[Symbols.retry], res.rows.map((row) => Object.assign({}, row)));

            await this[Symbols.pg].query(this[Symbols.sql].completeJobs, [results.passed]);
            await this[Symbols.pg].query(this[Symbols.sql].resetJobs, [results.reset]);
            await this[Symbols.pg].query(this[Symbols.sql].errorJobs, [results.failed]);
            await this[Symbols.pg].query('COMMIT');
            ++runCount;
        }

        await this[Symbols.repeatRetries]();
        if (runCount) {
            this.emit('drainRetries');
        }
        await this[Symbols.working].release();
    }

    unpublish(jobs) {

        return this[Symbols.pg].query(this[Symbols.sql].completeJobs, [[].concat(jobs)]);
    }

    async [Symbols.repeat]() {

        clearTimeout(this[Symbols.timer]);

        if (this[Symbols.stopped]) {
            return;
        }

        const res = await this[Symbols.pg].query(this[Symbols.sql].findRecurring);
        if (res.rowCount) {
            this[Symbols.timer] = setTimeout(() => {

                this[Symbols.loop]();
            }, res.rows[0].next_run - Date.now());
        }
    }

    async [Symbols.repeatRetries]() {

        clearTimeout(this[Symbols.retryTimer]);

        if (this[Symbols.stopped]) {
            return;
        }

        const res = await this[Symbols.pg].query(this[Symbols.sql].findRetry);
        if (res.rowCount) {
            this[Symbols.retryTimer] = setTimeout(() => {

                this[Symbols.retryLoop]();
            }, res.rows[0].next_run - Date.now());
        }
    }

    async subscribe(fn) {

        if (this[Symbols.fn]) {
            throw new Error('A subscriber has already been added to this queue');
        }

        await this.connect();

        this[Symbols.fn] = internals.wrapSubscriber(fn, this.timeout);
        this[Symbols.loop]();
    }

    async retry(fn) {

        if (this[Symbols.retry]) {
            throw new Error('A retry handler has already been added to this queue');
        }

        await this.connect();

        this[Symbols.retry] = internals.wrapSubscriber(fn, this.timeout);
        this[Symbols.retryLoop]();
    }

    async end() {

        this[Symbols.stopped] = true;
        clearTimeout(this[Symbols.timer]);
        clearTimeout(this[Symbols.retryTimer]);

        await this[Symbols.working].acquire();

        while (this[Symbols.pg].activeQuery ||
               this[Symbols.pg].queryQueue.length) {

            await internals.timeout(10);
        }

        await this[Symbols.pg].end();

        if (this.healthcheckPort) {
            this[Symbols.healthcheck].close();
        }

        this.emit('end');
    }
}

module.exports = Porker;
