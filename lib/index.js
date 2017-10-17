'use strict';

const Pg = require('pg');
const Util = require('util');

const Sql = require('./sql');
const Symbols = require('./symbols');

const internals = {};
internals.timeout = Util.promisify(setTimeout);

class Porker {
    constructor({ connection, queue, errorThreshold = 0, timeout = 15000, concurrency = 1 } = {}) {

        if (!queue) {
            throw new Error('Missing required parameter: queue');
        }

        this.queue = queue;
        this.errorThreshold = errorThreshold;
        this.timeout = timeout;
        this.concurrency = concurrency;

        Object.defineProperty(this, Symbols.sql, {
            value: Sql.queries(this)
        });

        Object.defineProperty(this, Symbols.pg, {
            value: new Pg.Client(connection)
        });

        this[Symbols.pg].on('notification', (msg) => {

            this[Symbols.loop]();
        });
    }

    async connect() {

        if (this[Symbols.pg]._connected) {
            return;
        }

        await this[Symbols.pg].connect();
        await this[Symbols.pg].query(this[Symbols.sql].listenQueue);
    }

    async create() {

        await this.connect();

        await this[Symbols.pg].query('BEGIN');
        await this[Symbols.pg].query(this[Symbols.sql].createTable);
        await this[Symbols.pg].query(this[Symbols.sql].indexErrorCount);
        await this[Symbols.pg].query(this[Symbols.sql].indexPriority);
        await this[Symbols.pg].query(this[Symbols.sql].indexRepeatEvery);
        await this[Symbols.pg].query(this[Symbols.sql].indexStartedAt);
        await this[Symbols.pg].query('COMMIT');
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

        if (this[Symbols.working] ||
        !this[Symbols.fn]) {

            return;
        }

        this[Symbols.working] = true;

        while (!this[Symbols.stopped]) {
            await this[Symbols.pg].query('BEGIN');
            const res = await this[Symbols.pg].query(this[Symbols.sql].lockJob);

            // no jobs available, mark us as not working and exit the loop
            if (res.rowCount === 0) {
                this[Symbols.working] = false;
                await this[Symbols.pg].query('ABORT');
                break;
            }

            // run subscribe fn in parallel for each row
            const results = await Promise.all(res.rows.map(async (row) => {

                return {
                    id: row.id,
                    repeat: row.repeat_every,
                    failed: await this[Symbols.fn](Object.assign({}, row))
                };
            }));

            // gather results so we can do bulk actions
            const reduced = results.reduce((acc, result) => {

                if (result.failed) {
                    acc.failed.push(result.id);
                }
                else if (!result.repeat) {
                    acc.passed.push(result.id);
                }

                return acc;
            }, { passed: [], failed: [] });

            await this[Symbols.pg].query(this[Symbols.sql].completeJobs, [reduced.passed]);
            await this[Symbols.pg].query(this[Symbols.sql].errorJobs, [reduced.failed]);
            await this[Symbols.pg].query('COMMIT');
        }

        await this[Symbols.repeat]();
    }

    unpublish(jobs) {

        return this[Symbols.pg].query(this[Symbols.sql].completeJobs, [[].concat(jobs)]);
    }

    async [Symbols.repeat]() {

        clearTimeout(this[Symbols.timer]);
        const res = await this[Symbols.pg].query(this[Symbols.sql].findRecurring);
        if (res.rowCount) {
            this[Symbols.timer] = setTimeout(() => {

                this[Symbols.loop]();
            }, res.rows[0].next_run - Date.now());
        }
    }

    async subscribe(fn) {

        if (this[Symbols.fn]) {
            throw new Error('A subscriber has already been added to this queue');
        }

        await this.connect();

        // we wrap the user supplied function to guarantee
        // that it does not throw and returns a boolean
        // representing if it failed (or timed out)
        this[Symbols.fn] = (job) => {

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

                    await internals.timeout(this.timeout);
                    return true;
                })()
            ]);
        };

        this[Symbols.loop]();
    }

    async end() {

        this[Symbols.stopped] = true;
        while (this[Symbols.pg].activeQuery) {
            await internals.timeout(10);
        }

        await this[Symbols.pg].end();
    }
}

module.exports = Porker;
