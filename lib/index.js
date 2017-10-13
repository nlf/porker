'use strict';

const Pg = require('pg');
const Util = require('util');

const Sql = require('./sql');
const Symbols = require('./symbols');

const internals = {};
internals.timeout = Util.promisify(setTimeout);

class Porker {
    constructor({ host, port, user, password, database, queue, errorThreshold = 0, timeout = 15000 }) {

        if (!queue) {
            throw new Error('Missing required parameter: queue');
        }

        this.queue = queue;
        this.errorThreshold = errorThreshold;
        this.timeout = timeout;

        Object.defineProperty(this, Symbols.sql, {
            value: Sql.queries(this)
        });

        Object.defineProperty(this, Symbols.pg, {
            value: new Pg.Client({
                host,
                port,
                user,
                password,
                database
            })
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
        await this[Symbols.pg].query(this[Symbols.sql].createTable);
    }

    async publish(job, { priority = 0, repeat } = {}) {

        await this.connect();
        const res = await this[Symbols.pg].query(this[Symbols.sql].insertJob, [job, priority, repeat]);
        await this[Symbols.pg].query(this[Symbols.sql].notifyQueue);
        return res.rows[0];
    }

    async [Symbols.loop]() {

        if (this[Symbols.working] ||
        !this[Symbols.fn]) {

            return;
        }

        this[Symbols.working] = true;

        // eslint-disable-next-line no-constant-condition
        while (!this[Symbols.stopped]) {
            await this[Symbols.pg].query('BEGIN');
            const res = await this[Symbols.pg].query(this[Symbols.sql].lockJob);

            // no jobs available, mark us as not working and exit the loop
            if (res.rowCount === 0) {
                this[Symbols.working] = false;
                await this[Symbols.pg].query('ABORT');
                break;
            }

            const job = Object.assign({}, res.rows[0]);
            try {
                await Promise.race([
                    this[Symbols.fn](job),
                    (async () => {

                        await internals.timeout(this.timeout);
                        throw new Error('Timed out');
                    })()
                ]);

                if (!job.repeat_every) {
                    await this[Symbols.pg].query(this[Symbols.sql].completeJob, [job.id]);
                }
            }
            catch (err) {
                await this[Symbols.pg].query(this[Symbols.sql].errorJob, [job.id]);
            }
            await this[Symbols.pg].query('COMMIT');
        }

        await this[Symbols.repeat]();
    }

    unpublish(job) {

        const id = typeof job === 'object' ? job.id : job;
        return this[Symbols.pg].query(this[Symbols.sql].completeJob, [id]);
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
        this[Symbols.fn] = fn;
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
