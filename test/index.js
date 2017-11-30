'use strict';

const AsyncMutex = require('../lib/mutex');
const Porker = require('../');
const Symbols = require('../lib/symbols');

const Http = require('http');
const Pg = require('pg');

const Code = require('code');
const Lab = require('lab');
const Util = require('util');

const get = Util.promisify(Http.get);
const timeout = Util.promisify(setTimeout);
const { afterEach, describe, it } = exports.lab = Lab.script();
const { expect, fail } = Code;


describe('Porker', () => {

    const connection = process.env.PORKER_CONNECTION ? { connectionString: process.env.PORKER_CONNECTION } : { database: 'porker_test_suite' };

    afterEach(async () => {

        const client = new Pg.Client(connection);
        await client.connect();
        await client.query('BEGIN');
        let res = await client.query(`SELECT 'DROP TABLE IF EXISTS ' || quote_ident(table_schema) || '.' || quote_ident(table_name) || ' CASCADE;' AS drop_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND NOT table_schema ~ '^(information_schema|pg_.*)$'`);
        for (const row of res.rows) {
            await client.query(row.drop_table);
        }

        res = await client.query(`SELECT 'DROP SEQUENCE IF EXISTS ' || quote_ident(relname) || ' CASCADE;' AS drop_sequence FROM pg_statio_user_sequences`);
        for (const row of res.rows) {
            await client.query(row.drop_sequence);
        }
        await client.query('COMMIT');
        await client.end();
    });

    it('throws when no queue is specified', () => {

        expect(() => {

            new Porker();
        }).to.throw('Missing required parameter: queue');
    });

    it('throws when a subscriber is added twice', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        await worker.subscribe(async () => {});
        await expect(worker.subscribe(async () => {})).to.reject('A subscriber has already been added to this queue');

        await worker.end();
    });

    it('throws when retrier is added twice', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        await worker.retry(async () => {});
        await expect(worker.retry(async () => {})).to.reject('A retry handler has already been added to this queue');

        await worker.end();
    });

    it('does not throw when queues have a dash', async () => {

        const worker = new Porker({ connection, queue: 'test-queue' });
        await expect(worker.create()).to.not.reject();

        await worker.end();
    });

    it('can create its own table', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT column_name FROM information_schema.columns WHERE table_name = \'test_jobs\'');
        expect(res.rowCount).to.equal(7);

        const rows = res.rows.map((row) => Object.assign({}, row));
        expect(rows).to.contain({ column_name: 'id' });
        expect(rows).to.contain({ column_name: 'priority' });
        expect(rows).to.contain({ column_name: 'started_at' });
        expect(rows).to.contain({ column_name: 'repeat_every' });
        expect(rows).to.contain({ column_name: 'error_count' });
        expect(rows).to.contain({ column_name: 'retry_at' });
        expect(rows).to.contain({ column_name: 'args' });

        await worker.end();
    });

    it('can drop its own table', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const db = worker[Symbols.pg];
        let res = await db.query('SELECT column_name FROM information_schema.columns WHERE table_name = \'test_jobs\'');
        expect(res.rowCount).to.equal(7);

        const rows = res.rows.map((row) => Object.assign({}, row));
        expect(rows).to.contain({ column_name: 'id' });
        expect(rows).to.contain({ column_name: 'priority' });
        expect(rows).to.contain({ column_name: 'started_at' });
        expect(rows).to.contain({ column_name: 'repeat_every' });
        expect(rows).to.contain({ column_name: 'error_count' });
        expect(rows).to.contain({ column_name: 'retry_at' });
        expect(rows).to.contain({ column_name: 'args' });

        await worker.drop();
        res = await db.query('SELECT column_name FROM information_schema.columns WHERE table_name = \'test_jobs\'');
        expect(res.rowCount).to.equal(0);
        await worker.end();
    });

    it('can handle a single job', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = new AsyncMutex();

        worker.once('drain', () => {

            drained.release();
        });

        await worker.publish({ some: 'data' });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
        });

        await listener.acquire();
        await drained.acquire();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle a failing job', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        const [id] = await worker.publish({ some: 'data' });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
            throw new Error('Uh oh');
        });

        await Promise.all([
            listener.acquire(),
            drained.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(1);
        expect(res.rows[0].id).to.equal(id);
        expect(res.rows[0].error_count).to.equal(1);
        expect(res.rows[0].retry_at).to.be.above(new Date());

        await worker.end();
    });

    it('can retry a failed job', async () => {

        const worker = new Porker({ connection, queue: 'test', retryDelay: '10 milliseconds' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        const drainedRetries = new AsyncMutex();
        worker.once('drainRetries', () => {

            drainedRetries.release();
        });

        await worker.publish({ some: 'data' });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
            throw new Error('Uh oh');
        });

        const retrier = new AsyncMutex();
        await worker.retry((job) => {

            expect(job.args).to.equal({ some: 'data' });
            expect(job.error_count).to.equal(1);
            retrier.release();
        });

        await Promise.all([
            listener.acquire(),
            retrier.acquire(),
            drained.acquire(),
            drainedRetries.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle two jobs', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        await worker.publish({ some: 'data' });
        await worker.publish({ some: 'data' });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
        });

        await Promise.all([
            listener.acquire(),
            listener.acquire(),
            drained.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can retry two failed jobs', async () => {

        const worker = new Porker({ connection, queue: 'test', retryDelay: '1 millisecond' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.on('drain', () => {

            drained.release();
        });

        const drainedRetries = new AsyncMutex();
        worker.on('drainRetries', () => {

            drainedRetries.release();
        });

        await worker.publish({ some: 'data' });
        await worker.publish({ some: 'data' });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
            throw new Error('Uh oh');
        });

        const retrier = new AsyncMutex();
        await worker.retry((job) => {

            expect(job.args).to.equal({ some: 'data' });
            expect(job.error_count).to.equal(1);
            retrier.release();
        });

        await Promise.all([
            listener.acquire(),
            listener.acquire(),
            drained.acquire(),
            retrier.acquire(),
            retrier.acquire(),
            drainedRetries.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can bulk publish jobs', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        await worker.publish([{ some: 'data' }, { some: 'data' }]);

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
        });

        await Promise.all([
            listener.acquire(),
            listener.acquire(),
            drained.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle a publish after a subscription', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
        });

        await worker.publish({ some: 'data' });

        await Promise.all([
            listener.acquire(),
            drained.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle two publishes after a subscription', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
        });

        await worker.publish({ some: 'data' });
        await listener.acquire();

        await worker.publish({ some: 'data' });
        await listener.acquire();

        await drained.acquire();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can timeout a job', async () => {

        const worker = new Porker({ connection, queue: 'test', timeout: 1 });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        const listener = new AsyncMutex();
        await worker.subscribe(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
            listener.release();
            await timeout(10);
        });

        await worker.publish({ some: 'data' });

        await Promise.all([
            listener.acquire(),
            drained.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        const row = Object.assign({}, res.rows[0]);
        expect(row).to.contain({ error_count: 1, args: { some: 'data' } });

        await worker.end();
    });

    it('can create a recurring job', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
            listener.release();
        });

        await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await Promise.all([
            listener.acquire(),
            listener.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        const row = Object.assign({}, res.rows[0]);
        expect(row).to.contain({ error_count: 0, args: { timer: 'data' } });
        expect(row.repeat_every).to.not.equal(null);

        await worker.end();
    });

    it('can retry a recurring job', async () => {

        const worker = new Porker({ connection, queue: 'test', retryDelay: '10 milliseconds' });
        await worker.create();

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
            listener.release();
            throw new Error('Uh oh');
        });

        const retrier = new AsyncMutex();
        await worker.retry((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
            retrier.release();
        });

        await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await listener.acquire();
        await retrier.acquire();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        const row = Object.assign({}, res.rows[0]);
        expect(row).to.contain({ error_count: 1, args: { timer: 'data' } });
        expect(row.repeat_every).to.not.equal(null);

        await worker.end();
    });

    it('can retry a failed job and reset it', async () => {

        const worker = new Porker({ connection, queue: 'test', retryDelay: '10 milliseconds' });
        await worker.create();

        const drained = new AsyncMutex();
        worker.once('drain', () => {

            drained.release();
        });

        const drainedRetries = new AsyncMutex();
        worker.once('drainRetries', () => {

            drainedRetries.release();
        });

        const listener = new AsyncMutex();
        await worker.subscribe((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
            listener.release();
            throw new Error('Uh oh');
        });

        const retrier = new AsyncMutex();
        await worker.retry((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            retrier.release();
        });

        const [id] = await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });

        await Promise.all([
            listener.acquire(),
            drained.acquire(),
            retrier.acquire(),
            drainedRetries.acquire()
        ]);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        const row = Object.assign({}, res.rows[0]);
        expect(row.id).to.equal(id);
        expect(row.error_count).to.equal(0);
        expect(row.repeat_every).to.not.equal(null);
        expect(row.retry_at).to.equal(null);

        await worker.end();
    });

    it('can unpublish a job', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const [job] = await worker.publish({ some: 'data' });

        const db = worker[Symbols.pg];
        while (db.activeQuery) {
            await timeout(1);
        }

        let res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        expect(res.rows[0].id).to.equal(job);

        await worker.unpublish(job);
        res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can bulk unpublish jobs', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const jobs = await worker.publish([{ some: 'data' }, { some: 'data' }]);
        expect(jobs.length).to.equal(2);

        const db = worker[Symbols.pg];
        while (db.activeQuery) {
            await timeout(1);
        }

        let res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(2);
        expect(res.rows[0].id).to.equal(jobs[0]);
        expect(res.rows[1].id).to.equal(jobs[1]);

        await worker.unpublish(jobs);
        res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('returns 400 on healthcheck when not connected', async () => {

        const worker = new Porker({ connection, queue: 'test', healthcheckPort: 4500 });

        try {
            await get(`http://localhost:${worker.healthcheckPort}`);
            fail('this should not be reachable');
        }
        catch (err) {
            expect(err.statusCode).to.equal(400);
        }

        worker[Symbols.healthcheck].close();
    });

    it('returns 200 on healthcheck when connected', async () => {

        const worker = new Porker({ connection, queue: 'test', healthcheckPort: 4500 });

        await worker.create();

        // why this throws even for a 200, i have no idea
        try {
            await get(`http://localhost:${worker.healthcheckPort}`);
            fail('this should not be reachable');
        }
        catch (err) {
            expect(err.statusCode).to.equal(200);
        }

        await worker.end();
    });

    it('has no healthcheck listener by default', async () => {

        const worker = new Porker({ connection, queue: 'test' });

        expect(worker[Symbols.healthcheck]).to.not.exist();

        await worker.create();
        await worker.end();
    });
});
