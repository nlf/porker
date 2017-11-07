'use strict';

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

const internals = {};
internals.instrument = (fn) => {

    const f = async function (job) {

        try {
            await fn(job);
        }
        catch (err) {
            f.err = err;
        }

        f.called = true;
        ++f.count;

        if (f.err) {
            const err = f.err;
            f.err = undefined;
            throw err;
        }
    };

    f.called = false;
    f.count = 0;
    f.fired = async () => {

        while (!f.called) {
            await Util.promisify(setImmediate)();
        }

        f.called = false;
    };

    return f;
};


describe('Porker', () => {

    const connection = process.env.PORKER_CONNECTION ? { connectionString: process.env.PORKER_CONNECTION } : { database: 'porker_test_suite' };

    afterEach(async () => {

        const client = new Pg.Client(connection);
        await client.connect();
        let res = await client.query(`SELECT 'DROP TABLE IF EXISTS ' || quote_ident(table_schema) || '.' || quote_ident(table_name) || ' CASCADE;' AS drop_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND NOT table_schema ~ '^(information_schema|pg_.*)$'`);
        for (const row of res.rows) {
            await client.query(row.drop_table);
        }

        res = await client.query(`SELECT 'DROP SEQUENCE IF EXISTS ' || quote_ident(relname) || ' CASCADE;' AS drop_sequence FROM pg_statio_user_sequences`);
        for (const row of res.rows) {
            await client.query(row.drop_sequence);
        }
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

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        await worker.publish({ some: 'data' });

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);
        await listener.fired();

        expect(listener.count).to.equal(1);

        await drained.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle a failing job', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const [id] = await worker.publish({ some: 'data' });

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
            throw new Error('Uh oh');
        });

        await worker.subscribe(listener);
        await listener.fired();

        expect(listener.count).to.equal(1);

        await drained.fired();

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

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const drainedRetries = internals.instrument((job) => {});
        worker.on('drainRetries', drainedRetries);

        await worker.publish({ some: 'data' });

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
            throw new Error('Uh oh');
        });

        const retrier = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
            expect(job.error_count).to.equal(1);
        });

        await worker.subscribe(listener);
        await listener.fired();

        expect(listener.count).to.equal(1);

        await worker.retry(retrier);
        await retrier.fired();

        expect(retrier.count).to.equal(1);

        await drained.fired();
        await drainedRetries.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle two jobs', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        await worker.publish({ some: 'data' });
        await worker.publish({ some: 'data' });

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await listener.fired();
        await listener.fired();

        expect(listener.count).to.equal(2);

        await drained.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can retry two failed jobs', async () => {

        const worker = new Porker({ connection, queue: 'test', retryDelay: '10 milliseconds' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const drainedRetries = internals.instrument((job) => {});
        worker.on('drainRetries', drainedRetries);

        await worker.publish({ some: 'data' });
        await worker.publish({ some: 'data' });

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
            throw new Error('Uh oh');
        });

        const retrier = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
            expect(job.error_count).to.equal(1);
        });

        await worker.subscribe(listener);
        await listener.fired();
        await listener.fired();
        expect(listener.count).to.equal(2);

        await worker.retry(retrier);
        await retrier.fired();
        await retrier.fired();
        expect(retrier.count).to.equal(2);

        await drained.fired();
        await drainedRetries.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can bulk publish jobs', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        await worker.publish([{ some: 'data' }, { some: 'data' }]);

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await listener.fired();
        await listener.fired();
        expect(listener.count).to.equal(2);

        await drained.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle a publish after a subscription', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await worker.publish({ some: 'data' });
        await listener.fired();

        expect(listener.count).to.equal(1);

        await drained.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can handle two publishes after a subscription', async () => {

        const worker = new Porker({ connection, queue: 'test' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await worker.publish({ some: 'data' });
        await listener.fired();

        await worker.publish({ some: 'data' });
        await listener.fired();

        expect(listener.count).to.equal(2);

        await drained.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * from test_jobs');
        expect(res.rowCount).to.equal(0);

        await worker.end();
    });

    it('can timeout a job', async () => {

        const worker = new Porker({ connection, queue: 'test', timeout: 1 });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
            await timeout(10);
        });

        await worker.subscribe(listener);

        await worker.publish({ some: 'data' });
        await listener.fired();

        await drained.fired();

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

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
        });

        await worker.subscribe(listener);

        await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await listener.fired();
        await listener.fired();
        expect(listener.count).to.equal(2);

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        const row = Object.assign({}, res.rows[0]);
        expect(row).to.contain({ error_count: 0, args: { timer: 'data' } });
        expect(row.repeat_every).to.not.equal(null);

        await worker.end();
    });

    it('can retry a failed job and reset it', async () => {

        const worker = new Porker({ connection, queue: 'test', retryDelay: '10 milliseconds' });
        await worker.create();

        const drained = internals.instrument((job) => {});
        worker.on('drain', drained);

        const drainedRetries = internals.instrument((job) => {});
        worker.on('drainRetries', drainedRetries);

        const listener = internals.instrument((job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
            throw new Error('Uh oh');
        });

        const retrier = internals.instrument((job) => {

            expect(job.args).to.equal({ timer: 'data' });
        });

        await worker.subscribe(listener);

        const [id] = await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await listener.fired();

        expect(listener.count).to.equal(1);

        await worker.retry(retrier);
        await retrier.fired();
        await drainedRetries.fired(); // fires immediately after adding handler

        expect(retrier.count).to.equal(1);

        await drained.fired();
        await drainedRetries.fired();

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
