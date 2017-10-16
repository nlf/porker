'use strict';

const Porker = require('../');
const Symbols = require('../lib/symbols');

const ChildProcess = require('child_process');
const Code = require('code');
const Lab = require('lab');
const Util = require('util');

const exec = Util.promisify(ChildProcess.exec);
const timeout = Util.promisify(setTimeout);
const { afterEach, beforeEach, describe, it } = exports.lab = Lab.script();
const expect = Code.expect;

const internals = {};
internals.instrument = (fn) => {

    const f = async function (job) {

        await fn(job);
        f.called = true;
        ++f.count;
    };

    f.called = false;
    f.count = 0;
    f.fired = async () => {

        while (!f.called) {
            await Util.promisify(setImmediate)();
        }

        return f.called;
    };

    f.reset = () => {

        f.called = false;
    };

    return f;
};

describe('Porker', () => {

    beforeEach(async () => {

        await exec('createdb porker_test_suite');
    });

    afterEach(async () => {

        await exec('dropdb porker_test_suite');
    });

    it('throws when no queue is specified', async () => {

        expect(() => {

            new Porker({ database: 'porker_test_suite' });
        }).to.throw('Missing required parameter: queue');
    });

    it('throws when a subscriber is added twice', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        await worker.subscribe(async () => {});
        await expect(worker.subscribe(async () => {})).to.reject('A subscriber has already been added to this queue');

        await worker.end();
    });

    it('does not throw when queues have a dash', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test-queue' });
        await expect(worker.create()).to.not.reject();

        await worker.end();
    });

    it('can create its own table', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT column_name FROM information_schema.columns WHERE table_name = \'test_jobs\'');
        expect(res.rowCount).to.equal(6);

        const rows = res.rows.map((row) => Object.assign({}, row));
        expect(rows).to.contain({ column_name: 'id' });
        expect(rows).to.contain({ column_name: 'priority' });
        expect(rows).to.contain({ column_name: 'started_at' });
        expect(rows).to.contain({ column_name: 'repeat_every' });
        expect(rows).to.contain({ column_name: 'error_count' });
        expect(rows).to.contain({ column_name: 'args' });

        await worker.end();
    });

    it('can handle a single job', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        await worker.publish({ some: 'data' });

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);
        await listener.fired();

        expect(listener.count).to.equal(1);

        await worker.end();
    });

    it('can handle two jobs', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        await worker.publish({ some: 'data' });
        await worker.publish({ some: 'data' });

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await listener.fired();
        listener.reset();
        await listener.fired();

        expect(listener.count).to.equal(2);

        await worker.end();
    });

    it('can handle a publish after a subscription', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await worker.publish({ some: 'data' });
        await listener.fired();

        expect(listener.count).to.equal(1);

        await worker.end();
    });

    it('can handle two publishes after a subscription', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
        });

        await worker.subscribe(listener);

        await worker.publish({ some: 'data' });
        await listener.fired();

        listener.reset();
        await worker.publish({ some: 'data' });
        await listener.fired();

        expect(listener.count).to.equal(2);

        await worker.end();
    });

    it('can timeout a job', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test', timeout: 1 });
        await worker.create();

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ some: 'data' });
            await timeout(10);
        });

        await worker.subscribe(listener);

        await worker.publish({ some: 'data' });
        await listener.fired();

        const db = worker[Symbols.pg];
        const res = await db.query('SELECT * FROM test_jobs');
        expect(res.rowCount).to.equal(1);
        const row = Object.assign({}, res.rows[0]);
        expect(row).to.contain({ error_count: 1, args: { some: 'data' } });

        await worker.end();
    });

    it('can create a recurring job', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
        });

        await worker.subscribe(listener);

        await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await listener.fired();

        listener.reset();
        await listener.fired();
        expect(listener.count).to.equal(2);

        await worker.end();
    });

    it('can unpublish a recurring job (using full job object)', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
        });

        await worker.subscribe(listener);

        const job = await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await listener.fired();

        listener.reset();
        await listener.fired();
        expect(listener.count).to.equal(2);

        listener.reset();
        await worker.unpublish(job);

        await Promise.race([
            listener.fired(),
            timeout(150)
        ]);

        expect(listener.count).to.equal(2);

        await worker.end();
    });

    it('can unpublish a recurring job (using only job id)', async () => {

        const worker = new Porker({ database: 'porker_test_suite', queue: 'test' });
        await worker.create();

        const listener = internals.instrument(async (job) => {

            expect(job.args).to.equal({ timer: 'data' });
            expect(job.repeat_every).to.not.equal(null);
        });

        await worker.subscribe(listener);

        const job = await worker.publish({ timer: 'data' }, { repeat: '100 milliseconds' });
        await listener.fired();

        listener.reset();
        await listener.fired();
        expect(listener.count).to.equal(2);

        listener.reset();
        await worker.unpublish(job.id);

        await Promise.race([
            listener.fired(),
            timeout(150)
        ]);

        expect(listener.count).to.equal(2);

        await worker.end();
    });
});
