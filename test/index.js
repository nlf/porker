'use strict';

const Porker = require('../');

const ChildProcess = require('child_process');
const Code = require('code');
const Lab = require('lab');
const Util = require('util');

const exec = Util.promisify(ChildProcess.exec);
const { afterEach, beforeEach, describe, it } = exports.lab = Lab.script();
const expect = Code.expect;

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

    it('does not throw when a queue is specified', async () => {

        expect(() => {

            new Porker({ database: 'porker_test_suite', queue: 'test' });
        }).to.not.throw();
    });
});
