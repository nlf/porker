'use strict';

const Fs = require('fs');
const Keyfob = require('keyfob');
const Pg = require('pg');
const Util = require('util');

const Symbols = require('./symbols');

const timeout = Util.promisify(setTimeout);

class Porker {
  constructor({ host, port, user, password, database, name, errorThreshold = 0, timeout = 15000 }) {

    if (!name) {
      throw new Error('Missing required parameter: name');
    }

    this[Symbols.pg] = new Pg.Client({
      host,
      port,
      user,
      password,
      database
    });

    this[Symbols.sql] = Keyfob.load({ path: './sql', fn: (path) => {

      return Fs.readFileSync(path, 'utf8')
        .replace(/\$\{queue\}/gi, name)
        .replace(/\$\{errorThreshold\}/gi, errorThreshold);
    }});

    this.name = name;
    this.errorThreshold = errorThreshold;
    this.timeout = timeout;

    this[Symbols.pg].on('notification', (msg) => {

      if (msg.channel === `${this.name}_jobs`) {
        this[Symbols.loop]();
      }
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
    let res = await this[Symbols.pg].query(this[Symbols.sql].tableExists);
    if (res.rows[0].exists !== `${this.name}_jobs`) {
      await this[Symbols.pg].query(this[Symbols.sql].createTable);
    }
  }

  async publish(job, { priority, repeat }) {

    await this.connect();
    let res = await this[Symbols.pg].query(this[Symbols.sql].insertJob, [job, priority, repeat]);
    await this[Symbols.pg].query(this[Symbols.sql].notifyQueue);
    return res.rows[0];
  }

  async [Symbols.loop]() {

    if (this[Symbols.working] ||
        !this[Symbols.fn]) {

      return;
    }

    this[Symbols.working] = true;

    while (true) {
      await this[Symbols.pg].query('BEGIN');
      let res = await this[Symbols.pg].query(this[Symbols.sql].lockJob);

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

            await timeout(this.timeout);
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
    let res = await this[Symbols.pg].query(this[Symbols.sql].findRecurring);
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

    await this[Symbols.pg].end();
  }
}

module.exports = Porker;
