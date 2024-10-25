"use strict";
/**
 * @import { PorkerOptions, TableDefinitions } from "../../"
 * @import { Test } from "tap"
 */

const { Client } = require("pg");
const { Porker } = require("../../");

/**
 * @typedef {object} TestArgs
 * @property {string} name
 * @property {string} start
 */

/**
 * @typedef {object} TestValues
 * @property {Client} db
 * @property {Porker} worker
 * @property {string} channel
 * @property {TableDefinitions} tables
 * @property {TestArgs} args
 */

/**
 * Performs all the work necessary to bootstrap a simple integration test
 * @param {Test} t
 * @param {PorkerOptions} [extra]
 * @returns {Promise<TestValues>}
 */
async function bootstrap(t, extra) {
  const db = new Client(connection);

  const channel = t.name.replaceAll(/\W+/g, "_");

  const tables = {
    jobs: `${channel}_jobs`,
    runs: `${channel}_runs`,
  };

  const worker = new Porker({ connection, tables, ...extra });

  const args = {
    name: t.name,
    start: new Date().toISOString(),
  };

  await db.connect();
  await worker.create();

  t.teardown(async () => {
    await db.end();
    await worker.end();
    await worker.drop();
  });

  return {
    db,
    worker,
    channel,
    tables,
    args,
  };
}

/**
 * Return constants related to a test
 * @param {Test} t
 * @returns {{ args: { name: string; start: string; }; channel: string; tables: TableDefinitions }}
 */
function getConstants(t) {
  const slug = t.name.replaceAll(/\W+/g, "_");

  return {
    args: {
      name: t.name,
      start: new Date().toISOString(),
    },
    channel: slug,
    tables: {
      jobs: `${slug}_jobs`,
      runs: `${slug}_runs`,
    },
  };
}

const connection = { database: "porker-test", user: "porker-test", password: "porker-test" };

module.exports = {
  bootstrap,
  getConstants,
  connection,
};
