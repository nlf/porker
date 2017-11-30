'use strict';

module.exports = {
    connection: Symbol('connection'),
    fn: Symbol('fn'),
    healthcheck: Symbol('healthcheck'),
    healthcheckHandler: Symbol('healthcheckHandler'),
    loop: Symbol('loop'),
    pg: Symbol('pg'),
    repeat: Symbol('repeat'),
    repeatRetries: Symbol('repeatRetries'),
    retry: Symbol('retry'),
    retryLoop: Symbol('retryLoop'),
    retryTimer: Symbol('retryTimer'),
    retryWorker: Symbol('retryWorking'),
    sql: Symbol('sql'),
    stopped: Symbol('stopped'),
    working: Symbol('working')
};
