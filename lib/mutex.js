'use strict';

module.exports = class AsyncMutex {
    constructor() {

        this.queue = [];
        this.released = 0;
    }

    release() {

        ++this.released;
        while (this.released && this.queue.length) {
            const resolve = this.queue.shift();
            resolve();
            --this.released;
        }
    }

    acquire() {

        let resolve;
        const p = new Promise((r) => {

            resolve = r;
        });

        if (this.released) {
            --this.released;
            resolve();
        }
        else {
            this.queue.push(resolve);
        }

        return p;
    }
};
