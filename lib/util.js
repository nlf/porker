"use strict";

/**
 * Simple replacement for Promise.defer()
 */
class Deferred {
  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

module.exports = {
  Deferred,
};
