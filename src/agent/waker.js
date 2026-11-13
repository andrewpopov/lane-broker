/** A sleep that `poke()` or an abort signal ends early: how a NOTIFY wakes the loops that otherwise poll. */
export class Waker {
  #waiters = new Set();

  wait(ms, signal) {
    return new Promise((resolve) => {
      if (signal?.aborted) return resolve();
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', done);
        this.#waiters.delete(done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      signal?.addEventListener('abort', done, { once: true });
      this.#waiters.add(done);
    });
  }

  poke() {
    for (const done of [...this.#waiters]) done();
  }
}
