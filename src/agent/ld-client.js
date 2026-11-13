import pg from 'pg';

export class LdClosedError extends Error {}

// Retried verbatim after a reconnect. Everything else (42501 no principal, 22023 bad input, a bug) fails loud and leaves live executions alone.
const RETRYABLE_SQLSTATES = new Set(['57P01', '57014', '55P03', '53300']);
const NETWORK_ERRORS = new Set(['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'ENOTFOUND', 'EAI_AGAIN']);
const DEAD_CONNECTION_MESSAGE = /Connection terminated|not queryable|connection error|Connection ended/i;

export function isRetryable(err) {
  const code = String(err?.code ?? '');
  return RETRYABLE_SQLSTATES.has(code) || code.startsWith('08') || NETWORK_ERRORS.has(code) || DEAD_CONNECTION_MESSAGE.test(String(err?.message));
}

export function backoffMs(attempt, { minMs, maxMs }, random = Math.random) {
  return Math.min(maxMs, minMs * 2 ** attempt) * (0.5 + random() / 2);
}

const connectPg = async (config) => {
  const client = new pg.Client(config);
  client.on('error', () => {});
  await client.connect();
  return client;
};

/**
 * The agent's door to lane_dispatch: one call connection and one LISTEN connection, every call through one FIFO. `gen` counts
 * connection (re)establishments of either kind plus `reassert` answers. While it is ahead of the last reassert the next job is
 * `ld_reassert`, then `ld_register`, so nothing else reaches a session the queue has not re-adopted; queued calls keep their place.
 * A job is the call plus its `persist` callback: the reassert snapshot is only taken between jobs, so it never sees a phase from
 * before a transition whose call has already returned. `persist(answer, {stale})` is told when `gen` moved while the call was in flight.
 */
export class LdClient {
  gen = 0;
  registered = null;
  #reasserted = -1;
  #client = null;
  #listenClient = null;
  #tail = Promise.resolve();
  #closed = false;
  #sleepers = new Set();

  constructor({ pg: pgConfig, version, schedule = null, snapshot = () => [], onReassert, onWake, onCall, backoff = { minMs: 250, maxMs: 15_000 }, random = Math.random, connect = connectPg }) {
    Object.assign(this, { pgConfig, version, schedule, snapshot, onReassert, onWake, onCall, backoff, random, connect });
  }

  #sleep(ms) {
    return new Promise((resolve) => {
      const done = () => { clearTimeout(timer); this.#sleepers.delete(done); resolve(); };
      const timer = setTimeout(done, ms);
      this.#sleepers.add(done);
    });
  }

  #assertOpen() {
    if (this.#closed) throw new LdClosedError('ld client closed');
  }

  #drop() {
    const client = this.#client;
    this.#client = null;
    client?.end().catch(() => {});
  }

  async #connected() {
    if (this.#client) return;
    const client = await this.connect(this.pgConfig);
    client.on('error', () => { if (this.#client === client) this.#client = null; });
    this.#client = client;
    this.gen += 1;
  }

  async #sql(name, args) {
    this.onCall?.(name, args);
    const placeholders = args.map((_, i) => `$${i + 1}`).join(', ');
    const { rows: [row] } = await this.#client.query(`SELECT lane_dispatch.${name}(${placeholders}) AS v`, args);
    return row.v;
  }

  async #gate() {
    while (this.gen !== this.#reasserted) {
      const gen = this.gen;
      const items = this.snapshot();
      const results = await this.#sql('ld_reassert', [JSON.stringify(items)]);
      if (!Array.isArray(results) || results.length !== items.length) throw new Error(`ld_reassert answered ${JSON.stringify(results)} for ${items.length} item(s)`);
      this.registered = await this.#sql('ld_register', [this.version, this.schedule === null ? null : JSON.stringify(this.schedule)]);
      await this.onReassert?.(items, results);
      this.#reasserted = gen;
    }
  }

  #enqueue(job) {
    const run = this.#tail.then(job);
    this.#tail = run.catch(() => {});
    return run;
  }

  async #attempt(fn) {
    for (let attempt = 0; ; attempt++) {
      this.#assertOpen();
      try {
        await this.#connected();
        await this.#gate();
        return await fn();
      } catch (err) {
        if (this.#closed || !isRetryable(err)) throw err;
        this.#drop();
        await this.#sleep(backoffMs(attempt, this.backoff, this.random));
      }
    }
  }

  #run(name, args, { persist } = {}) {
    return this.#enqueue(async () => {
      let sentAt;
      const answer = await this.#attempt(() => { sentAt = this.gen; return this.#sql(name, args); });
      const stale = this.gen !== sentAt;
      if (answer?.reassert === true) this.gen += 1;
      await persist?.(answer, { stale });
      return answer;
    });
  }

  async open() {
    await this.#enqueue(() => this.#attempt(async () => {}));
    await this.#connectListen({ bump: false });
  }

  async #connectListen({ bump }) {
    for (let attempt = 0; !this.#closed; attempt++) {
      let client;
      try {
        client = await this.connect(this.pgConfig);
        let died = false;
        const dead = () => { died = true; this.#listenDied(client); };
        client.on('error', dead);
        client.on('end', dead);
        client.on('notification', (msg) => this.onWake?.({ reason: 'notify', channel: msg.channel }));
        for (const channel of ['ld_work', `ld_host_${this.registered.host}`]) await client.query(`LISTEN ${client.escapeIdentifier(channel)}`);
        if (this.#closed) return await client.end().catch(() => {});
        if (died) throw new Error('Connection terminated while subscribing');
        this.#listenClient = client;
        if (bump) {
          this.gen += 1;
          this.onWake?.({ reason: 'listen-reconnected' });
        }
        return;
      } catch (err) {
        client?.end().catch(() => {});
        if (!isRetryable(err)) throw err;
        await this.#sleep(backoffMs(attempt, this.backoff, this.random));
      }
    }
  }

  #listenDied(client) {
    if (this.#listenClient !== client) return;
    this.#listenClient = null;
    client.end().catch(() => {});
    if (!this.#closed) this.#connectListen({ bump: true }).catch((error) => this.onWake?.({ reason: 'listen-failed', error }));
  }

  claim(req, freeCpu, freeMem, heldKeys = [], opts) {
    return this.#run('ld_claim', [req, freeCpu, String(freeMem), heldKeys], opts);
  }

  start(id, epoch, token, opts) {
    return this.#run('ld_start', [id, epoch, token], opts);
  }

  heartbeat(id, epoch, token, opts) {
    return this.#run('ld_heartbeat', [id, epoch, token], opts);
  }

  finish(id, epoch, token, result, opts) {
    return this.#run('ld_finish', [id, epoch, token, result], opts);
  }

  release(id, epoch, token, proof, opts) {
    return this.#run('ld_release', [id, epoch, token, proof], opts);
  }

  log(id, epoch, token, seq, bytes, opts) {
    return this.#run('ld_log', [id, epoch, token, seq, bytes], opts);
  }

  async close() {
    this.#closed = true;
    for (const wake of [...this.#sleepers]) wake();
    const [call, listen] = [this.#client, this.#listenClient];
    this.#client = this.#listenClient = null;
    await Promise.all([call, listen].map((c) => c?.end().catch(() => {})));
  }
}
