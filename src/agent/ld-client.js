import pg from 'pg';

export class LdClosedError extends Error {}

export const PROTOCOL = 1;

export const NOT_LIVE = Object.freeze({ skipped: 'not-live' });

// Retried verbatim after a reconnect. Everything else (42501 no principal, 22023 bad input, a bug) fails loud and leaves live executions alone.
const RETRYABLE_SQLSTATES = new Set(['57P01', '57P02', '57P03', '57014', '55P03', '53300']);
const NETWORK_ERRORS = new Set(['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'ENOTFOUND', 'EAI_AGAIN']);
// pg's client-side bounds (query_timeout, connectionTimeoutMillis) surface as message-only errors.
const DEAD_CONNECTION_MESSAGE = /Connection terminated|not queryable|connection error|Connection ended|Query read timeout|^timeout expired$/i;

const TURN_ATTEMPTS = 2;
const YIELD = Symbol('yield');

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
 * The agent's door to lane_dispatch: one call connection and one LISTEN connection, every call through one FIFO. Each new call
 * connection starts with `ld_register`, before any queued call. A turn makes at most TURN_ATTEMPTS attempts; a call still failing
 * retryably goes to the tail with the same request, so no call holds the head of the queue for long and `persist` runs once, on the real answer.
 */
export class LdClient {
  registered = null;
  #client = null;
  #listenClient = null;
  #tail = Promise.resolve();
  #closed = false;
  #sleepers = new Set();

  constructor({ pg: pgConfig, version, schedule = null, onWake, onCall, backoff = { minMs: 250, maxMs: 15_000 }, random = Math.random, connect = connectPg, closeTimeoutMs = 3000 }) {
    Object.assign(this, { pgConfig, version, schedule, onWake, onCall, backoff, random, connect, closeTimeoutMs });
  }

  #sleep(ms) {
    if (this.#closed) return Promise.resolve();
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
    if (this.#closed) {
      client.end().catch(() => {});
      throw new LdClosedError('ld client closed');
    }
    client.on('error', () => { if (this.#client === client) this.#client = null; });
    this.#client = client;
    try {
      this.registered = await this.#sql('ld_register', [this.version, PROTOCOL, this.schedule === null ? null : JSON.stringify(this.schedule)]);
    } catch (err) {
      this.#drop();
      throw err;
    }
  }

  async #sql(name, args) {
    this.#assertOpen();
    this.onCall?.(name, args);
    const placeholders = args.map((_, i) => `$${i + 1}`).join(', ');
    const { rows: [row] } = await this.#client.query(`SELECT lane_dispatch.${name}(${placeholders}) AS v`, args);
    return row.v;
  }

  async #turn(job, call) {
    for (let attempt = 0; attempt < TURN_ATTEMPTS; attempt++) {
      this.#assertOpen();
      try {
        await this.#connected();
        if (!this.#client) throw new Error('Connection ended before the call was sent');
        return await call();
      } catch (err) {
        if (this.#closed || !isRetryable(err)) throw err;
        this.#drop();
        if (attempt < TURN_ATTEMPTS - 1) await this.#sleep(backoffMs(job.failures, this.backoff, this.random));
        job.failures += 1;
      }
    }
    return YIELD;
  }

  #submit(call, persist) {
    return new Promise((resolve, reject) => {
      const job = { failures: 0 };
      const turn = async () => {
        try {
          const answer = await this.#turn(job, call);
          if (answer === YIELD) return void this.#sleep(backoffMs(job.failures, this.backoff, this.random)).then(() => this.#enqueue(turn));
          if (answer !== NOT_LIVE) await persist?.(answer);
          resolve(answer);
        } catch (err) {
          reject(err);
        }
      };
      this.#enqueue(turn);
    });
  }

  #enqueue(turn) {
    this.#tail = this.#tail.then(turn);
  }

  #run(name, args, { persist, live } = {}) {
    let sent = false;
    return this.#submit(() => {
      if (!sent && live && !live()) return NOT_LIVE;
      sent = true;
      return this.#sql(name, args);
    }, persist);
  }

  async open() {
    await this.#submit(async () => {});
    await this.#connectListen({ reconnect: false });
  }

  async #connectListen({ reconnect }) {
    for (let attempt = 0; !this.#closed; attempt++) {
      let client;
      try {
        client = await this.connect(this.pgConfig);
        if (this.#closed) return await client.end().catch(() => {});
        let died = false;
        const dead = () => { died = true; this.#listenDied(client); };
        client.on('error', dead);
        client.on('end', dead);
        client.on('notification', (msg) => this.onWake?.({ reason: 'notify', channel: msg.channel }));
        for (const channel of ['ld_work', `ld_host_${this.registered.host}`]) await client.query(`LISTEN ${client.escapeIdentifier(channel)}`);
        if (this.#closed) return await client.end().catch(() => {});
        if (died) throw new Error('Connection terminated while subscribing');
        this.#listenClient = client;
        if (reconnect) this.onWake?.({ reason: 'listen-reconnected' });
        return;
      } catch (error) {
        client?.end().catch(() => {});
        if (!reconnect && !isRetryable(error)) throw error;
        this.onWake?.({ reason: 'listen-failed', error });
        await this.#sleep(backoffMs(attempt, this.backoff, this.random));
      }
    }
  }

  #listenDied(client) {
    if (this.#listenClient !== client) return;
    this.#listenClient = null;
    client.end().catch(() => {});
    if (!this.#closed) this.#connectListen({ reconnect: true });
  }

  /** `ld_register` again through the same FIFO: an idempotent upsert, used as a liveness ping when no other call is due. */
  register(opts) {
    return this.#run('ld_register', [this.version, PROTOCOL, this.schedule === null ? null : JSON.stringify(this.schedule)], { ...opts, persist: (answer) => { this.registered = answer; } });
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

  get listening() {
    return this.#listenClient !== null;
  }

  /** True while a call connection exists that has registered: false after a drop, until the next call reconnects. */
  get registeredOnConnection() {
    return this.#client !== null && this.registered !== null;
  }

  /** `end()` waits for the socket to close, which a blackholed network never does: after `closeTimeoutMs` the socket is destroyed. */
  #endBounded(client) {
    if (!client) return Promise.resolve();
    let timer;
    const timedOut = new Promise((resolve) => { timer = setTimeout(() => { client.connection?.stream?.destroy(); resolve(); }, this.closeTimeoutMs); });
    return Promise.race([client.end().catch(() => {}), timedOut]).finally(() => clearTimeout(timer));
  }

  async close() {
    this.#closed = true;
    for (const wake of [...this.#sleepers]) wake();
    const [call, listen] = [this.#client, this.#listenClient];
    this.#client = this.#listenClient = null;
    await Promise.all([call, listen].map((c) => this.#endBounded(c)));
  }
}
