import fs from 'node:fs';
import { parse as parsePgConnectionString } from 'pg-connection-string';
import { laneQueueUrlProblem } from './config.js';
import { connect } from './lane-db.js';


export const QUEUE_CONNECT_TIMEOUT_MS = 5_000;
export const QUEUE_QUERY_TIMEOUT_MS = 15_000;
const MAX_ATTEMPTS = 6;
const BACKOFF_BASE_MS = 100;
const BACKOFF_CAP_MS = 2_000;
const TOO_MANY_CONNECTIONS = '53300';
// a 53300 happens before anything is sent, so every call retries it; a guard kill may follow a commit, so only idempotent calls retry that

// SQL parameter types in order; `idempotent` says whether a call that may already have committed can be sent again
const FUNCTIONS = {
  ld_queue_ref: { types: [], idempotent: true },
  ld_enqueue_fenced: { types: ['text', 'uuid', 'text', 'text', 'text', 'jsonb', 'jsonb', 'jsonb', 'text[]', 'real', 'bigint', 'int'], idempotent: false },
  ld_get: { types: ['uuid'], idempotent: true },
  ld_list_mine: { types: ['text[]', 'int'], idempotent: true },
  ld_withdraw: { types: ['uuid'], idempotent: false },
  ld_cancel: { types: ['uuid'], idempotent: false },
  ld_attach_urls: { types: ['uuid', 'int', 'jsonb'], idempotent: true },
  ld_log_read: { types: ['uuid', 'int', 'int'], idempotent: true },
  ld_log_ack: { types: ['uuid', 'int', 'int'], idempotent: true },
  ld_placeable: { types: ['text', 'text[]'], idempotent: true },
};

export class QueueError extends Error {
  constructor(message, { code = null, cause } = {}) {
    super(message, { cause });
    this.name = 'QueueError';
    this.code = code;
  }
}

const isServerError = (err) => typeof err?.severity === 'string';

const outcomeUnknown = (err) => !isServerError(err) || err.code === '57P01';

function retryable(err, { phase, idempotent }) {
  if (err.code === TOO_MANY_CONNECTIONS) return true;
  return phase === 'query' && idempotent && outcomeUnknown(err);
}

const readSecretFile = (file, what) => {
  try {
    return fs.readFileSync(file, 'utf8');
  } catch (err) {
    throw new QueueError(`queue ${what} file ${file} unreadable (${err.code ?? err.message})`);
  }
};

function connectionConfig({ url, passwordFile, caFile }) {
  const password = readSecretFile(passwordFile, 'password').replace(/\r?\n$/, '');
  if (password === '') throw new QueueError(`queue password file ${passwordFile} is empty`);
  return {
    ...parsePgConnectionString(url),
    password,
    ssl: { ca: readSecretFile(caFile, 'CA'), rejectUnauthorized: true },
    connectionTimeoutMillis: QUEUE_CONNECT_TIMEOUT_MS,
    query_timeout: QUEUE_QUERY_TIMEOUT_MS,
  };
}

const jitteredBackoffMs = (attempt, random) => Math.floor(random() * Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt));

// `connectClient` and `sleep` are injectable so a test can reach a database without TLS and observe the retry schedule
export function createQueueClient({
  url, passwordFile, caFile, connectClient = connect, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), random = Math.random,
}) {
  const problem = laneQueueUrlProblem(url);
  if (problem !== null) throw new QueueError(`laneQueueUrl ${problem}`);

  async function invoke(name, args) {
    const { types, idempotent } = FUNCTIONS[name];
    const sql = `SELECT lane_dispatch.${name}(${types.map((type, i) => `$${i + 1}::${type}`).join(', ')}) AS v`;
    const values = args.map((value, i) => (types[i] === 'jsonb' ? JSON.stringify(value) : value));
    for (let attempt = 1; ; attempt++) {
      let client;
      let phase = 'connect';
      try {
        client = await connectClient(connectionConfig({ url, passwordFile, caFile }));
        client.on('error', () => {});
        phase = 'query';
        const { rows: [row] } = await client.query(sql, values);
        await client.end();
        return row.v;
      } catch (err) {
        client?.connection?.stream?.destroy();
        if (attempt < MAX_ATTEMPTS && retryable(err, { phase, idempotent })) {
          await sleep(jitteredBackoffMs(attempt, random));
          continue;
        }
        throw err instanceof QueueError ? err : new QueueError(`${name} failed: ${err.message}`, { code: err.code ?? null, cause: err });
      }
    }
  }

  return {
        queueRef: () => invoke('ld_queue_ref', []),

        enqueueFenced: ({ expectRef, id, repoKey, lane, laneKey, binding, header, snapshot, requiresTags, cpuCores, memoryBytes, priority = 0 }) =>
      invoke('ld_enqueue_fenced', [expectRef, id, repoKey, lane, laneKey, binding, header, snapshot, requiresTags, cpuCores, memoryBytes, priority]),

        get: (id) => invoke('ld_get', [id]),

        listMine: ({ states = null, limit = 100 } = {}) => invoke('ld_list_mine', [states, limit]),

        withdraw: (id) => invoke('ld_withdraw', [id]),

        cancel: (id) => invoke('ld_cancel', [id]),

        attachUrls: (id, epoch, urls) => invoke('ld_attach_urls', [id, epoch, urls]),

        logRead: (id, epoch, afterSeq) => invoke('ld_log_read', [id, epoch, afterSeq]),

        logAck: (id, epoch, uptoSeq) => invoke('ld_log_ack', [id, epoch, uptoSeq]),

        placeable: (repoKey, requiresTags) => invoke('ld_placeable', [repoKey, requiresTags]),
  };
}
