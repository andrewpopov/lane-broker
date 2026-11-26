import fs from 'node:fs';
import { parse as parsePgConnectionString } from 'pg-connection-string';
import { isSocketQueueUrl, laneQueueUrlProblem } from './config.js';
import { connect } from './lane-db.js';


export const QUEUE_CONNECT_TIMEOUT_MS = 5_000;
export const QUEUE_QUERY_TIMEOUT_MS = 15_000;
export const QUEUE_PROBE = { attempts: 1, connectTimeoutMs: 2_000, queryTimeoutMs: 3_000 };
const MAX_ATTEMPTS = 6;
const BACKOFF_BASE_MS = 100;
const BACKOFF_CAP_MS = 2_000;
const TOO_MANY_CONNECTIONS = '53300';
// a 53300 happens before anything is sent, so every call retries it; a guard kill may follow a commit, so only idempotent calls retry that

// SQL parameter types in order (a call sends as many as it has arguments, so a call that names none of the trailing defaults runs against an older schema); `idempotent` says whether a call that may already have committed can be sent again
const FUNCTIONS = {
  ld_queue_ref: { types: [], idempotent: true },
  ld_enqueue_fenced: { types: ['text', 'uuid', 'text', 'text', 'text', 'jsonb', 'jsonb', 'jsonb', 'text[]', 'real', 'bigint', 'int', 'int', 'boolean', 'text', 'real', 'text[]', 'text[]', 'real'], idempotent: false },
  ld_get: { types: ['uuid'], idempotent: true },
  ld_list_mine: { types: ['text[]', 'int'], idempotent: true },
  ld_withdraw: { types: ['uuid'], idempotent: false },
  ld_cancel: { types: ['uuid'], idempotent: false },
  ld_placeable: { types: ['text', 'text[]'], idempotent: true },
  ld_placeable_detail: { types: ['text', 'text[]'], idempotent: true },
  // lane_admin only (`lane pull gc`); every one is safe to send again
  ld_prune_candidates: { types: ['interval', 'interval', 'timestamptz', 'uuid', 'int'], idempotent: true },
  ld_prune: { types: ['uuid[]', 'interval'], idempotent: true },
  ld_blobs_live: { types: ['jsonb'], idempotent: true },
};

const intervalOf = (ms) => `${Math.ceil(ms / 1000)} seconds`;

export class QueueError extends Error {
  constructor(message, { code = null, cause } = {}) {
    super(message, { cause });
    this.name = 'QueueError';
    this.code = code;
  }
}

const TRANSIENT_SQLSTATES = new Set(['53300', '57P01', '57P02', '57P03', '57014', '55P03', '40P01', '40001']);
const TLS_FAILURE_CODE = /^(ERR_TLS_|ERR_SSL_|CERT_|UNABLE_TO_|DEPTH_ZERO_|SELF_SIGNED_|HOSTNAME_MISMATCH)/;

/** A certificate or protocol failure answers the same way on every retry, so it is a fault to fix and never an outage to wait out. */
export function tlsFailure(err) {
  for (let cause = err, depth = 0; cause && depth < 5; cause = cause.cause, depth++) if (TLS_FAILURE_CODE.test(String(cause.code ?? ''))) return true;
  return false;
}

/** Only a transport failure, a timeout, 53300, 57P01 or 08xxx says the queue cannot be reached; any other server SQLSTATE will answer the same way again. */
export function isQueueUnreachable(err) {
  if (err?.name !== 'QueueError' || tlsFailure(err)) return false;
  const code = String(err.code ?? '');
  if (TRANSIENT_SQLSTATES.has(code) || code.startsWith('08')) return true;
  return err.cause !== undefined && typeof err.cause.severity !== 'string';
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

export function connectionConfig({ url, passwordFile, caFile }, { connectTimeoutMs = QUEUE_CONNECT_TIMEOUT_MS, queryTimeoutMs = QUEUE_QUERY_TIMEOUT_MS } = {}) {
  const deadlines = { connectionTimeoutMillis: connectTimeoutMs, query_timeout: queryTimeoutMs };
  if (isSocketQueueUrl(url)) return { ...parsePgConnectionString(url), ssl: false, ...deadlines };
  const password = readSecretFile(passwordFile, 'password').replace(/\r?\n$/, '');
  if (password === '') throw new QueueError(`queue password file ${passwordFile} is empty`);
  return {
    ...parsePgConnectionString(url),
    password,
    ssl: { ca: readSecretFile(caFile, 'CA'), rejectUnauthorized: true },
    ...deadlines,
  };
}

const jitteredBackoffMs = (attempt, random) => Math.floor(random() * Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt));

// `connectClient` and `sleep` are injectable so a test can reach a database without TLS and observe the retry schedule
export function createQueueClient({
  url, passwordFile, caFile, connectClient = connect, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), random = Math.random,
}) {
  const problem = laneQueueUrlProblem(url);
  if (problem !== null) throw new QueueError(`laneQueueUrl ${problem}`);

  async function invoke(name, args, { attempts = MAX_ATTEMPTS, ...timeouts } = {}) {
    const { idempotent } = FUNCTIONS[name];
    const types = FUNCTIONS[name].types.slice(0, args.length);
    const sql = `SELECT lane_dispatch.${name}(${types.map((type, i) => `$${i + 1}::${type}`).join(', ')}) AS v`;
    const values = args.map((value, i) => (types[i] === 'jsonb' ? JSON.stringify(value) : value));
    for (let attempt = 1; ; attempt++) {
      let client;
      let phase = 'connect';
      try {
        client = await connectClient(connectionConfig({ url, passwordFile, caFile }, timeouts));
        client.on('error', () => {});
        phase = 'query';
        const { rows: [row] } = await client.query(sql, values);
        await client.end();
        return row.v;
      } catch (err) {
        client?.connection?.stream?.destroy();
        if (attempt < attempts && retryable(err, { phase, idempotent })) {
          await sleep(jitteredBackoffMs(attempt, random));
          continue;
        }
        throw err instanceof QueueError ? err : new QueueError(`${name} failed: ${err.message}`, { code: err.code ?? null, cause: err });
      }
    }
  }

  return {
        queueRef: (options) => invoke('ld_queue_ref', [], options),

        enqueueFenced: ({ expectRef, id, repoKey, lane, laneKey, binding, header, snapshot, requiresTags, avoidTags = [], requires = [], cpuCores, minCpuCores, memoryBytes, priority = 0, minProtocol = 1 }) => {
      // only a lane that sets avoidTags (007), requires (008) or an elastic floor (009) needs the longer function, so every other enqueue still works against an older queue
      const elastic = Number.isFinite(minCpuCores) && minCpuCores < cpuCores;
      const trailing = elastic ? [true, null, null, avoidTags, requires, minCpuCores]
        : requires.length > 0 ? [true, null, null, avoidTags, requires]
        : avoidTags.length > 0 ? [true, null, null, avoidTags] : [];
      return invoke('ld_enqueue_fenced', [expectRef, id, repoKey, lane, laneKey, binding, header, snapshot, requiresTags, cpuCores, memoryBytes, priority, minProtocol, ...trailing]);
    },

        get: (id) => invoke('ld_get', [id]),

        listMine: ({ states = null, limit = 100 } = {}) => invoke('ld_list_mine', [states, limit]),

        withdraw: (id) => invoke('ld_withdraw', [id]),

        cancel: (id) => invoke('ld_cancel', [id]),

        placeable: (repoKey, requiresTags, options) => invoke('ld_placeable', [repoKey, requiresTags], options),

        placeableDetail: (repoKey, requiresTags, options) => invoke('ld_placeable_detail', [repoKey, requiresTags], options),

        pruneCandidates: ({ objectsAfterMs, rowsAfterMs, after = null, limit = 200 }) =>
      invoke('ld_prune_candidates', [intervalOf(objectsAfterMs), intervalOf(rowsAfterMs), after?.finished_at ?? null, after?.id ?? null, limit]),

        prune: (ids, rowsAfterMs) => invoke('ld_prune', [ids, intervalOf(rowsAfterMs)]),

        blobsLive: (refs) => invoke('ld_blobs_live', [refs]),
  };
}
