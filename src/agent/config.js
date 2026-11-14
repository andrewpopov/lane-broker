import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { parse as parsePgConnectionString } from 'pg-connection-string';
import { DEFAULT_GLOBAL_CONFIG, defaultBrokerHome, loadGlobalConfig, laneQueueUrlProblem } from '../config.js';
import { defaultStateHome } from '../state.js';
import { S3Error, parseS3Credentials } from '../s3-client.js';
import { scheduleProblem } from '../runner-schedule.js';

export class AgentConfigError extends Error {}

export const AGENT_CONFIG_DIR = path.join(os.homedir(), '.config', 'lane-agent');

export const MAX_SPEED_FACTOR = 4;

// A liveness call (claim or heartbeat) at least this often, in every state: the queue treats a runner silent for 8 of these as gone.
export const LIVENESS_MS = 15_000;

// A dead peer must surface as an error the client retries, not a hung query: keepalive probes, a connect bound, and a client-side bound above the role's 2 s statement_timeout.
export const PG_LIVENESS = Object.freeze({ keepAlive: true, connectionTimeoutMillis: 10_000, query_timeout: 20_000 });

const DEFAULTS = { stopClaimingBeforeCloseMs: 30 * 60_000, requireAcPower: false, pollMs: LIVENESS_MS, speedFactor: 1, fetchBudgetMs: 10 * 60_000 };
const KNOWN = new Set(['laneQueueUrl', 'passwordFile', 'caFile', 'runnerName', 'rustfs', 'rustfsEnvFile', 'brokerHome', 'stateDir', 'schedule', ...Object.keys(DEFAULTS)]);
const RUNNER_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;
const RUSTFS_KEYS = new Set(['endpoint', 'region', 'snapBucket', 'outBucket']);

// One descriptor, no symlink followed: the mode and owner checked are those of the bytes returned.
export function readPrivateFile(file, uid) {
  let fd;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  } catch (err) {
    throw new AgentConfigError(`${file}: unreadable (${err.code ?? err.message})`);
  }
  try {
    const st = fs.fstatSync(fd);
    if (!st.isFile()) throw new AgentConfigError(`${file}: not a regular file`);
    const mode = st.mode & 0o777;
    if (mode !== 0o600) throw new AgentConfigError(`${file}: mode must be 0600, is ${mode.toString(8).padStart(4, '0')}`);
    if (st.uid !== uid) throw new AgentConfigError(`${file}: must be owned by uid ${uid}, is owned by ${st.uid}`);
    return fs.readFileSync(fd, 'utf8');
  } finally {
    fs.closeSync(fd);
  }
}

function parseQueueUrl(raw) {
  const problem = laneQueueUrlProblem(raw);
  if (problem) throw new AgentConfigError(`"laneQueueUrl" ${problem}`);
  const url = new URL(raw);
  if (url.password) throw new AgentConfigError('"laneQueueUrl" must not carry a password; use "passwordFile"');
  if (url.search) throw new AgentConfigError('"laneQueueUrl" takes no query: TLS is always verify-full');
  const { host, port, database, user } = parsePgConnectionString(raw);
  if (!user) throw new AgentConfigError('"laneQueueUrl" needs a user');
  return { host, port: port ? Number(port) : 5432, database, user };
}

function positiveNumber(cfg, name, { max = Infinity } = {}) {
  const v = cfg[name] === undefined ? DEFAULTS[name] : cfg[name];
  if (typeof v !== 'number' || !Number.isFinite(v) || v <= 0 || v > max) throw new AgentConfigError(`"${name}" must be a number in (0, ${max}]`);
  return v;
}

function absolutePath(cfg, name, fallback) {
  const v = cfg[name] ?? fallback;
  if (typeof v !== 'string' || !path.isAbsolute(v)) throw new AgentConfigError(`"${name}" must be an absolute path`);
  return v;
}

function parseRustfs(cfg, dir, uid) {
  const block = cfg.rustfs;
  if (block === null || typeof block !== 'object' || Array.isArray(block)) throw new AgentConfigError('"rustfs" must be an object with an "endpoint"');
  const unknown = Object.keys(block).filter((k) => !RUSTFS_KEYS.has(k));
  if (unknown.length) throw new AgentConfigError(`"rustfs": unknown field(s) ${unknown.join(', ')}`);
  if (typeof block.endpoint !== 'string' || !/^https?:\/\/[^/\s]+\/?$/.test(block.endpoint)) throw new AgentConfigError('"rustfs.endpoint" must be an http(s) origin such as https://host:9000');
  const defaults = DEFAULT_GLOBAL_CONFIG.lanePull.rustfs;
  const named = { region: block.region ?? defaults.region, snapBucket: block.snapBucket ?? defaults.snapBucket, outBucket: block.outBucket ?? defaults.outBucket };
  for (const [name, value] of Object.entries(named)) if (typeof value !== 'string' || value === '') throw new AgentConfigError(`"rustfs.${name}" must be a non-empty string`);
  const envFile = path.resolve(dir, cfg.rustfsEnvFile ?? 'rustfs.env');
  try {
    return { endpoint: block.endpoint, ...named, creds: parseS3Credentials(readPrivateFile(envFile, uid), envFile) };
  } catch (err) {
    if (err instanceof S3Error) throw new AgentConfigError(err.message);
    throw err;
  }
}

// `config.json`, the password file and the CA are each 0600 and ours; TLS is verify-full and the password never enters a connection string.
export function loadAgentConfig({ file = path.join(AGENT_CONFIG_DIR, 'config.json'), brokerConfig, uid = process.getuid() } = {}) {
  let cfg;
  try {
    cfg = JSON.parse(readPrivateFile(file, uid));
  } catch (err) {
    if (err instanceof AgentConfigError) throw err;
    throw new AgentConfigError(`${file}: invalid JSON (${err.message})`);
  }
  if (cfg === null || typeof cfg !== 'object' || Array.isArray(cfg)) throw new AgentConfigError(`${file}: must be a JSON object`);
  const unknown = Object.keys(cfg).filter((k) => !KNOWN.has(k));
  if (unknown.length) throw new AgentConfigError(`${file}: unknown field(s) ${unknown.join(', ')}`);
  const brokerHome = absolutePath(cfg, 'brokerHome', defaultBrokerHome());
  const stateDir = absolutePath(cfg, 'stateDir', defaultStateHome());
  const broker = brokerConfig ?? loadGlobalConfig({ home: brokerHome });
  if (broker.remoteBlobCache !== true) throw new AgentConfigError('the broker config must have "remoteBlobCache": true: a pulled snapshot is only runnable from the blob cache');

  const target = parseQueueUrl(cfg.laneQueueUrl);
  const dir = path.dirname(file);
  const password = readPrivateFile(path.resolve(dir, cfg.passwordFile ?? 'password'), uid).trim();
  const ca = readPrivateFile(path.resolve(dir, cfg.caFile ?? 'ca.pem'), uid);
  if (cfg.schedule !== undefined) {
    const problem = scheduleProblem(cfg.schedule);
    if (problem) throw new AgentConfigError(`${file}: ${problem}`);
  }
  if (cfg.requireAcPower !== undefined && typeof cfg.requireAcPower !== 'boolean') throw new AgentConfigError('"requireAcPower" must be a boolean');

  if (typeof cfg.runnerName !== 'string' || !RUNNER_NAME.test(cfg.runnerName)) throw new AgentConfigError('"runnerName" must be the runner\'s host name as the queue knows it (letters, digits, ".", "_", "-")');
  const rustfs = parseRustfs(cfg, dir, uid);

  return {
    runnerName: cfg.runnerName,
    brokerHome,
    stateDir,
    rustfs,
    fetchBudgetMs: positiveNumber(cfg, 'fetchBudgetMs'),
    pg: { ...target, ...PG_LIVENESS, password, ssl: { ca, rejectUnauthorized: true, servername: target.host } },
    schedule: cfg.schedule ?? null,
    stopClaimingBeforeCloseMs: positiveNumber(cfg, 'stopClaimingBeforeCloseMs'),
    requireAcPower: cfg.requireAcPower ?? DEFAULTS.requireAcPower,
    pollMs: positiveNumber(cfg, 'pollMs', { max: LIVENESS_MS }),
    speedFactor: positiveNumber(cfg, 'speedFactor', { max: MAX_SPEED_FACTOR }),
  };
}
