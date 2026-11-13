import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { parse as parsePgConnectionString } from 'pg-connection-string';
import { loadGlobalConfig, laneQueueUrlProblem } from '../config.js';
import { scheduleProblem } from '../runner-schedule.js';

export class AgentConfigError extends Error {}

export const AGENT_CONFIG_DIR = path.join(os.homedir(), '.config', 'lane-agent');

export const MAX_SPEED_FACTOR = 4;

const DEFAULTS = { stopClaimingBeforeCloseMs: 30 * 60_000, requireAcPower: false, pollMs: 15_000, urlWaitMs: 60_000, speedFactor: 1 };
const KNOWN = new Set(['laneQueueUrl', 'passwordFile', 'caFile', 'schedule', ...Object.keys(DEFAULTS)]);

// One descriptor, no symlink followed: the mode and owner checked are those of the bytes returned.
function readPrivateFile(file, uid) {
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

// `config.json`, the password file and the CA are each 0600 and ours; TLS is verify-full and the password never enters a connection string.
export function loadAgentConfig({ file = path.join(AGENT_CONFIG_DIR, 'config.json'), brokerConfig = loadGlobalConfig(), uid = process.getuid() } = {}) {
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
  if (brokerConfig.remoteBlobCache !== true) throw new AgentConfigError('the broker config must have "remoteBlobCache": true: a pulled snapshot is only runnable from the blob cache');

  const target = parseQueueUrl(cfg.laneQueueUrl);
  const dir = path.dirname(file);
  const password = readPrivateFile(path.resolve(dir, cfg.passwordFile ?? 'password'), uid).trim();
  const ca = readPrivateFile(path.resolve(dir, cfg.caFile ?? 'ca.pem'), uid);
  if (cfg.schedule !== undefined) {
    const problem = scheduleProblem(cfg.schedule);
    if (problem) throw new AgentConfigError(`${file}: ${problem}`);
  }
  if (cfg.requireAcPower !== undefined && typeof cfg.requireAcPower !== 'boolean') throw new AgentConfigError('"requireAcPower" must be a boolean');

  return {
    pg: { ...target, password, ssl: { ca, rejectUnauthorized: true, servername: target.host } },
    schedule: cfg.schedule ?? null,
    stopClaimingBeforeCloseMs: positiveNumber(cfg, 'stopClaimingBeforeCloseMs'),
    requireAcPower: cfg.requireAcPower ?? DEFAULTS.requireAcPower,
    pollMs: positiveNumber(cfg, 'pollMs'),
    urlWaitMs: positiveNumber(cfg, 'urlWaitMs'),
    speedFactor: positiveNumber(cfg, 'speedFactor', { max: MAX_SPEED_FACTOR }),
  };
}
