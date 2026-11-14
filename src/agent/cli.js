import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { AGENT_CONFIG_DIR, AgentConfigError, loadAgentConfig } from './config.js';
import pg from 'pg';
import { AgentOwnedError } from './store.js';
import { SNAP_PROBE_KEY } from './fetch.js';
import { outProbeKey } from './upload.js';
import { s3ClientOf, startAgent } from './run.js';

const PROBE_BODY = 'lane agent probe\n';
const PROBE_DEADLINE_MS = 10_000;

const flagValue = (args, flag) => { const at = args.indexOf(flag); return at >= 0 ? args[at + 1] : undefined; };

const pkgVersion = () => JSON.parse(fs.readFileSync(fileURLToPath(new URL('../../package.json', import.meta.url)), 'utf8')).version;

export async function reachPg(pgConfig) {
  const client = new pg.Client(pgConfig);
  client.on('error', () => {});
  await client.connect();
  try {
    await client.query('SELECT 1');
  } finally {
    await client.end().catch(() => {});
  }
}

/**
 * The RustFS permissions a runner key must have: a GET of the known readable object (which also proves the key authenticates) and a PUT
 * then HEAD of its own probe under its host prefix. Each step is `[label, error|null]`.
 */
async function checkRustfs({ rustfs, runnerName }, s3) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-agent-check-'));
  const probeFile = path.join(dir, 'probe');
  fs.writeFileSync(probeFile, PROBE_BODY);
  const outKey = outProbeKey(runnerName);
  const steps = [
    [`GET ${rustfs.snapBucket}/${SNAP_PROBE_KEY}`, async () => {
      if (!(await s3.getObjectToFile(rustfs.snapBucket, SNAP_PROBE_KEY, path.join(dir, 'snap'), { deadlineMs: PROBE_DEADLINE_MS })).exists) throw new Error('missing: the admin writes it once');
    }],
    [`PUT ${rustfs.outBucket}/${outKey}`, () => s3.putObjectFromFile(rustfs.outBucket, outKey, probeFile, { deadlineMs: PROBE_DEADLINE_MS })],
    [`HEAD ${rustfs.outBucket}/${outKey}`, async () => {
      const head = await s3.headObject(rustfs.outBucket, outKey, { deadlineMs: PROBE_DEADLINE_MS });
      if (!head.exists || head.size !== PROBE_BODY.length) throw new Error('the object just written is not there');
    }],
  ];
  try {
    const results = [];
    for (const [label, run] of steps) results.push([label, await run().then(() => null, (err) => err.message)]);
    return results;
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/** `lane agent --check`: the config loads, the queue answers a login, and the runner's RustFS key reads and writes what it must. It registers nothing. */
export async function checkAgent({ loadConfig = loadAgentConfig, probePg = reachPg, makeS3 = s3ClientOf, out = process.stdout } = {}) {
  const config = loadConfig();
  const steps = [
    [`pg ${config.pg.host}:${config.pg.port} as ${config.pg.user}`, await probePg(config.pg).then(() => null, (err) => err.code ?? err.message)],
    ...await checkRustfs(config, makeS3(config.rustfs, { deadlineMs: PROBE_DEADLINE_MS })),
  ];
  for (const [label, error] of steps) out.write(`${label}: ${error ?? 'ok'}\n`);
  return { exitCode: steps.every(([, error]) => error === null) ? 0 : 1 };
}

/** Bosun SIGKILLs at 60 s: a stop with a leftover handle must still exit 0 before that. */
export const EXIT_DEADLINE_MS = 55_000;

const FLAGS_WITH_VALUE = new Set(['--root', '--config']);

/** `lane agent [--check] [--root <remoteRoot>] [--config <dir>]`. Without `--check` it runs until SIGTERM or SIGINT. Every credential comes from files under the config dir (default `~/.config/lane-agent`), never from the environment. */
export async function agentCommand(args, { remoteRoot, configDir = flagValue(args, '--config') ?? AGENT_CONFIG_DIR, loadConfig = () => loadAgentConfig({ file: path.join(configDir, 'config.json') }), signals = process, err = process.stderr, start = startAgent, exitDeadlineMs = EXIT_DEADLINE_MS, ...checkOptions } = {}) {
  const known = new Set(['--check', ...FLAGS_WITH_VALUE]);
  const unknown = args.find((a, i) => a.startsWith('--') && !known.has(a) && !FLAGS_WITH_VALUE.has(args[i - 1]));
  if (unknown !== undefined) {
    err.write(`lane agent: unknown flag "${unknown}"\n`);
    return { exitCode: 2 };
  }
  try {
    if (args.includes('--check')) return await checkAgent({ loadConfig, ...checkOptions });
    const agent = await start({ remoteRoot, config: loadConfig(), version: pkgVersion() });
    await new Promise((resolve) => {
      for (const signal of ['SIGTERM', 'SIGINT']) signals.once(signal, resolve);
    });
    let timer;
    const overdue = new Promise((resolve) => { timer = setTimeout(() => { err.write(`lane agent: stop did not finish within ${exitDeadlineMs} ms; exiting anyway\n`); resolve(); }, exitDeadlineMs); });
    await Promise.race([agent.stop(), overdue]);
    clearTimeout(timer);
    return { exitCode: 0 };
  } catch (error) {
    if (!(error instanceof AgentConfigError || error instanceof AgentOwnedError)) throw error;
    err.write(`lane agent: ${error.message}\n`);
    return { exitCode: 1 };
  }
}
