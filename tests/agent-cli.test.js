import test, { describe, before, after } from 'node:test';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { BIN } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import assert from 'node:assert/strict';
import { checkAgent, agentCommand, reachPg } from '../src/agent/cli.js';
import { AgentConfigError } from '../src/agent/config.js';
import { createS3Client } from '../src/s3-client.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { assertCanaryFails } from './helpers/mutant-module.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World } from './lane-dispatch-fixture.js';
import { agentPg } from './agent-harness.js';

const capture = () => { const lines = []; return { lines, write: (s) => lines.push(s), text: () => lines.join('') }; };
const configFor = (stub, extra = {}) => ({
  runnerName: 'wintop', pg: { host: 'pitelite', port: 5432, user: 'lane_agent_wintop' },
  rustfs: { endpoint: stub.endpoint, region: STUB_REGION, snapBucket: 'lane-snap', outBucket: 'lane-out', creds: STUB_CREDS, ...extra },
});
const fresh = async () => {
  const stub = await startS3Stub();
  stub.objects.set('lane-snap/.probe', { body: Buffer.from('probe'), lastModified: Date.now() });
  return stub;
};
const check = async (stub, { load, pg = async () => {}, config = configFor(stub), makeS3 } = {}) => {
  const out = capture();
  const { checkAgent: run } = load ? await load('agent/cli.js') : { checkAgent };
  const { exitCode } = await run({ loadConfig: () => config, probePg: pg, out, ...(makeS3 ? { makeS3 } : {}) });
  return { exitCode, text: out.text() };
};
const failsWith = (fetchImpl) => (rustfs, options) => createS3Client({ endpoint: rustfs.endpoint, region: rustfs.region, creds: rustfs.creds, ...options, fetchImpl });
const denyPut = failsWith(async (url, init) => (init?.method === 'PUT' ? new Response('<Error><Code>AccessDenied</Code></Error>', { status: 403 }) : fetch(url, init)));

const scenarios = {
  '--check passes when pg answers, the probe object reads, and the host probe PUTs and HEADs': async (stub, load) => {
    const { exitCode, text } = await check(stub, { load });
    assert.equal(exitCode, 0, text);
    assert.match(text, /GET lane-snap\/\.probe: ok/);
    assert.match(text, /PUT lane-out\/wintop\/\.probe: ok/);
    assert.match(text, /HEAD lane-out\/wintop\/\.probe: ok/);
    assert.deepEqual(stub.requests.map((r) => `${r.method} ${r.path}`), ['GET /lane-snap/.probe', 'PUT /lane-out/wintop/.probe', 'HEAD /lane-out/wintop/.probe']);
  },

  '--check fails when the readable probe object is missing': async (stub, load) => {
    stub.objects.delete('lane-snap/.probe');
    const { exitCode, text } = await check(stub, { load });
    assert.equal(exitCode, 1);
    assert.match(text, /GET lane-snap\/\.probe: missing/);
  },

  '--check fails when the key may not PUT under its host prefix, and when the secret is wrong': async (stub, load) => {
    const denied = await check(stub, { load, makeS3: denyPut });
    assert.equal(denied.exitCode, 1);
    assert.match(denied.text, /PUT lane-out\/wintop\/\.probe: .*403/);
    const wrong = await check(stub, { load, config: configFor(stub, { creds: { accessKeyId: STUB_CREDS.accessKeyId, secretAccessKey: 'not-it' } }) });
    assert.equal(wrong.exitCode, 1);
    assert.match(wrong.text, /GET lane-snap\/\.probe: .*403/);
  },

  '--check fails when pg does not answer, and an object it cannot HEAD back fails the check': async (stub, load) => {
    const down = await check(stub, { load, pg: async () => { throw Object.assign(new Error('x'), { code: 'ECONNREFUSED' }); } });
    assert.equal(down.exitCode, 1);
    assert.match(down.text, /pg pitelite:5432 as lane_agent_wintop: ECONNREFUSED/);
    const vanishing = await check(stub, { load, makeS3: failsWith(async (url, init) => (init?.method === 'HEAD' ? new Response(null, { status: 404 }) : fetch(url, init))) });
    assert.equal(vanishing.exitCode, 1);
    assert.match(vanishing.text, /HEAD lane-out\/wintop\/\.probe: the object just written is not there/);
  },
};

const CLI = 'agent/cli.js';
const canaries = [
  ['--check fails when the readable probe object is missing', ["if (!(await s3.getObjectToFile(rustfs.snapBucket, SNAP_PROBE_KEY, path.join(dir, 'snap'), { deadlineMs: PROBE_DEADLINE_MS })).exists) throw new Error('missing: the admin writes it once');", "await s3.getObjectToFile(rustfs.snapBucket, SNAP_PROBE_KEY, path.join(dir, 'snap'), { deadlineMs: PROBE_DEADLINE_MS });"]],
  ['--check fails when pg does not answer, and an object it cannot HEAD back fails the check', ["if (!head.exists || head.size !== PROBE_BODY.length) throw new Error('the object just written is not there');", '']],
  ['--check fails when the readable probe object is missing', ['steps.every(([, error]) => error === null) ? 0 : 1', '0']],
];

describe('lane agent --check against a fake S3', () => {
  let stub;
  const run = async (fn, load) => { stub = await fresh(); try { await fn(stub, load); } finally { await stub.close(); } };
  for (const [name, scenario] of Object.entries(scenarios)) test(name, () => run(scenario, undefined));
  for (const [name, mutation] of canaries) {
    test(`canary: with the guard removed, "${name}" fails on its own assertion`, async (t) => {
      const failure = await assertCanaryFails(CLI, mutation, (load) => run(scenarios[name], load));
      t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
    });
  }
});

test('--check on a config that does not load prints the reason and exits 1; an unknown flag exits 2', async () => {
  const err = capture();
  const bad = () => { throw new AgentConfigError('no config here'); };
  assert.equal((await agentCommand(['--check'], { loadConfig: bad, err })).exitCode, 1);
  assert.match(err.text(), /no config here/);
  assert.equal((await agentCommand(['--nope'], { err: capture() })).exitCode, 2);
});

test('reachPg rejects for a port nothing listens on', async () => {
  await assert.rejects(reachPg({ host: '127.0.0.1', port: 1, user: 'u', database: 'd', connectionTimeoutMillis: 2000 }), (e) => e.code === 'ECONNREFUSED');
});

describe('lane agent --check pg probe on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  test('reachPg logs in as the agent and refuses an unknown role', async () => {
    const w = await World.create(cluster);
    await reachPg(agentPg(w));
    await assert.rejects(reachPg({ ...agentPg(w), user: 'no_such_role' }));
  });
});

test('lane agent --check --config <dir> reads everything from that dir with an env of PATH and HOME, and a hostile LANE_BROKER_HOME that it ignores', async () => {
  const stub = await fresh();
  try {
    const home = makeTmpDir('agent-home-');
    fs.mkdirSync(path.join(home, '.config', 'lane-broker'), { recursive: true });
    fs.writeFileSync(path.join(home, '.config', 'lane-broker', 'config.json'), JSON.stringify({ remoteBlobCache: true }));
    const dir = makeTmpDir('agent-cfg-');
    const write = (name, text) => fs.writeFileSync(path.join(dir, name), text, { mode: 0o600 });
    write('config.json', JSON.stringify({ laneQueueUrl: 'postgres://lane_agent_wintop@127.0.0.1:1/lane_queue', runnerName: 'wintop', rustfs: { endpoint: stub.endpoint } }));
    write('password', 'pw\n');
    write('ca.pem', 'CA\n');
    write('rustfs.env', `AWS_ACCESS_KEY_ID=${STUB_CREDS.accessKeyId}\nAWS_SECRET_ACCESS_KEY=${STUB_CREDS.secretAccessKey}\n`);
    const child = spawn(process.execPath, [BIN, 'agent', '--check', '--config', dir], { env: { PATH: process.env.PATH, HOME: home, LANE_BROKER_HOME: path.join(home, 'hostile'), LANE_BROKER_STATE: path.join(home, 'hostile-state') } });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { out += d; });
    const code = await new Promise((resolve) => child.on('close', resolve));
    assert.match(out, /GET lane-snap\/\.probe: ok/);
    assert.match(out, /PUT lane-out\/wintop\/\.probe: ok/);
    assert.match(out, /HEAD lane-out\/wintop\/\.probe: ok/);
    assert.match(out, /pg 127\.0\.0\.1:1 as lane_agent_wintop: ECONNREFUSED/);
    assert.equal(code, 1, 'only the unreachable pg fails the check');
    const missing = await new Promise((resolve) => {
      const c = spawn(process.execPath, [BIN, 'agent', '--check', '--config', path.join(dir, 'nope')], { env: { PATH: process.env.PATH, HOME: home } });
      let text = '';
      c.stderr.on('data', (d) => { text += d; });
      c.on('close', (status) => resolve({ status, text }));
    });
    assert.equal(missing.status, 1);
    assert.match(missing.text, /nope\/config\.json: unreadable/);
  } finally {
    await stub.close();
  }
});
