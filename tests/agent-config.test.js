import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { AgentConfigError, loadAgentConfig } from '../src/agent/config.js';

const URL_OK = 'postgres://lane_agent_wintop@pitelite:5432/lane_queue';
const RUSTFS = { endpoint: 'https://pitelite:9000' };

function setup(overrides = {}, { mode = 0o600 } = {}) {
  const dir = makeTmpDir('agent-config-');
  fs.writeFileSync(path.join(dir, 'password'), 'hunter2\n', { mode: 0o600 });
  fs.writeFileSync(path.join(dir, 'ca.pem'), 'CA PEM\n', { mode: 0o600 });
  fs.writeFileSync(path.join(dir, 'rustfs.env'), 'AWS_ACCESS_KEY_ID=AKIAX\nAWS_SECRET_ACCESS_KEY=s3cret\n', { mode: 0o600 });
  const file = path.join(dir, 'config.json');
  fs.writeFileSync(file, JSON.stringify({ laneQueueUrl: URL_OK, runnerName: 'wintop', rustfs: RUSTFS, ...overrides }), { mode });
  for (const name of ['password', 'ca.pem', 'rustfs.env']) fs.chmodSync(path.join(dir, name), 0o600);
  fs.chmodSync(file, mode);
  return { dir, file };
}

const load = (file, extra = {}) => loadAgentConfig({ file, brokerConfig: { remoteBlobCache: true }, ...extra });
const refused = (file, pattern, extra) => assert.throws(() => load(file, extra), (err) => err instanceof AgentConfigError && pattern.test(err.message), String(pattern));

test('T5 a valid config builds a verify-full pg config with the password from its file and no sslmode anywhere', () => {
  const { file } = setup();
  const config = load(file);
  assert.deepEqual(config.pg, {
    host: 'pitelite', port: 5432, database: 'lane_queue', user: 'lane_agent_wintop', password: 'hunter2',
    keepAlive: true, connectionTimeoutMillis: 10_000, query_timeout: 20_000,
    ssl: { ca: 'CA PEM\n', rejectUnauthorized: true, servername: 'pitelite' },
  });
  assert.deepEqual([config.speedFactor, config.pollMs, config.requireAcPower, config.schedule, config.fetchBudgetMs, config.runnerName], [1, 15_000, false, null, 600_000, 'wintop']);
});

test('T5 a relative passwordFile and caFile resolve against the config directory, not the cwd', () => {
  const { dir, file } = setup({ passwordFile: 'secrets/pw', caFile: 'secrets/ca' });
  fs.mkdirSync(path.join(dir, 'secrets'));
  for (const name of ['pw', 'ca']) fs.writeFileSync(path.join(dir, 'secrets', name), `${name}\n`, { mode: 0o600 });
  const config = load(file);
  assert.deepEqual([config.pg.password, config.pg.ssl.ca], ['pw', 'ca\n']);
});

test('T5 a VIP or failover host is refused', () => {
  for (const url of ['postgres://u@svc:pg-primary:5432/lane_queue', 'postgres://u@pg-primary.homelab:5432/lane_queue', 'postgres://u@svc:pg-primary/lane_queue']) {
    refused(setup({ laneQueueUrl: url }).file, /laneQueueUrl/);
  }
});

test('T5 a password or a query in the URL is refused', () => {
  refused(setup({ laneQueueUrl: 'postgres://lane_agent_wintop:secret@pitelite:5432/lane_queue' }).file, /passwordFile/);
  refused(setup({ laneQueueUrl: `${URL_OK}?sslmode=disable` }).file, /verify-full/);
});

test('T5 a password file that is not mode 0600 is refused', () => {
  const { dir, file } = setup();
  fs.chmodSync(path.join(dir, 'password'), 0o640);
  refused(file, /password: mode must be 0600, is 0640/);
});

test('T5 a CA or password file owned by another uid is refused', () => {
  const { file } = setup();
  refused(file, /must be owned by uid \d+, is owned by/, { uid: process.getuid() + 1 });
});

test('T5 a missing CA is refused', () => {
  const { dir, file } = setup();
  fs.rmSync(path.join(dir, 'ca.pem'));
  refused(file, /ca\.pem: unreadable \(ENOENT\)/);
});

test('T5 a config.json that is not mode 0600, or is a symlink, is refused', () => {
  refused(setup({}, { mode: 0o644 }).file, /config\.json: mode must be 0600/);
  const { dir, file } = setup();
  const link = path.join(dir, 'link.json');
  fs.symlinkSync(file, link);
  refused(link, /unreadable \(ELOOP\)/);
});

test('T5 the broker config must have remoteBlobCache true', () => {
  const { file } = setup();
  for (const remoteBlobCache of [false, undefined, 'true']) refused(file, /remoteBlobCache/, { brokerConfig: { remoteBlobCache } });
});

test('T5 speedFactor must satisfy 0 < x <= 4', () => {
  for (const bad of [0, -1, 4.01, 'fast', null, Infinity]) refused(setup({ speedFactor: bad }).file, /"speedFactor" must be a number in \(0, 4\]/);
  for (const good of [0.25, 1, 4]) assert.equal(load(setup({ speedFactor: good }).file).speedFactor, good);
});

test('T5 unknown fields and a bad schedule are refused', () => {
  refused(setup({ speedfactor: 2 }).file, /unknown field\(s\) speedfactor/);
  refused(setup({ schedule: { timezone: 'Nowhere/Land', available: [] } }).file, /timezone/);
});

test('the RustFS key is read from rustfs.env under the private-file rule, with the broker defaults for region and buckets', () => {
  const { rustfs } = load(setup().file);
  assert.deepEqual(rustfs, { endpoint: 'https://pitelite:9000', region: 'us-east-1', snapBucket: 'lane-snap', outBucket: 'lane-out', creds: { accessKeyId: 'AKIAX', secretAccessKey: 's3cret' } });
  const { dir, file } = setup();
  fs.chmodSync(path.join(dir, 'rustfs.env'), 0o644);
  refused(file, /rustfs\.env: mode must be 0600, is 0644/);
  refused(setup().file, /must be owned by uid \d+, is owned by/, { uid: process.getuid() + 1 });
  const missing = setup();
  fs.rmSync(path.join(missing.dir, 'rustfs.env'));
  refused(missing.file, /rustfs\.env: unreadable \(ENOENT\)/);
  const partial = setup();
  fs.writeFileSync(path.join(partial.dir, 'rustfs.env'), 'AWS_ACCESS_KEY_ID=AKIAX\n', { mode: 0o600 });
  refused(partial.file, /must define AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY/);
});

test('rustfs needs an http(s) origin, runnerName a plain host name, and pollMs may not exceed the 15 s liveness bound', () => {
  refused(setup({ rustfs: {} }).file, /"rustfs\.endpoint"/);
  refused(setup({ rustfs: { endpoint: 'pitelite:9000' } }).file, /"rustfs\.endpoint"/);
  refused(setup({ rustfs: { endpoint: 'https://p:9000', bucket: 'x' } }).file, /unknown field\(s\) bucket/);
  for (const bad of [undefined, '', 'a/b', '../x']) refused(setup({ runnerName: bad }).file, /"runnerName"/);
  refused(setup({ pollMs: 15_001 }).file, /"pollMs" must be a number in \(0, 15000\]/);
  assert.equal(load(setup({ pollMs: 5000, fetchBudgetMs: 1000 }).file).fetchBudgetMs, 1000);
});

test('brokerHome and stateDir are absolute, default to the broker defaults, and the global config is read from brokerHome whatever LANE_BROKER_HOME says', () => {
  const config = load(setup().file);
  assert.deepEqual([config.brokerHome, config.stateDir], [path.join(os.homedir(), '.config', 'lane-broker'), path.join(os.homedir(), '.cache', 'lane-broker')]);
  refused(setup({ brokerHome: 'relative/dir' }).file, /"brokerHome" must be an absolute path/);
  refused(setup({ stateDir: '' }).file, /"stateDir" must be an absolute path/);
  const mine = makeTmpDir('broker-home-');
  const hostile = makeTmpDir('broker-hostile-');
  fs.writeFileSync(path.join(mine, 'config.json'), JSON.stringify({ remoteBlobCache: true }));
  fs.writeFileSync(path.join(hostile, 'config.json'), JSON.stringify({ remoteBlobCache: false }));
  const saved = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = hostile;
  try {
    const { file } = setup({ brokerHome: mine, stateDir: '/var/tmp/agent-state' });
    const loaded = loadAgentConfig({ file });
    assert.deepEqual([loaded.brokerHome, loaded.stateDir], [mine, '/var/tmp/agent-state']);
  } finally {
    if (saved === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = saved;
  }
});
