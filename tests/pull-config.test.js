import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { ConfigError, DEFAULT_GLOBAL_CONFIG, loadGlobalConfig, pullConfigured, pullRunners, pushRunners } from '../src/config.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { assertCanaryFails } from './helpers/mutant-module.js';

/** BRAIN-1036 R2: the pull-transport config keys and their load-time refusals (T9, T20). */

const QUEUE_URL = 'postgres://lane_submit_train@pitelite:5432/lane_queue?sslmode=verify-full';
const SOCKET_URL = 'postgres://lane_submit_train@%2Fvar%2Frun%2Fpostgresql/lane_queue';
const PULL_KEYS = {
  passwordFile: '/data/train/config/queue.pw',
  caFile: '/data/train/config/ca.pem',
  rustfs: { endpoint: 'https://pitelite:9000', credentialsEnvFile: '/data/train/config/rustfs.env' },
};

function load(extra, loadFrom = loadGlobalConfig) {
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, ...extra });
  const prior = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    return loadFrom();
  } finally {
    process.env.LANE_BROKER_HOME = prior;
  }
}

const refusesNaming = (extra, key) => assert.throws(() => load(extra), (err) => err instanceof ConfigError && err.message.includes(`"${key}"`), JSON.stringify(extra));

test('T9 laneQueueUrl refuses svc: hosts, the pg-primary name, multi-host URLs, target_session_attrs and any other database, naming the key', () => {
  assert.equal(load({ laneQueueUrl: QUEUE_URL }).laneQueueUrl, QUEUE_URL);
  for (const [url, reason] of [
    ['postgres://lane_submit_train@svc:pg-primary/lane_queue', /svc: host/],
    ['postgres://lane_submit_train@pg-primary/lane_queue', /pg-primary name/],
    ['postgres://lane_submit_train@PG-Primary.tailnet.ts.net:5432/lane_queue', /pg-primary name/],
    ['postgres://lane_submit_train@pitelite:5432,wintop:5432/lane_queue', /single-host|exactly one host/],
    ['postgres://lane_submit_train@pitelite,wintop/lane_queue', /single-host|exactly one host/],
    [`${QUEUE_URL}&target_session_attrs=read-write`, /target_session_attrs/],
    ['postgres://u@pitelite:5432/lane_queue?host=pg-primary', /"host" query parameter/],
    ['postgres://u@pitelite/lane_queue?%68ost=svc%3Apg-primary', /"host" query parameter/],
    ['postgres://u@pg%2Dprimary:5432/lane_queue', /pg-primary name/],
    ['postgres://u@svc%3Apg-primary/lane_queue', /svc: host/],
    ['postgres://u@pitelite/lane_queue?host=a,b', /"host" query parameter/],
    ['postgres://u@pitelite/lane_queue?hostaddr=', /"hostaddr" query parameter/],
    ['postgres://u@pitelite/lane_queue?port=', /"port" query parameter/],
    ['postgres://u@pitelite/lane_queue?dbname=postgres', /"dbname" query parameter/],
    ['postgres://u@pitelite/lane_queue?service=x', /"service" query parameter/],
    ['postgres://u@pitelite/lane_queue?sslrootcert=/nonexistent/ca.pem', /"sslrootcert" query parameter/],
    ['postgres://u@pitelite/lane_queue?sslkey=/nonexistent/k.pem', /"sslkey" query parameter/],
    ['postgres://u@pitelite/lane_queue?%E0%A4%A=1', /malformed query string/],
    ['postgres://lane_submit_train@pitelite:5432/postgres', /lane_queue database/],
    ['postgres://lane_submit_train@pitelite:5432/', /lane_queue database/],
    ['postgres://lane_submit_train@pitelite:5432', /lane_queue database/],
    ['https://pitelite/lane_queue', /postgres:\/\/ URL/],
    ['', /postgres:\/\/ URL/],
    [42, /postgres:\/\/ URL/],
  ]) {
    assert.throws(() => load({ laneQueueUrl: url }), (err) => err instanceof ConfigError && err.message.includes('"laneQueueUrl"') && reason.test(err.message), String(url));
  }
});

test('a Unix socket laneQueueUrl is accepted and every other laneQueueUrl rule still applies to it', () => {
  assert.equal(load({ laneQueueUrl: SOCKET_URL }).laneQueueUrl, SOCKET_URL);
  for (const [url, reason] of [
    ['postgres://lane_submit_train@%2Fvar%2Frun%2Fpostgresql/postgres', /lane_queue database/],
    [`${SOCKET_URL}?host=pitelite`, /"host" query parameter/],
    [`${SOCKET_URL}?sslrootcert=/ca.pem`, /"sslrootcert" query parameter/],
    ['postgres://lane_submit_train@%2Fvar%2Frun%2Fpostgresql,pitelite/lane_queue', /single-host|exactly one host/],
  ]) {
    assert.throws(() => load({ laneQueueUrl: url }), (err) => err instanceof ConfigError && err.message.includes('"laneQueueUrl"') && reason.test(err.message), url);
  }
});

test('T9 a host merely containing pg-primary is not the VIP name', () => {
  assert.ok(load({ laneQueueUrl: 'postgres://u@pg-primary-2.example:5432/lane_queue' }));
});

test('T20 the Mac config (a pull runner, no URL, no pull keys) loads: pull is not configured and push placement is unchanged', () => {
  const cfg = load({ runners: [{ name: 'skybox', ssh: 'skybox-runner' }, { name: 'wintop', transport: 'pull' }] });
  assert.equal(cfg.lanePull.enabled, false);
  assert.equal(cfg.laneQueueUrl, undefined);
  assert.equal(pullConfigured(cfg), false);
  assert.deepEqual(pushRunners(cfg).map((r) => r.name), ['skybox']);
  assert.deepEqual(pullRunners(cfg).map((r) => r.name), ['wintop']);
});

test('a config with no pull runner is exactly the old config: every runner is a push runner, lanePull is the off default', () => {
  const runners = [{ name: 'skybox', ssh: 'skybox-runner' }, { name: 'wintop', ssh: 'wintop-runner', transport: 'push' }];
  const cfg = load({ runners });
  assert.deepEqual(pushRunners(cfg), runners);
  assert.deepEqual(pullRunners(cfg), []);
  assert.deepEqual(cfg.lanePull, DEFAULT_GLOBAL_CONFIG.lanePull);
  assert.equal(cfg.lanePull.enabled, false);
  assert.deepEqual(pushRunners({}), []);
  assert.deepEqual(pushRunners(undefined), []);
});

test('runners[].transport: ssh is optional for pull only, and the value is checked', () => {
  assert.ok(load({ runners: [{ name: 'wintop', transport: 'pull' }] }));
  assert.ok(load({ runners: [{ name: 'wintop', transport: 'pull', ssh: 'wintop-runner' }] }));
  assert.throws(() => load({ runners: [{ name: 'wintop' }] }), /ssh must be a non-empty string/);
  assert.throws(() => load({ runners: [{ name: 'wintop', transport: 'push' }] }), /ssh must be a non-empty string/);
  assert.throws(() => load({ runners: [{ name: 'wintop', transport: 'pull', ssh: '-oProxyCommand=x' }] }), /ssh must be a non-empty string/);
  assert.throws(() => load({ runners: [{ name: 'wintop', transport: 'carrier-pigeon', ssh: 'x' }] }), /transport must be "push" or "pull"/);
});

test('lanePull: defaults merge under what the file sets, and bad values are refused by name', () => {
  const cfg = load({ lanePull: { pollMs: 5000, rustfs: { endpoint: 'https://pitelite:9000' } } });
  assert.equal(cfg.lanePull.pollMs, 5000);
  assert.equal(cfg.lanePull.rustfs.region, 'us-east-1');
  assert.equal(cfg.lanePull.rustfs.snapBucket, 'lane-snap');
  assert.equal(cfg.lanePull.rustfs.endpoint, 'https://pitelite:9000');
  refusesNaming({ lanePull: { enabled: 'yes' } }, 'lanePull.enabled');
  refusesNaming({ lanePull: { pollMs: 0 } }, 'lanePull.pollMs');
  refusesNaming({ lanePull: { rustfs: { endpoint: 'pitelite:9000' } } }, 'lanePull.rustfs.endpoint');
  refusesNaming({ lanePull: 'on' }, 'lanePull');
});

async function deadlineRefusals(loadModule) {
  const { loadGlobalConfig: loadFrom } = await loadModule('config.js');
  const { home } = freshEnv();
  const prior = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    for (const key of ['unprovenCancelLostAfterMs', 'queueUnreachableAfterMs']) {
      writeGlobalConfig(home, { version: 1, lanePull: { [key]: 0 } });
      assert.throws(() => loadFrom(), (err) => err.message.includes(`"lanePull.${key}"`), `${key} of 0 is refused by name`);
    }
  } finally {
    process.env.LANE_BROKER_HOME = prior;
  }
}

test('lanePull.unprovenCancelLostAfterMs (10 min) and queueUnreachableAfterMs (15 min) replace the quarantine and URL keys, and the validator is what refuses a bad value', async () => {
  const { lanePull } = DEFAULT_GLOBAL_CONFIG;
  assert.deepEqual([lanePull.unprovenCancelLostAfterMs, lanePull.queueUnreachableAfterMs], [600_000, 900_000]);
  for (const gone of ['quarantineLostAfterMs', 'urlTtlSec', 'urlRefreshSec']) assert.equal(lanePull[gone], undefined, `${gone} is gone`);
  await deadlineRefusals(async (file) => import(`../src/${file}`));
  await assertCanaryFails('config.js', ["'unprovenCancelLostAfterMs', 'queueUnreachableAfterMs', 'prepareBudgetMs', 'blobRefreshAfterDays'", "'blobRefreshAfterDays'"], deadlineRefusals);
});

test('lanePull.enabled needs every connection key, and names the missing ones', () => {
  assert.throws(() => load({ lanePull: { enabled: true } }), (err) => ['laneQueueUrl', 'lanePull.passwordFile', 'lanePull.caFile', 'lanePull.rustfs.endpoint', 'lanePull.rustfs.credentialsEnvFile'].every((k) => err.message.includes(`"${k}"`)));
  assert.throws(() => load({ laneQueueUrl: QUEUE_URL, lanePull: { enabled: true, ...PULL_KEYS, caFile: undefined } }), /"lanePull\.caFile"/);
  assert.ok(load({ laneQueueUrl: QUEUE_URL, lanePull: { enabled: true, ...PULL_KEYS } }));
});

const SOCKET_PULL = { rustfs: PULL_KEYS.rustfs };

function socketPullRules({ loadGlobalConfig: loadFrom, pullConfigured: configured }) {
  const loadWith = (extra) => load(extra, loadFrom);
  const refuses = (extra, key) => assert.throws(() => loadWith(extra), (err) => err instanceof ConfigError && err.message.includes(`"${key}"`), JSON.stringify(extra));
  assert.ok(loadWith({ laneQueueUrl: SOCKET_URL, lanePull: { enabled: true, ...SOCKET_PULL } }));
  for (const key of ['passwordFile', 'caFile']) {
    refuses({ laneQueueUrl: SOCKET_URL, lanePull: { enabled: true, ...SOCKET_PULL, [key]: PULL_KEYS[key] } }, `lanePull.${key}`);
    refuses({ laneQueueUrl: SOCKET_URL, lanePull: { ...SOCKET_PULL, [key]: PULL_KEYS[key] } }, `lanePull.${key}`);
  }
  assert.throws(() => loadWith({ laneQueueUrl: QUEUE_URL, lanePull: { enabled: true, ...SOCKET_PULL } }), (err) => err.message.includes('"lanePull.passwordFile"') && err.message.includes('"lanePull.caFile"'));
  const runners = [{ name: 'wintop', transport: 'pull' }];
  assert.equal(configured(loadWith({ runners, laneQueueUrl: SOCKET_URL, lanePull: { enabled: true, ...SOCKET_PULL } })), true);
  const tcpWithoutFiles = { ...loadWith({ runners, laneQueueUrl: QUEUE_URL, lanePull: { enabled: true, ...PULL_KEYS } }), lanePull: { enabled: true, ...SOCKET_PULL } };
  assert.equal(configured(tcpWithoutFiles), false, 'a TCP URL without the files is not configured');
}

const SOCKET_PULL_TEST = 'a socket laneQueueUrl enables pull without passwordFile or caFile, refuses either, and a TCP URL still needs both';
test(SOCKET_PULL_TEST, async () => {
  socketPullRules({ loadGlobalConfig, pullConfigured });
  const rules = (load) => load('config.js').then(socketPullRules);
  await assertCanaryFails('config.js', ["...(isSocketQueueUrl(cfg.laneQueueUrl) ? [] : [['lanePull.passwordFile'", "...(false ? [] : [['lanePull.passwordFile'"], rules, /"lanePull\.enabled" needs/);
  await assertCanaryFails('config.js', ["if (isSocketQueueUrl(cfg.laneQueueUrl)) {\n    for (const field of ['passwordFile', 'caFile']) assert(", "if (false) {\n    for (const field of ['passwordFile', 'caFile']) assert("], rules);
  await assertCanaryFails('config.js', ["[...(isSocketQueueUrl(cfg.laneQueueUrl) ? [] : [pull.passwordFile, pull.caFile])", "[...(true ? [] : [pull.passwordFile, pull.caFile])"], rules);
  await assertCanaryFails('config.js', ["[...(isSocketQueueUrl(cfg.laneQueueUrl) ? [] : [pull.passwordFile, pull.caFile])", "[...(false ? [] : [pull.passwordFile, pull.caFile])"], rules);
});

test('pullConfigured: enabled, a pull runner and every key, from the config alone (no file is read)', () => {
  const full = { runners: [{ name: 'wintop', transport: 'pull' }], laneQueueUrl: QUEUE_URL, lanePull: { enabled: true, ...PULL_KEYS } };
  const reads = [];
  const real = fs.readFileSync;
  fs.readFileSync = (...args) => (reads.push(args[0]), real(...args));
  try {
    const cfg = load(full);
    reads.length = 0;
    const statCalls = [];
    const realStat = fs.statSync;
    fs.statSync = (...args) => (statCalls.push(args[0]), realStat(...args));
    try {
      assert.equal(pullConfigured(cfg), true, 'the files named do not exist, and that is not asked');
    } finally {
      fs.statSync = realStat;
    }
    assert.deepEqual(reads, []);
    assert.deepEqual(statCalls, []);
    assert.equal(pullConfigured(load({ ...full, runners: [{ name: 'skybox', ssh: 's' }] })), false, 'no pull runner');
    assert.equal(pullConfigured(load({ ...full, lanePull: { ...full.lanePull, enabled: false } })), false, 'not enabled');
    assert.equal(pullConfigured({ ...cfg, laneQueueUrl: undefined }), false, 'no queue URL');
    assert.equal(pullConfigured({ ...cfg, lanePull: { ...cfg.lanePull, rustfs: { ...cfg.lanePull.rustfs, credentialsEnvFile: undefined } } }), false);
    assert.equal(pullConfigured(undefined), false);
  } finally {
    fs.readFileSync = real;
  }
});

const LEDGER_FLOOR = 35 * 24 * 3_600_000;

async function ledgerRetentionRefusal(loadModule) {
  const { loadGlobalConfig } = await loadModule('config.js');
  const attempt = (ledgerRetentionMs) => {
    const { home } = freshEnv();
    writeGlobalConfig(home, { version: 1, lanePull: { ledgerRetentionMs } });
    const prior = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig().lanePull.ledgerRetentionMs;
    } finally {
      process.env.LANE_BROKER_HOME = prior;
    }
  };
  assert.equal(attempt(LEDGER_FLOOR), LEDGER_FLOOR, 'the floor itself is allowed');
  assert.throws(() => attempt(LEDGER_FLOOR - 1), /lanePull\.ledgerRetentionMs.*35 days/, 'below the queue backup retention is refused by name');
}

test('lanePull.ledgerRetentionMs defaults to 45 days and is refused below the 35 day backup retention; the check is what refuses', async () => {
  assert.equal(DEFAULT_GLOBAL_CONFIG.lanePull.ledgerRetentionMs, 45 * 24 * 3_600_000);
  await ledgerRetentionRefusal((file) => import(`../src/${file}`));
  await assertCanaryFails('config.js', ['assert(pull.ledgerRetentionMs >= LEDGER_RETENTION_FLOOR_MS,', 'assert(true || pull.ledgerRetentionMs >= LEDGER_RETENTION_FLOOR_MS,'], ledgerRetentionRefusal);
});
