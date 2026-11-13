import test, { beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { makeStubPg, sqlError } from './helpers/stub-pg.js';
import { LdClient, LdClosedError, NOT_LIVE, isRetryable } from '../src/agent/ld-client.js';
import { openAgentCore } from '../src/agent/agent.js';
import { ExecutionTable, ensureAgentDirs, newExecutionRecord } from '../src/agent/store.js';

const ID = '11111111-1111-4111-8111-111111111111';
const TOKEN = '22222222-2222-4222-8222-222222222222';
const claimView = { id: ID, epoch: 1, token: TOKEN, repo_key: 'repoA', lane: 'test', lane_key: 'lk', binding: {}, header: {}, snapshot: {} };
const BACKOFF = { minMs: 5, maxMs: 20 };

const untilTrue = async (what, check) => {
  for (let i = 0; i < 200 && !check(); i++) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.ok(check(), what);
};

let remoteRoot;
let savedState;
const closers = [];

beforeEach(() => {
  const base = makeTmpDir('agent-stub-');
  remoteRoot = path.join(base, 'remote');
  savedState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_STATE = path.join(base, 'state');
});

afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  if (savedState === undefined) delete process.env.LANE_BROKER_STATE;
  else process.env.LANE_BROKER_STATE = savedState;
});

function newClient(stub, extra = {}) {
  const events = [];
  const client = new LdClient({ pg: {}, version: 't', connect: stub.connect, backoff: BACKOFF, onWake: (e) => events.push(e), ...extra });
  closers.push(() => client.close());
  return { client, events };
}

test('a work NOTIFY heard while a LISTEN reconnect is still subscribing cannot reach the server ahead of ld_reassert', async () => {
  const stub = makeStubPg();
  let client;
  const claims = [];
  ({ client } = newClient(stub, { onWake: (e) => { if (e.reason === 'notify') claims.push(client.claim('44444444-4444-4444-8444-444444444444', 8, 1e9)); } }));
  await client.open();
  stub.sqlLog.length = 0;
  stub.onListen = (c, n) => { if (n === 1) c.emit('notification', { channel: 'ld_work' }); };
  const old = stub.listenClient();
  old.emit('end');
  await untilTrue('the reconnect finished', () => stub.listenClient() !== old && stub.listenClient()?.listens.length === 2 && claims.length === 1);
  await Promise.all(claims);
  assert.deepEqual(stub.names(), ['ld_reassert', 'ld_register', 'ld_claim']);
});

test('a retryable ld_register failure keeps the reassert verdicts: the record is KILLING and the second reassert no longer carries it', async () => {
  ensureAgentDirs(remoteRoot);
  new ExecutionTable(remoteRoot).put({ ...newExecutionRecord(claimView), subphase: 'RUNNING' });
  let registers = 0;
  const stub = makeStubPg({ answers: {
    ld_reassert: ([items]) => JSON.parse(items).map((item) => ({ id: item.id, result: 'fenced' })),
    ld_register: () => { if (registers++ === 0) throw sqlError('57P01'); return { host: 'a1', max_claims: 2 }; },
  } });
  const core = await openAgentCore({ remoteRoot, config: { pg: {}, schedule: null }, version: 't', connect: stub.connect, backoff: BACKOFF, log: () => {} });
  closers.push(() => core.close());
  assert.deepEqual(stub.sqlLog.filter((c) => c.name === 'ld_reassert').map((c) => JSON.parse(c.args[0]).length), [1, 0]);
  assert.equal(new ExecutionTable(remoteRoot).get(`${ID}.e1`).subphase, 'KILLING');
});

test('close() while a connect is pending ends that connection when it arrives and sends no SQL', async () => {
  const stub = makeStubPg();
  let arrive;
  const late = new Promise((resolve) => { arrive = resolve; });
  let connecting = 0;
  const connect = async (config) => { connecting += 1; await late; return stub.connect(config); };
  const { client } = newClient(stub, { connect });
  const pending = client.claim('44444444-4444-4444-8444-444444444444', 8, 1e9);
  const settled = assert.rejects(pending, LdClosedError);
  await untilTrue('the connect started', () => connecting === 1);
  await client.close();
  arrive();
  await settled;
  assert.equal(stub.sqlLog.length, 0, 'nothing was sent after close');
  assert.equal(stub.clients.length, 1);
  assert.equal(stub.clients[0].ended, true, 'the late connection was ended, not leaked');
});

test('the LISTEN loop keeps backing off through 57P03 and non-retryable errors, reports each, and recovers', async () => {
  const stub = makeStubPg();
  const { client, events } = newClient(stub);
  await client.open();
  stub.failConnects.push(sqlError('57P03'), sqlError('42501'), sqlError('57P02'));
  stub.listenClient().emit('end');
  await untilTrue('listen came back', () => events.some((e) => e.reason === 'listen-reconnected'));
  assert.deepEqual(events.filter((e) => e.reason === 'listen-failed').map((e) => e.error.code), ['57P03', '42501', '57P02']);
  assert.equal(stub.listenClient().listens.length, 2);
  for (const code of ['57P02', '57P03']) assert.equal(isRetryable(sqlError(code)), true, code);
  assert.equal(isRetryable(sqlError('42501')), false);
});

test('a call whose live() turns false is resolved without being sent, and persist is skipped', async () => {
  const stub = makeStubPg();
  const { client } = newClient(stub);
  await client.open();
  let persisted = 0;
  const answer = await client.heartbeat(ID, 1, TOKEN, { live: () => false, persist: () => { persisted += 1; } });
  assert.equal(answer, NOT_LIVE);
  assert.deepEqual(answer, { skipped: 'not-live' });
  assert.equal(persisted, 0);
  assert.ok(!stub.names().includes('ld_heartbeat'));
  await client.heartbeat(ID, 1, TOKEN, { live: () => true });
  assert.equal(stub.names().at(-1), 'ld_heartbeat');
});

test('a finish queued before a reassert that ends its record is not sent after it', async () => {
  ensureAgentDirs(remoteRoot);
  new ExecutionTable(remoteRoot).put({ ...newExecutionRecord(claimView), subphase: 'RUNNING' });
  let fence = false;
  const stub = makeStubPg({ answers: { ld_reassert: ([items]) => JSON.parse(items).map((item) => ({ id: item.id, result: fence ? 'fenced' : 'ok', ...(fence ? {} : { state: 'running' }) })) } });
  const core = await openAgentCore({ remoteRoot, config: { pg: {}, schedule: null }, version: 't', connect: stub.connect, backoff: BACKOFF, log: () => {} });
  closers.push(() => core.close());
  fence = true;
  stub.clients[0].emit('error', new Error('Connection terminated'));
  const live = () => core.table.get(`${ID}.e1`)?.subphase === 'RUNNING';
  const first = core.ld.finish(ID, 1, TOKEN, { kind: 'ok' }, { live });
  const second = core.ld.heartbeat(ID, 1, TOKEN, { live });
  assert.deepEqual([await first, await second], [NOT_LIVE, NOT_LIVE]);
  assert.equal(core.table.get(`${ID}.e1`).subphase, 'KILLING');
  assert.ok(!stub.names().includes('ld_finish') && !stub.names().includes('ld_heartbeat'));
});
