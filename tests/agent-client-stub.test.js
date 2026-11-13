import test, { beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { makeStubPg, sqlError } from './helpers/stub-pg.js';
import { LdClient, LdClosedError, NOT_LIVE, isRetryable } from '../src/agent/ld-client.js';
import { openAgentCore } from '../src/agent/agent.js';
import { agentPaths, ensureAgentDirs } from '../src/agent/store.js';
import { readJsonSafe } from '../src/state.js';

const ID = '11111111-1111-4111-8111-111111111111';
const TOKEN = '22222222-2222-4222-8222-222222222222';
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

test('a work NOTIFY heard while a LISTEN reconnect is still subscribing cannot reach the server ahead of ld_register', async () => {
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
  assert.deepEqual(stub.names(), ['ld_register', 'ld_claim']);
});

test('a retryable ld_register failure is retried on a fresh connection before any other call is sent', async () => {
  let registers = 0;
  const stub = makeStubPg({ answers: { ld_register: () => { if (registers++ === 0) throw sqlError('57P01'); return { host: 'a1', max_claims: 2, protocol_min: 1 }; } } });
  const { client } = newClient(stub);
  await client.claim('44444444-4444-4444-8444-444444444444', 8, 1e9);
  assert.deepEqual(stub.names(), ['ld_register', 'ld_register', 'ld_claim']);
  assert.deepEqual(stub.sqlLog[0].args, ['t', 1, null], 'the protocol number travels with the version');
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

test('a call that has been sent keeps retrying to a real answer when live() turns false meanwhile', async () => {
  let registers = 0;
  let alive = true;
  let finishes = 0;
  const stub = makeStubPg({ answers: {
    ld_register: () => { if (++registers > 1) alive = false; return { host: 'a1', max_claims: 2, protocol_min: 1 }; },
    ld_finish: () => { if (finishes++ === 0) throw sqlError('57P01'); return { fenced: false, state: 'finished' }; },
  } });
  const { client } = newClient(stub);
  await client.open();
  const answer = await client.finish(ID, 1, TOKEN, { kind: 'ok' }, { live: () => alive });
  assert.deepEqual(answer, { fenced: false, state: 'finished' });
  assert.equal(alive, false, 'live() did turn false during the reconnect');
  assert.equal(stub.names().filter((n) => n === 'ld_finish').length, 2);
});

test('a LISTEN failure after startup is logged and recorded in status.json even with no onWake from the caller', async () => {
  ensureAgentDirs(remoteRoot);
  const stub = makeStubPg();
  const log = [];
  const core = await openAgentCore({ remoteRoot, config: { pg: {}, schedule: null }, version: 't', connect: stub.connect, backoff: BACKOFF, log: (line) => log.push(line) });
  closers.push(() => core.close());
  const old = stub.listenClient();
  stub.failConnects.push(sqlError('57P03'));
  const before = Date.now();
  old.emit('end');
  await untilTrue('listen came back', () => stub.listenClient() !== old && stub.listenClient()?.listens.length === 2);
  assert.deepEqual(log, ['listen-failed: 57P03']);
  const { listenError } = readJsonSafe(agentPaths(remoteRoot).status);
  assert.equal(listenError.error, '57P03');
  assert.ok(listenError.at >= before && listenError.at <= Date.now());
  core.tick();
  assert.equal(readJsonSafe(agentPaths(remoteRoot).status).listenError.error, '57P03', 'a later tick keeps the last LISTEN error');
});
