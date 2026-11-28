import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { laneRun, laneSpawn, waitFor } from './helpers.js';
import { setup, resultOf } from './remote-harness.js';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails } from './helpers/mutant-module.js';
import { paths, atomicWriteJson, ensureStateDirs } from '../src/state.js';

/** BRAIN-940 R1: push adoption relays a kill's reason from the runner's record into the result `lane result --json` prints. */

const MANIFEST = 'm'.repeat(64);
const BINDING = { protocol: 1, generation: 0, manifestHash: MANIFEST, runner: 'skybox' };

function seedAdoptable(ctx, record) {
  const id = crypto.randomUUID();
  ensureStateDirs(ctx.state);
  atomicWriteJson(path.join(paths(ctx.state).attempts, `${id}.json`), {
    id, generation: 0, executor: 'remote', phase: 'running', runner: 'skybox', startedAt: Date.now() - 60_000,
    supervisor: { pid: 2 ** 22 + 7, startTime: null, bootId: 'dead' }, binding: BINDING,
  });
  const dir = path.join(ctx.runnerRoot, 'tickets', id);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'result.json'), JSON.stringify({ protocol: 1, ticketId: id, generation: 0, manifestHash: MANIFEST, kind: 'completed', signal: null, runMs: 10, ...record }));
  return id;
}

const RECORDS = {
  'run-limit': { exit: 124, reason: 'run-limit', runLimitMs: 21_600_000, guard: 'run-limit', stopProven: true },
  deadline_exceeded: { exit: 124, reason: 'deadline_exceeded', deadlineMs: 60_000, deadline: { ms: 60_000, source: 'lane', mode: 'enforce' }, stopProven: false },
};

async function adoptedResult(load, record) {
  const ctx = setup();
  const globalCfg = JSON.parse(fs.readFileSync(path.join(ctx.home, 'config.json'), 'utf8'));
  const id = seedAdoptable(ctx, record);
  const { reconcileRemoteAttempt: reconcile } = await load('remote-reconcile.js');
  const outcome = await reconcile(ctx.state, id, { globalCfg, env: ctx.env, sshBin: 'ssh', deadlineMs: 20_000 });
  assert.deepEqual(outcome, { settled: true, state: 'adopted', exit: 124 });
  const shown = await laneRun(['result', id, '--json'], { env: ctx.env });
  return JSON.parse(shown.stdout);
}

const realLoad = (module) => import(`../src/${module}`);

for (const [name, record] of Object.entries(RECORDS)) {
  test(`push adoption: a ${name} record reaches lane result --json with its reason and window`, async () => {
    const result = await adoptedResult(realLoad, record);
    assert.equal(result.exit, 124);
    assert.equal(result.reason, name);
    for (const field of ['runLimitMs', 'deadlineMs', 'deadline', 'guard', 'stopProven']) {
      if (field in record) assert.deepEqual(result[field], record[field], field);
    }
  });
}

test('push adoption: an ordinary exit 124 carries no reason', async () => {
  const result = await adoptedResult(realLoad, { exit: 124, reason: null });
  assert.equal(result.reason ?? null, null);
});

test('canary: with relayedSettlement removed from push adoption, a run-limit record reads as a bare exit 124', async () => {
  await assertCanaryFails('remote-reconcile.js', ['    ...relayedSettlement(record),\n', ''], (load) => adoptedResult(load, RECORDS['run-limit']).then((r) => assert.equal(r.reason, 'run-limit')));
});


/** A real push dispatch whose runner-side broker record is rewritten, at remote-exec's own hold, to carry `record`. */
async function pushedResult(load, record) {
  const ctx = setup();
  const gate = makeTmpDir('push-hold-');
  const hold = { LANE_BROKER_TEST_HOLD_AT: 'remote-exec-before-result', LANE_BROKER_TEST_HOLD_READY: path.join(gate, 'ready'), LANE_BROKER_TEST_HOLD_GO: path.join(gate, 'go') };
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'node', '-e', 'process.exit(0)'], { env: { ...ctx.env, ...hold }, cwd: ctx.repoDir, bin: load.laneBin });
  const closed = new Promise((resolve) => child.on('close', resolve));
  await waitFor(() => fs.existsSync(hold.LANE_BROKER_TEST_HOLD_READY), { timeoutMs: 60_000 });
  const results = paths(ctx.runnerState).results;
  const file = await waitFor(() => fs.readdirSync(results).find((name) => name.endsWith('.json')) ?? null, { timeoutMs: 30_000 });
  const local = JSON.parse(fs.readFileSync(path.join(results, file), 'utf8'));
  fs.writeFileSync(path.join(results, file), JSON.stringify({ ...local, ...record }));
  fs.writeFileSync(hold.LANE_BROKER_TEST_HOLD_GO, 'go');
  assert.equal(await closed, 124);
  const rows = fs.readFileSync(paths(ctx.state).history, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
  assert.equal(rows.length, 1);
  return { row: rows[0], result: resultOf(ctx.state, rows[0].id) };
}

for (const [name, record] of Object.entries(RECORDS)) {
  test(`push publish: a ${name} kill on the runner reaches the submitter's result and history with its reason`, async () => {
    const { row, result } = await pushedResult({ laneBin: new URL('../bin/lane.js', import.meta.url).pathname }, record);
    for (const shown of [result, row]) {
      assert.equal(shown.exit, 124);
      assert.equal(shown.reason, name);
    }
    for (const field of ['runLimitMs', 'deadlineMs', 'deadline', 'guard', 'stopProven']) {
      if (field in record) assert.deepEqual(result[field], record[field], field);
    }
  });
}

test('canary: with relayedSettlement removed from push publish, a run-limit kill reads as a bare exit 124', async () => {
  await assertCanaryFails('supervisor.js', ['      ...relayedSettlement(dispatch.result),\n', ''], (load) => pushedResult(load, RECORDS['run-limit']).then(({ result }) => assert.equal(result.reason, 'run-limit')));
});
