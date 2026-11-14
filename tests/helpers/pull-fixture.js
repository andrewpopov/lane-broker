import { after as afterAll, afterEach, before, describe, test } from 'node:test';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { createAttempt, readAttempt, updateAttempt } from '../../src/attempts.js';
import { atomicWriteJson, paths, ensureStateDirs, readJsonSafe } from '../../src/state.js';
import { buildExecHeader } from '../../src/remote-client.js';
import { buildManifest } from '../../src/remote-manifest.js';
import { createS3Client } from '../../src/s3-client.js';
import { makeTmpDir } from './tmp.js';
import { sha256Hex } from '../../src/s3-presign.js';
import { freshEnv } from '../helpers.js';
import { makeDispatchArgs, makeGitWorktree } from '../remote-harness.js';
import { pgSkipOption } from '../lane-db-harness.js';
import { startDispatchCluster, tid, World } from '../lane-dispatch-fixture.js';
import { harnessClient } from './queue-fixture.js';
import { assertCanaryFails, realSrc } from './mutant-module.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './s3-stub.js';
import { StubRunner } from './stub-runner.js';

export const ID = tid(1);
/** The row an operator's cancel of a restored ticket used to leave: cancelled, but never proof that the run stopped. */
export const UNPROVEN_CANCEL_ROW = "state = 'cancelled', cancel_requested = true, finished_at = now(), result = jsonb_build_object('kind', 'cancelled', 'cancelConfirmed', false, 'source', 'operator', 'epoch', epoch, 'author', 'queue')";
export const ARTIFACT_PATH = 'reports/run/output.json';
const line = (value) => Buffer.from(`${JSON.stringify(value)}\n`);

export function artifactsStream(ticketId, files) {
  const entries = Object.entries(files).map(([p, text]) => ({ path: p, size: Buffer.byteLength(text), sha256: sha256Hex(text) }));
  return Buffer.concat([line({ protocol: 1, ticketId, files: entries }), ...entries.flatMap((e) => [line(e), Buffer.from(files[e.path])]), line({ end: true })]);
}

// `load` picks the module tree, so a canary runs the same scenario against mutated code
export async function pullFixture(t, w, { load = realSrc, id = ID, stub = null, files = { 'a.txt': 'x\n' }, cfg = {}, runnerLogin, state: sharedState } = {}) {
  const clock = { now: Date.now() };
  const s3Stub = stub ?? await startS3Stub();
  if (!stub) t.after(() => s3Stub.close());
  const s3 = createS3Client({ endpoint: s3Stub.endpoint, region: STUB_REGION, creds: STUB_CREDS });
  const { stepPullTicket, preparePullTicket, preparePullTicketWithRetry, withdrawQueued, proveNeverEnqueued, runPullTicket } = await load('pull-ticket.js');
  const state = sharedState ?? freshEnv().state;
  ensureStateDirs(state);
  const worktreeRoot = makeGitWorktree(files);
  const manifest = buildManifest(worktreeRoot);
  const args = makeDispatchArgs({ ticketId: id, repoKey: 'repoA', lane: 'test' });
  const header = buildExecHeader({ runner: { name: 'pull' }, ...args });
  const attempt = await createAttempt(state, id);
  const binding = { protocol: header.protocol, ticketId: id, generation: 0, manifestHash: manifest.manifestHash };
  const fx = {
    id, state, clock, stub: s3Stub, s3, queue: harnessClient(t, w).queue, runner: new StubRunner(w, { ...(runnerLogin ? { login: runnerLogin } : {}), s3, tmpDir: makeTmpDir('pull-fixture-runner-', t) }), worktreeRoot, manifest, header, binding,
    generation: attempt.generation, reports: [], hooks: {}, logPath: path.join(paths(state).logs, `${id}.log`), resultPath: path.join(paths(state).results, `${id}.json`),
    writes: [],
    envelopeFields: {
      binding, header, repoKey: 'repoA', lane: 'test', laneKey: 'repoA:test', cpuCores: 1, memoryBytes: 1000, requiresTags: [], priority: 0,
      worktreeRoot, declaredArtifacts: ['reports/run/*.json'],
    },
    ctx(over = {}) {
      return {
        root: state, id, generation: attempt.generation, queue: fx.queue, s3, logPath: fx.logPath, resultPath: fx.resultPath, now: () => clock.now, hooks: fx.hooks,
        report: (event) => fx.reports.push(event),
        writeResult: (result, { fsync }) => { fx.writes.push(result); atomicWriteJson(fx.resultPath, result, { fsync }); }, lockTimeoutMs: 2000, objectDeadlineMs: 3000, cfg, sleep: async () => {}, ...over,
      };
    },
    step: (over) => stepPullTicket(fx.ctx(over)),
    prepare: (over) => preparePullTicket(fx.ctx({ prepare: { envelopeFields: fx.envelopeFields, worktreeRoot, manifest }, ...over })),
    prepareRetrying: (over) => preparePullTicketWithRetry(fx.ctx({ prepare: { envelopeFields: fx.envelopeFields, worktreeRoot, manifest }, ...over })),
    run: (over) => runPullTicket(fx.ctx(over)),
    withdraw: (over) => withdrawQueued(fx.ctx(over)),
    proveNeverEnqueued: (over) => proveNeverEnqueued(fx.ctx(over)),
    async enqueue(over) {
      await fx.prepare(over);
      await fx.step(over);
      return fx.step(over);
    },
    async claimed(over) {
      await fx.enqueue(over);
      await fx.runner.claim();
      await fx.step(over);
      await fx.runner.heartbeat();
    },
    async hold(over) {
      await fx.claimed(over);
      await fx.runner.start();
    },
    async su() {
      return w.su();
    },
    async row() {
      return (await (await w.su()).query('SELECT * FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0];
    },
    async setRow(sql, ...params) {
      return (await w.su()).query(`UPDATE lane_dispatch.tickets SET ${sql} WHERE id = $1`, [id, ...params]);
    },
    pull: () => readAttempt(state, id)?.pull,
    async killSupervisor() {
      const { pid } = spawnSync(process.execPath, ['-e', '0']);
      await updateAttempt(state, id, attempt.generation, (current) => ({ supervisor: { ...current.supervisor, pid, startTime: null } }));
    },
    published: () => readJsonSafe(fx.resultPath),
  };
  return fx;
}

/** A describe block over one disposable cluster: every scenario once against the real tree, and each canary against a tree with one guard removed. */
export function definePullSuite(title, scenarios, canaries) {
  describe(title, { skip: pgSkipOption, timeout: 300_000 }, () => {
    let cluster;
    before(async () => { cluster = await startDispatchCluster(); });
    afterAll(async () => { await cluster?.closeClients(); cluster?.stop(); });
    afterEach(() => cluster.closeClients());
    for (const [name, scenario] of Object.entries(scenarios)) test(name, async (t) => scenario(realSrc, await World.create(cluster), t));
    for (const [name, file, mutation, expected, assertionPattern] of canaries) {
      test(`canary: with "${(Array.isArray(mutation[0]) ? mutation[0][0] : mutation[0]).trim().slice(0, 70)}" removed, "${name}" fails`, async (t) => {
        const world = await World.create(cluster);
        const failure = await assertCanaryFails(file, mutation, (load) => scenarios[name](load, world, t), expected, assertionPattern);
        t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0].slice(0, 160)}`);
      });
    }
  });
}
