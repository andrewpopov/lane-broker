import assert from 'node:assert/strict';
import fs from 'node:fs';
import { readAttempt } from '../src/attempts.js';
import { paths } from '../src/state.js';
import { recordSettledResult } from '../src/supervisor.js';
import { definePullSuite, pullFixture } from './helpers/pull-fixture.js';

const mutateWorktree = (fx) => fs.writeFileSync(`${fx.worktreeRoot}/a.txt`, 'changed while the snapshot was being uploaded\n');

/** BRAIN-1036 R7 on a disposable Postgres: the queued timeout, a result rebuilt from the ledger, and an attempt another publisher settled. */

const DAY = 24 * 3_600_000;
const NEVER = new Error('the ticket was never withdrawn');

const stepClock = (fx, advance, maxSteps = 40) => {
  let steps = 0;
  return async () => {
    fx.clock.now += advance;
    if (++steps > maxSteps) throw NEVER;
  };
};

const runUntilWithdrawn = async (fx, over, { prepare = true } = {}) => {
  const first = prepare ? { prepare: { envelopeFields: fx.envelopeFields, worktreeRoot: fx.worktreeRoot, manifest: fx.manifest } } : {};
  return fx.run({ ...first, ...over }).catch((err) => (err === NEVER ? { outcome: 'still queued' } : Promise.reject(err)));
};

const settledLost = async (fx) => {
  const step = await fx.step();
  assert.deepEqual([step.state, step.decision?.kind, step.decision?.exit], ['settled', 'lost', fx.published()?.exit]);
  return step;
};

const ticketOf = (fx) => ({ id: fx.id, key: 'repoA:test', repoId: 'repoA', lane: 'test', weight: 1, resources: {}, command: 'true', headTree: 'abc', resultPath: fx.resultPath });
const historyRows = (fx) => fs.readFileSync(paths(fx.state).history, 'utf8').split('\n').filter((line) => line.includes(`"id":"${fx.id}"`));
const withHistory = (fx) => ({ writeResult: (result, opts) => recordSettledResult(fx.state, ticketOf(fx), result, opts) });

const SCENARIOS = {
  'a queued pull ticket nobody claims is withdrawn once its timeout passes: the queue holds withdrawn and the attempt holds the proof': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    const { assertWithdrawCommitted } = await load('pull-ticket.js');
    fx.envelopeFields.queuedTimeoutMs = 60_000;
    const outcome = await runUntilWithdrawn(fx, { sleep: stepClock(fx, 15_000, 20) });
    assert.deepEqual([outcome.outcome, outcome.proof, /^queued-timeout: .*60000ms/.test(outcome.reason)], ['withdrawn', true, true]);
    assert.equal((await fx.row()).state, 'withdrawn');
    assert.equal(fx.pull().withdrawProof, true);
    assert.doesNotThrow(() => assertWithdrawCommitted(fx.state, fx.id), 'the fallback is allowed once the withdraw committed');
  },

  'with no queued timeout a ticket waits however long it is queued (BRAIN-509: train and pr lanes have none)': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    const outcome = await runUntilWithdrawn(fx, { sleep: stepClock(fx, DAY, 30) });
    assert.equal(outcome.outcome, 'still queued');
    assert.equal((await fx.row()).state, 'queued');
    assert.equal(fx.pull().withdrawProof, undefined);
  },

  'a claimed ticket is never offered to the withdraw by the timeout, however old it is': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.envelopeFields.queuedTimeoutMs = 1000;
    await fx.hold();
    const withdraws = [];
    const queue = { ...fx.queue, withdraw: async (id) => { withdraws.push(id); return fx.queue.withdraw(id); } };
    const outcome = await runUntilWithdrawn(fx, { queue, sleep: stepClock(fx, DAY, 5) }, { prepare: false });
    assert.equal(outcome.outcome, 'still queued');
    assert.deepEqual(withdraws, []);
  },

  'the queued timeout lives in the envelope: after the supervisor died, whoever steps the ticket withdraws it and settles it ledgered as never started': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.envelopeFields.queuedTimeoutMs = 60_000;
    await fx.enqueue();
    await fx.killSupervisor();
    fx.clock.now += 59_000;
    assert.equal((await fx.step()).state, 'queued');
    fx.clock.now += 2_000;
    const step = await settledLost(fx);
    assert.equal(step.decision.reason, 'withdrawn-never-started');
    assert.equal((await fx.row()).state, 'withdrawn');
    assert.equal(readAttempt(fx.state, fx.id), null);
  },

  'a withdraw the queue committed whose supervisor died before the fallback settles ledgered as never started, 75 or 76 where dispatch-only; a live supervisor is left to its fallback': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.envelopeFields.publication = { refusalExit: 76 };
    await fx.enqueue();
    assert.equal((await fx.withdraw()).proof, true);
    assert.equal((await fx.step()).state, 'withdrawn', 'the live supervisor takes its own committed withdraw to the fallback');
    await fx.killSupervisor();
    const step = await settledLost(fx);
    assert.deepEqual([step.decision.reason, fx.published().exit, fx.published().reason], ['withdrawn-never-started', 76, 'withdrawn-never-started']);
    assert.ok(fs.existsSync(`${paths(fx.state).pullLedger}/${fx.id}.json`), 'the decision is ledgered');
    assert.equal(readAttempt(fx.state, fx.id), null);

    const plain = await pullFixture(t, w, { load, id: '00000000-0000-4000-8000-0000000000e2' });
    await plain.enqueue();
    await plain.withdraw();
    await plain.killSupervisor();
    await settledLost(plain);
    assert.equal(plain.published().exit, 75);
  },

  'a withdrawn row whose supervisor is alive is left to it; with the supervisor dead the row itself is the proof, persisted and settled never started': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.queue.withdraw(fx.id);
    assert.equal((await fx.step()).state, 'withdrawn');
    assert.equal(fx.pull().withdrawProof, undefined);
    await fx.killSupervisor();
    const step = await settledLost(fx);
    assert.equal(step.decision.reason, 'withdrawn-never-started');
    assert.ok(fs.existsSync(`${paths(fx.state).pullLedger}/${fx.id}.json`));
  },

  'an attempt already falling back to local is never touched by recovery, though its supervisor died and its snapshot or proof is recorded': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    const { reconcilePullAttempt } = await load('pull-recover.js');
    const { updateAttempt } = await import('../src/attempts.js');
    mutateWorktree(fx);
    await assert.rejects(fx.prepare(), /file changed/);
    assert.equal((await fx.proveNeverEnqueued()).proof, true);
    await updateAttempt(fx.state, fx.id, fx.generation, { executor: 'local' });
    await fx.killSupervisor();
    const before = JSON.stringify(readAttempt(fx.state, fx.id));
    assert.equal((await fx.step()).state, 'local');
    const recovered = await reconcilePullAttempt(readAttempt(fx.state, fx.id), (ticket) => fx.ctx({ generation: ticket.generation }));
    assert.deepEqual(recovered, { settled: false, state: 'skipped', reason: 'pull ticket is local' }, 'pull recovery must not pick a local attempt');
    assert.equal(JSON.stringify(readAttempt(fx.state, fx.id)), before);
    assert.deepEqual([fx.published(), await fx.row()], [null, undefined]);
    assert.ok(!fs.existsSync(`${paths(fx.state).pullLedger}/${fx.id}.json`));

    const enqueued = await pullFixture(t, w, { load, id: '00000000-0000-4000-8000-0000000000e3' });
    await enqueued.enqueue();
    await updateAttempt(enqueued.state, enqueued.id, enqueued.generation, { executor: 'local', pull: { stage: 'ENQUEUING' } });
    await enqueued.killSupervisor();
    assert.equal((await enqueued.step()).state, 'local');
  },

  'a snapshot that cannot be prepared before anything was enqueued is proven never started, allows the fallback, and a dead supervisor settles it ledgered': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    const { assertWithdrawCommitted } = await load('pull-ticket.js');
    mutateWorktree(fx);
    await assert.rejects(fx.prepare(), /file changed/);
    assert.throws(() => assertWithdrawCommitted(fx.state, fx.id), /before the queue has committed a withdraw/);
    assert.equal((await fx.proveNeverEnqueued()).proof, true);
    assert.doesNotThrow(() => assertWithdrawCommitted(fx.state, fx.id));
    await fx.killSupervisor();
    const step = await settledLost(fx);
    assert.equal(step.decision.reason, 'never-enqueued');
  },

  'the never-enqueued proof is refused once any enqueue attempt was recorded': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    assert.equal((await fx.proveNeverEnqueued()).proof, false);
    assert.equal(fx.pull().neverEnqueued, undefined);
  },

  'an S3 server error while uploading the snapshot is waited out and retried, resuming the same envelope': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    let uploads = 0;
    fx.stub.failMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') && ++uploads <= 2 ? 503 : 0);
    const sleeps = [];
    await assert.doesNotReject(fx.run({ prepare: { envelopeFields: fx.envelopeFields, worktreeRoot: fx.worktreeRoot, manifest: fx.manifest }, sleep: async (ms) => { sleeps.push(ms); if (sleeps.length > 3) throw NEVER; } }).catch((err) => (err === NEVER ? null : Promise.reject(err))));
    assert.deepEqual([uploads, sleeps.slice(0, 2).length], [3, 2], 'the two failed uploads were each waited out and the third landed');
    assert.equal((await fx.row())?.state, 'queued', 'the retried preparation reached the queue');
  },

  'a result rebuilt from the ledger replaces the lost file without a second history row': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    const { reconstructLedgered } = await load('pull-recover.js');
    await fx.hold();
    await fx.runner.finish(await fx.runner.putOutputs({ log: 'out\n' }));
    const writer = withHistory(fx);
    const settled = await fx.step(writer);
    assert.equal(settled.state, 'settled');
    assert.equal(historyRows(fx).length, 1);
    fs.rmSync(fx.resultPath);
    assert.equal(readAttempt(fx.state, fx.id), null);
    const rebuilt = await reconstructLedgered(fx.state, fx.id, fx.resultPath, () => fx.ctx(writer));
    assert.equal(rebuilt, true);
    assert.equal(fx.published().exit, 0, 'the result file is back');
    assert.equal(historyRows(fx).length, 1, 'the rebuild counted the ticket a second time');
  },

  'an attempt another publisher settled first ends the supervisor with that result\'s exit, not a crash': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish({ exit: 4, ...(await fx.runner.putOutputs({ log: 'out\n' })) });
    assert.equal((await fx.step()).state, 'settled');
    fs.rmSync(`${paths(fx.state).pullLedger}/${fx.id}.json`);
    const outcome = await fx.run();
    assert.deepEqual([outcome.outcome, outcome.exitCode, outcome.already], ['confirmed', 4, true]);
  },
};

const unbounded = 'with no queued timeout a ticket waits however long it is queued (BRAIN-509: train and pr lanes have none)';

const TIMEOUT_LIVES_IN_ENVELOPE = 'the queued timeout lives in the envelope: after the supervisor died, whoever steps the ticket withdraws it and settles it ledgered as never started';
const COMMITTED_WITHDRAW = 'a withdraw the queue committed whose supervisor died before the fallback settles ledgered as never started, 75 or 76 where dispatch-only; a live supervisor is left to its fallback';
const NOT_PREPARED = 'a snapshot that cannot be prepared before anything was enqueued is proven never started, allows the fallback, and a dead supervisor settles it ledgered';

const CANARIES = [
  [TIMEOUT_LIVES_IN_ENVELOPE, 'pull-ticket.js', ['const limit = run.envelope.queuedTimeoutMs;', 'const limit = run.queuedTimeoutMs;']],
  [COMMITTED_WITHDRAW, 'pull-ticket.js', ['provenNeverStarted(attempt.pull) && !supervisorAlive(attempt)) return settle', 'provenNeverStarted(attempt.pull)) return settle']],
  ['a withdrawn row whose supervisor is alive is left to it; with the supervisor dead the row itself is the proof, persisted and settled never started', 'pull-ticket.js', ['supervisorAlive(attempt) ? { state: \'withdrawn\' } : settleCommittedWithdraw(run)', '{ state: \'withdrawn\' }']],
  ['a withdrawn row whose supervisor is alive is left to it; with the supervisor dead the row itself is the proof, persisted and settled never started', 'pull-ticket.js', ['supervisorAlive(attempt) ? { state: \'withdrawn\' } : settleCommittedWithdraw(run)', 'settleCommittedWithdraw(run)']],
  ['an attempt already falling back to local is never touched by recovery, though its supervisor died and its snapshot or proof is recorded', 'pull-ticket.js', ["  if (attempt.executor === 'local') return { state: 'local' };\n", '']],
  [NOT_PREPARED, 'pull-ticket.js', ["pull.withdrawProof === true ? 'withdrawn-never-started' : 'never-enqueued'", "'withdrawn-never-started'"]],
  ['the never-enqueued proof is refused once any enqueue attempt was recorded', 'pull-ticket.js', ['if ((readAttempt(run.root, run.id)?.pull?.enqueue?.attempts ?? []).length > 0) return { proof: false };', '']],
  ['an S3 server error while uploading the snapshot is waited out and retried, resuming the same envelope', 'pull-ticket.js', ['      if (!isTransientError(err)) throw err;\n      if (ctx.cancelRequested?.()) throw new PrepareAbandoned(\'cancelled\', err);', '      throw err;']],
  ['an S3 server error while uploading the snapshot is waited out and retried, resuming the same envelope', 'pull-ticket.js', ['const envelope = existing ?? await createEnvelope(', 'const envelope = await createEnvelope(']],
  ['a queued pull ticket nobody claims is withdrawn once its timeout passes: the queue holds withdrawn and the attempt holds the proof', 'pull-ticket.js', ['return queuedTooLong(run, attempt) ? expireQueued(run, attempt) :', 'return false ? expireQueued(run, attempt) :'], /never withdrawn/],
  ['a queued pull ticket nobody claims is withdrawn once its timeout passes: the queue holds withdrawn and the attempt holds the proof', 'pull-ticket.js', ["  await persistPull(run, { withdrawProof: true });\n  return { proof: true };", '  return { proof: true };']],
  [unbounded, 'pull-ticket.js', ['return limit !== undefined && seenRowAt', 'return limit === undefined || seenRowAt']],
  ['a claimed ticket is never offered to the withdraw by the timeout, however old it is', 'pull-ticket.js', ["  switch (row.state) {\n    case 'queued':", "  if (queuedTooLong(run, attempt)) return expireQueued(run, attempt);\n  switch (row.state) {\n    case 'queued':"]],
  ['a result rebuilt from the ledger replaces the lost file without a second history row', 'pull-ticket.js', ['resultWriterOf(run)(result, { fsync: true, rebuilt: true });', 'resultWriterOf(run)(result, { fsync: true });']],
  ['an attempt another publisher settled first ends the supervisor with that result\'s exit, not a crash', 'pull-ticket.js', ["    if (step.state === 'settled' && step.decision === undefined) return settledElsewhere(ctx);\n", ''], /reading 'kind'/],
];

definePullSuite('pull dispatch: queued timeout, rebuilt results and foreign publishers', SCENARIOS, CANARIES);
