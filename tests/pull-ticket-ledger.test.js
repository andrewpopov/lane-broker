import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { readAttempt, updateAttempt } from '../src/attempts.js';
import { pullTicketDir } from '../src/pull-envelope.js';
import { ledgerDir, readLedger } from '../src/pull-ledger.js';
import { sha256Hex } from '../src/s3-presign.js';
import { paths, writeCancelMarkerFile } from '../src/state.js';
import { definePullSuite, pullFixture, artifactsStream, ARTIFACT_PATH } from './helpers/pull-fixture.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';
import { logins, call, tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R5: the ledger-first rule and the durability order of the effects it guards (T7, T25, T26, T33, T34, T37). */

const CRASH = new Error('killed after the ledger');
const crashAfterLedger = async (fx) => {
  fx.hooks.afterLedger = () => { throw CRASH; };
  await assert.rejects(fx.step(), CRASH);
  delete fx.hooks.afterLedger;
};
const next = (t, w, load, n, first) => pullFixture(t, w, { load, id: tid(n), stub: first?.stub });
const installed = (fx) => path.join(fs.realpathSync(fx.worktreeRoot), ARTIFACT_PATH);
const gets = (fx) => fx.stub.requests.filter((r) => r.method === 'GET').length;
const unreachable = new Proxy({}, { get: () => async () => { throw new Error('queue unreachable'); } });
const finishWithArtifacts = async (fx, content, { log = 'out\n' } = {}) => {
  const artifacts = artifactsStream(fx.id, { [ARTIFACT_PATH]: content });
  await fx.runner.finish(await fx.runner.putOutputs({ log, artifacts }));
  return artifacts;
};

async function recorded(fn, { unlinks = [] } = {}) {
  const recorder = recordFsyncs();
  const unlinkSync = fs.unlinkSync;
  fs.unlinkSync = (target, ...rest) => { recorder.events.push({ op: 'unlink', path: String(target) }); return unlinkSync(target, ...rest); };
  try {
    await fn();
  } finally {
    fs.unlinkSync = unlinkSync;
    recorder.stop();
  }
  return recorder;
}

const SCENARIOS = {
  'T7: after a crash behind the ledger, a different result on the row is reported and refused: the accepted one is published, nothing of the other is read': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"a":1}');
    await crashAfterLedger(fx);
    const readsBefore = gets(fx);
    await fx.setRow(`result = result || '{"exit": 3, "logSha256": "${'b'.repeat(64)}", "logBytes": 4}'::jsonb`);
    const settled = await fx.step();
    assert.deepEqual([settled.state, settled.decision.kind, settled.result.exit], ['settled', 'finished', 0]);
    assert.equal(fs.readFileSync(installed(fx), 'utf8'), '{"a":1}');
    assert.deepEqual(fx.reports.map((r) => r.kind), ['ledger-divergence']);
    assert.equal(gets(fx), readsBefore, 'no object was read for the other result');
  },

  'T25: a ledger entry decides alone: an invalid result, a valid one with other artifacts, a restored non-terminal row and an unreachable queue change nothing': async (load, w, t) => {
    const invalid = await pullFixture(t, w, { load });
    await invalid.hold();
    await invalid.runner.finish();
    await crashAfterLedger(invalid);
    await invalid.setRow(`result = '{"kind":"completed","exit":0}'::jsonb`);
    const first = await invalid.step();
    assert.deepEqual([first.decision.kind, first.result.exit], ['finished', 0]);
    assert.deepEqual(invalid.reports.map((r) => r.kind), ['ledger-divergence'], 'the other result was reported, never built as a candidate');

    const swapped = await next(t, w, load, 2, invalid);
    await swapped.hold();
    const original = await finishWithArtifacts(swapped, '{"original":1}');
    await crashAfterLedger(swapped);
    const other = artifactsStream(swapped.id, { [ARTIFACT_PATH]: '{"other":1}' });
    await swapped.runner.put('artifacts', other);
    await swapped.setRow(`result = result || '{"artifactsSha256": "${sha256Hex(other)}", "artifactsBytes": ${other.length}}'::jsonb`);
    const readsBefore = gets(swapped);
    assert.equal((await swapped.step()).decision.kind, 'finished');
    assert.equal(fs.readFileSync(installed(swapped), 'utf8'), '{"original":1}', 'the accepted artifacts were installed');
    assert.equal(gets(swapped), readsBefore, 'the other artifacts were never fetched, let alone decoded');
    assert.ok(original.length > 0);

    const restored = await next(t, w, load, 3, invalid);
    await restored.hold();
    await restored.runner.finish();
    await crashAfterLedger(restored);
    await restored.setRow("state = 'queued', result = NULL");
    assert.equal((await restored.step({ queue: unreachable })).decision.kind, 'finished', 'published with the queue unreachable');
  },

  'T25: three steppers on one dead supervisor create one ledger entry, install once and publish once': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"once":1}');
    fx.hooks.beforeInstall = () => new Promise((resolve) => setTimeout(resolve, 200));
    const recorder = await recorded(async () => {
      await Promise.all([1, 2, 3].map(() => fx.step({ lockTimeoutMs: 10_000 })));
    });
    assert.equal(fx.writes.length, 1, 'one publishTerminal');
    assert.equal(recorder.fsyncedPaths().filter((p) => p === installed(fx)).length, 1, 'one artifacts install');
    assert.equal(readLedger(fx.state, fx.id).decision.kind, 'finished');
    assert.equal(fs.readdirSync(ledgerDir(fx.state)).filter((f) => f.endsWith('.json')).length, 1);
  },

  'T26: the published exit equals the ledgered decision whether or not a local cancel marker is present': async (load, w, t) => {
    const finished = await pullFixture(t, w, { load });
    await finished.hold();
    await finished.runner.finish();
    writeCancelMarkerFile(finished.state, finished.id);
    const done = await finished.step();
    assert.deepEqual([done.decision.kind, finished.published().exit, finished.published().cancelRaced], ['finished', 0, true]);
    assert.equal(readLedger(finished.state, finished.id).decision.exit, finished.published().exit);

    const lost = await next(t, w, load, 2, finished);
    await lost.hold();
    await lost.setRow("state = 'lost'");
    writeCancelMarkerFile(lost.state, lost.id);
    await lost.step();
    assert.deepEqual([readLedger(lost.state, lost.id).decision.kind, lost.published().exit], ['lost', 75]);

    const unproven = await next(t, w, load, 3, finished);
    await unproven.hold();
    await unproven.setRow("state = 'quarantined'");
    await unproven.queue.cancel(unproven.id);
    writeCancelMarkerFile(unproven.state, unproven.id);
    const kept = await unproven.step();
    assert.deepEqual([kept.state, kept.exitCode, unproven.published()], ['unresolved', 1, null]);
    assert.equal(readLedger(unproven.state, unproven.id), null);
    assert.equal(readAttempt(unproven.state, unproven.id).unresolved, 'cancel-unconfirmed');
  },

  'T37b: with the attempt and the result both gone, the ledger entry rebuilds the result durably, once': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"rebuilt":1}');
    await crashAfterLedger(fx);
    fs.rmSync(path.join(paths(fx.state).attempts, `${fx.id}.json`));
    assert.equal(fx.published(), null);
    const recorder = await recorded(async () => {
      const out = await fx.step();
      assert.deepEqual([out.state, out.reconstructed, out.decision.kind], ['settled', true, 'finished']);
    });
    assert.equal(fx.published().exit, 0, 'a waiter reads the ledgered exit, never 1');
    assert.equal(fx.writes.length, 1);
    const resultRename = recorder.events.findIndex((e) => e.op === 'rename' && e.path === fx.resultPath);
    assert.ok(recorder.events.some((e, i) => i > resultRename && e.op === 'fsync' && e.path === paths(fx.state).results), 'the rebuilt result is durable');
    assert.equal((await fx.step()).already, true);
    assert.equal(fx.writes.length, 1, 'rebuilt once');
  },

  'a stale generation applies no effect: nothing is appended, installed or published': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"stale":1}');
    await crashAfterLedger(fx);
    await updateAttempt(fx.state, fx.id, 0, { generation: 1 });
    await fx.step().catch(() => {});
    assert.equal(fs.existsSync(installed(fx)), false, 'artifacts were installed for a superseded generation');
    assert.equal(fx.published(), null);
    assert.equal((await fx.step()).state, 'stale');
  },

  'an entry ledgered for one generation applies nothing to an attempt bumped to the next, even when the caller follows the bump': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"gen0":1}');
    await crashAfterLedger(fx);
    await updateAttempt(fx.state, fx.id, 0, { generation: 1, executor: 'local' });
    const out = await fx.step({ generation: 1 }).catch((err) => err);
    assert.equal(out.state, 'stale');
    assert.equal(fs.existsSync(installed(fx)), false);
    assert.equal(fx.published(), null);
    assert.equal(readAttempt(fx.state, fx.id).generation, 1, 'the bumped attempt is untouched');
  },

  'a stale-generation caller creates no ledger entry, so the correct caller still settles the ticket': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.hooks.afterMarker = () => { throw CRASH; };
    await assert.rejects(fx.prepare(), CRASH);
    await fx.killSupervisor();
    await updateAttempt(fx.state, fx.id, 0, { generation: 1 });
    const stale = await fx.step({ generation: 0 });
    assert.equal(stale.state, 'stale');
    assert.equal(readLedger(fx.state, fx.id), null, 'a stale caller must not stamp an entry');
    assert.equal((await fx.step({ generation: 1 })).decision.reason, 'prepare-interrupted');
    assert.equal(readLedger(fx.state, fx.id).generation, 1);
  },

  'a queue outage never delays a local publish: the result is written before the divergence check, which has its own short deadline': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish();
    await crashAfterLedger(fx);
    const hanging = { ...fx.queue, get: () => new Promise(() => {}) };
    const step = fx.step({ queue: hanging });
    await new Promise((resolve) => setTimeout(resolve, 600));
    assert.equal(fx.published().exit, 0, 'published while the queue read is still pending');
    assert.equal((await Promise.race([step, new Promise((_, reject) => setTimeout(() => reject(new Error('still running after 6000ms')), 6000))])).state, 'settled');
  },

  'T33: after a creator died behind the link, recovery makes the ledger directory durable before any effect, parents of the existing ticket directory first': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"ok":1}');
    fx.hooks.ledger = { afterLink: () => { throw CRASH; } };
    await assert.rejects(fx.step(), CRASH);
    delete fx.hooks.ledger;
    assert.ok(fs.existsSync(path.join(ledgerDir(fx.state), `${fx.id}.json`)));
    const recorder = await recorded(() => fx.step());
    const at = (op, target) => recorder.events.findIndex((e) => e.op === op && e.path === target);
    const ledgerSync = at('fsync', ledgerDir(fx.state));
    assert.ok(ledgerSync >= 0, 'the ledger directory was never fsynced');
    assert.ok(at('fsync', paths(fx.state).pull) >= 0 && at('fsync', paths(fx.state).pull) < ledgerSync, 'the parent of the existing ticket directory was fsynced first');
    assert.ok(ledgerSync < at('rename', installed(fx)), 'an effect ran before the ledger directory was durable');
    assert.ok(ledgerSync < at('rename', fx.resultPath));
  },

  'T34: every queue-authored cancel survives a restart through the real classifier and publisher': async (load, w, t) => {
    const queued = await pullFixture(t, w, { load });
    await queued.enqueue();
    await queued.queue.cancel(queued.id);
    await crashAfterLedger(queued);
    assert.equal((await queued.step()).decision.kind, 'cancelled');
    const entry = readLedger(queued.state, queued.id);
    assert.deepEqual([entry.attemptRef, entry.resultSha256, entry.decision.source], [null, null, 'queued']);
    assert.deepEqual([queued.published().exit, queued.published().remoteCancelConfirmed], [130, true]);

    const claimedReleased = await next(t, w, load, 2, queued);
    await claimedReleased.enqueue();
    await claimedReleased.runner.claim();
    await claimedReleased.queue.cancel(claimedReleased.id);
    await claimedReleased.runner.release('never-started');
    await crashAfterLedger(claimedReleased);
    assert.equal((await claimedReleased.step()).decision.kind, 'cancelled');
    assert.equal(readLedger(claimedReleased.state, claimedReleased.id).decision.proof, 'released');
    assert.equal(claimedReleased.published().exit, 130);

    const runningReleased = await next(t, w, load, 3, queued);
    await runningReleased.hold();
    await runningReleased.queue.cancel(runningReleased.id);
    await runningReleased.runner.release('cancel-confirmed');
    await crashAfterLedger(runningReleased);
    assert.equal((await runningReleased.step()).decision.kind, 'cancelled');
    assert.equal(runningReleased.published().exit, 130);

    const quarantined = await next(t, w, load, 4, queued);
    await quarantined.hold();
    await quarantined.setRow("state = 'quarantined'");
    await quarantined.queue.cancel(quarantined.id);
    assert.equal((await quarantined.step()).state, 'unresolved');
    quarantined.clock.now += 31 * 60_000;
    await crashAfterLedger(quarantined);
    assert.equal((await quarantined.step()).decision.reason, 'cancel-unproven');
    assert.equal(quarantined.published().exit, 75);

    const operator = await next(t, w, load, 5, queued);
    await operator.hold();
    await operator.setRow("state = 'quarantined'");
    assert.equal(await call(await w.as(logins.admin), 'ld_cancel_quarantined', operator.id), 'cancelled');
    assert.equal((await operator.step()).state, 'unresolved', 'an operator cancel is not proof that the run stopped');
  },

  'a runner-forged server cancel is never queue-authored: only author queue on a cancelled row is trusted': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish({ kind: 'cancelled', cancelConfirmed: true, source: 'queued', author: 'queue', exit: undefined, protocol: undefined, ticketId: undefined });
    const row = await fx.row();
    assert.equal(Object.hasOwn(row.result, 'author'), false, 'the queue strips the key before it can be stored');
    const settled = await fx.step();
    assert.deepEqual([settled.decision.kind, settled.decision.reason, settled.result.exit], ['lost', 'result-unbound', 75], 'a forged 130 is refused');

    const stored = await next(t, w, load, 2, fx);
    await stored.hold();
    await stored.setRow(`state = 'finished', result = '{"kind":"cancelled","cancelConfirmed":true,"source":"queued","author":"queue","epoch":1}'::jsonb`);
    assert.equal((await stored.step()).decision.reason, 'result-unbound', 'author queue on a row that is not cancelled is not trusted either');
  },

  'T37: artifacts are durable up through the worktree root before the installed marker, and the result is durable before the attempt record goes': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await finishWithArtifacts(fx, '{"deep":1}');
    const recorder = await recorded(() => fx.step());
    const root = fs.realpathSync(fx.worktreeRoot);
    const at = (op, target) => recorder.events.findIndex((e) => e.op === op && e.path === target);
    const marker = path.join(pullTicketDir(fx.state, fx.id), 'installed.json');
    const barriers = [installed(fx), path.join(root, 'reports/run'), path.join(root, 'reports'), root].map((target) => {
      assert.ok(at('fsync', target) >= 0, `no fsync of ${target}`);
      return at('fsync', target);
    });
    assert.ok(Math.max(...barriers) < at('rename', marker), 'the installed marker was written before every barrier');
    const resultTmpSync = recorder.events.findIndex((e) => e.op === 'fsync' && e.path && path.dirname(e.path) === paths(fx.state).results);
    const resultRename = at('rename', fx.resultPath);
    const resultDirSync = recorder.events.findIndex((e, i) => i > resultRename && e.op === 'fsync' && e.path === paths(fx.state).results);
    const attemptUnlink = recorder.events.findIndex((e) => e.op === 'unlink' && e.path === path.join(paths(fx.state).attempts, `${fx.id}.json`));
    assert.ok(resultTmpSync >= 0 && resultTmpSync < resultRename, 'the result was renamed before its bytes were fsynced');
    assert.ok(resultDirSync > resultRename && resultDirSync < attemptUnlink, 'the attempt record went before the result was durable');
  },
};

const SKIP_BARRIERS = 'for (const dir of dirs) ensureDurableDir(dir, { boundary: realRoot });';
const EARLY_MARKER = '  const result = installArtifacts(worktreeRoot, files, patterns);\n';

const CANARIES = [
  ['T37b: with the attempt and the result both gone, the ledger entry rebuilds the result durably, once', 'pull-ticket.js', ['reconstructResult(run, entry, extra));', "{ state: 'settled', already: true, decision: entry.decision });"]],
  ['an entry ledgered for one generation applies nothing to an attempt bumped to the next, even when the caller follows the bump', 'pull-ticket.js', ['attempt.generation !== run.generation || entry.generation !== attempt.generation', 'attempt.generation !== run.generation']],
  ['a stale-generation caller creates no ledger entry, so the correct caller still settles the ticket', 'pull-ticket.js', ["    if (attempt && attempt.generation !== run.generation) return { state: 'stale' };\n", '']],
  ['a stale generation applies no effect: nothing is appended, installed or published', 'pull-ticket.js', ["  if (attempt && (attempt.generation !== run.generation || entry.generation !== attempt.generation)) return { state: 'stale', decision: entry.decision };\n", '']],
  ['a queue outage never delays a local publish: the result is written before the divergence check, which has its own short deadline', 'pull-ticket.js', ['const DIVERGENCE_DEADLINE_MS = 2_000;', 'const DIVERGENCE_DEADLINE_MS = 60_000;'], /still running/],
  ['T7: after a crash behind the ledger, a different result on the row is reported and refused: the accepted one is published, nothing of the other is read', 'pull-ticket.js', ['    if (entry !== null) return applyEntry(run, entry);\n', '']],
  ['T25: a ledger entry decides alone: an invalid result, a valid one with other artifacts, a restored non-terminal row and an unreachable queue change nothing', 'pull-ticket.js', ['    if (entry !== null) return applyEntry(run, entry);\n', '']],
  ['T25: three steppers on one dead supervisor create one ledger entry, install once and publish once', 'pull-ticket.js', ['return withTicketLock(run.root, run.id, async () => {\n    const entry', 'return ((root, id, fn) => fn())(run.root, run.id, async () => {\n    const entry']],
  ['a runner-forged server cancel is never queue-authored: only author queue on a cancelled row is trusted', 'pull-ticket.js', ["row?.state === 'cancelled' && row.result?.author === 'queue' && ", "row?.result?.author === 'queue' && "]],
  ['a runner-forged server cancel is never queue-authored: only author queue on a cancelled row is trusted', 'pull-ticket.js', ["row?.state === 'cancelled' && row.result?.author === 'queue' && ", "row?.state === 'cancelled' && "]],
  ['T26: the published exit equals the ledgered decision whether or not a local cancel marker is present', 'pull-ticket.js', ["    cancelPrecedence: 'decision',\n", '']],
  ['T26: the published exit equals the ledgered decision whether or not a local cancel marker is present', 'pull-ticket.js', ['const outcome = ledgerAccept(run.root, entry, { hooks: run.hooks?.ledger });', 'const outcome = { entry: { v: 1, ...entry }, same: true };'], /changed while it was being applied/],
  ['T33: after a creator died behind the link, recovery makes the ledger directory durable before any effect, parents of the existing ticket directory first', 'pull-ledger.js', ['  ensureDurableDir(ledgerDir(root), { boundary: root });\n  return validate(JSON.parse(text));', '  return validate(JSON.parse(text));']],
  ['T34: every queue-authored cancel survives a restart through the real classifier and publisher', 'pull-ticket.js', ['if (isQueueAuthored(row)) {', 'if (false) {']],
  ['T34: every queue-authored cancel survives a restart through the real classifier and publisher', 'pull-ticket.js', ["    return result.source === 'queued' ? { kind: 'cancelled', exit: 130, source: 'queued' } : { kind: 'unproven', source: result.source };", "    return { kind: 'cancelled', exit: 130, source: result.source };"]],
  ['T37: artifacts are durable up through the worktree root before the installed marker, and the result is durable before the attempt record goes', 'remote-artifacts.js', [SKIP_BARRIERS, 'for (const dir of dirs) fsyncFile(dir);']],
  ['T37: artifacts are durable up through the worktree root before the installed marker, and the result is durable before the attempt record goes', 'remote-artifacts.js', [EARLY_MARKER, `${EARLY_MARKER}  if (installedMarkerPath) atomicWriteJson(installedMarkerPath, { paths: result.written }, { fsync: true });\n`]],
  ['T37: artifacts are durable up through the worktree root before the installed marker, and the result is durable before the attempt record goes', 'pull-ticket.js', ['    durable: true,\n', '    durable: false,\n']],
];

definePullSuite('pull ticket ledger and durability on a disposable Postgres', SCENARIOS, CANARIES);
