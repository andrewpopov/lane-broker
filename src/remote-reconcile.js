import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, appendHistory } from './state.js';
import { readAttempt, supervisorAlive, publishTerminal, remoteCancelledResult } from './attempts.js';
import { terminalRowDefaults } from './resources.js';
import { fetchRemoteResult, classifyRemoteResult, remoteCancel } from './remote-client.js';
import { relayedUsage, relayedDeps } from './remote-relay.js';
import { writeBrokerLog } from './admission.js';

/**
 * BRAIN-515: settle an ORPHANED-REMOTE attempt (remote executor, supervisor dead) from the runner's own answer. This NEVER
 * runs anything again: a job that may still be running on a runner is never re-run by the broker (BRAIN-437/363). It settles
 * only on
 *  - a runner result record that `classifyRemoteResult` CONFIRMS against the binding stored at dispatch (adopted: the
 *    runner's exit is kept), or
 *  - a runner-CONFIRMED `remote-cancel` of a job the runner reports gone with no result (lost: exit 75, `runner-lost`).
 * Anything else leaves the attempt untouched, with the state saying why: `running` (the runner still has it), `unreachable`
 * (no usable answer), `ambiguous` (a record that cannot be trusted), `skipped` (not an orphan, or already settled).
 *
 * Returns `{ settled, state, exit?, reason? }`; never throws.
 */
export async function reconcileRemoteAttempt(root, id, { globalCfg, sshBin = 'ssh', env, deadlineMs = 30_000 } = {}) {
  try {
    return await reconcile(root, id, { globalCfg, sshBin, env, deadlineMs });
  } catch (err) {
    return { settled: false, state: 'error', reason: err.message };
  }
}

const skipped = (reason) => ({ settled: false, state: 'skipped', reason });

async function reconcile(root, id, { globalCfg, sshBin, env, deadlineMs }) {
  const resultPath = path.join(paths(root).results, `${id}.json`);
  const attempt = readAttempt(root, id);
  if (!attempt) return skipped('no attempt record');
  if (attempt.executor !== 'remote') return skipped('not a remote attempt');
  if (supervisorAlive(attempt)) return skipped('its supervisor is alive');
  if (attempt.moving) return skipped('a runner-to-runner move was interrupted');
  if (fs.existsSync(resultPath)) return skipped('already settled');

  const runner = (globalCfg?.runners ?? []).find((r) => r.name === attempt.runner);
  if (!runner) return { settled: false, state: 'unreachable', reason: 'runner not configured' };

  const fetched = await fetchRemoteResult(runner, id, sshBin, deadlineMs, env);
  if (fetched === null) return { settled: false, state: 'unreachable', reason: `no usable answer from ${runner.name}` };

  const publish = (kind, build) => publishSettled(root, id, attempt, runner, resultPath, kind, build);

  if (!fetched.missing) return await adoptOrAmbiguous(attempt, id, fetched, publish);
  if (fetched.state === 'queued' || fetched.state === 'running') return { settled: false, state: 'running' };
  if (fetched.state !== 'gone') return { settled: false, state: 'ambiguous', reason: 'the runner reported no job state' };

  // The runner's job is gone with no result. Only a runner-confirmed cancel makes that safe to settle: the job can no longer
  // start or still be running there.
  if (!(await remoteCancel(runner, id, { sshBin, deadlineMs, env }))) {
    return { settled: false, state: 'unreachable', reason: 'remote cancel not confirmed' };
  }
  // The job may have published a result between the first fetch and the cancel; that real outcome wins over "lost".
  const late = await fetchRemoteResult(runner, id, sshBin, deadlineMs, env);
  if (late && !late.missing) {
    const adopted = await adoptOrAmbiguous(attempt, id, late, publish);
    if (adopted.state !== 'ambiguous') return adopted;
  }
  return publish('lost', () => ({
    id,
    exit: 75,
    signal: null,
    executor: 'remote',
    runner: runner.name,
    reason: 'runner-lost',
    lostDetail: attempt.unresolved ?? 'supervisor gone; runner reported the job gone',
    startedAt: attempt.startedAt,
    endedAt: Date.now(),
    reconciled: true,
  }));
}

/** A runner record is adopted only when it is confirmed against the binding stored at dispatch; a legacy attempt has none. */
async function adoptOrAmbiguous(attempt, id, record, publish) {
  const { binding } = attempt;
  if (!binding) return { settled: false, state: 'ambiguous', reason: 'legacy attempt has no dispatch binding' };
  const classified = classifyRemoteResult(record, { protocol: binding.protocol, ticketId: id, generation: binding.generation, manifestHash: binding.manifestHash });
  if (classified.outcome !== 'confirmed') return { settled: false, state: 'ambiguous', reason: classified.reason };
  return publish('adopted', () => ({
    id,
    exit: classified.exitCode,
    signal: null,
    executor: 'remote',
    runner: attempt.runner,
    remoteKind: record.kind,
    remotePhase: record.phase ?? null,
    ...relayedUsage(record),
    ...relayedDeps(record),
    startedAt: attempt.startedAt,
    endedAt: Date.now(),
    reconciled: true,
  }));
}

/** Publish through the shared exactly-once path. The fence ties the publish to the attempt this reconcile read: a record that
 *  changed hands (another runner, a restarted supervisor, a move) meanwhile is left alone. */
async function publishSettled(root, id, attempt, runner, resultPath, kind, buildResult) {
  let written = null;
  const published = await publishTerminal(
    root,
    id,
    attempt.generation,
    ({ cancelled }) => {
      if (cancelled) {
        // cancel precedence, same shape `lane cancel` writes
        written = remoteCancelledResult(id, attempt.startedAt);
        atomicWriteJson(resultPath, written);
        appendHistory(root, { ...terminalRowDefaults(attempt), id, exit: 130, cancelled: true, endedAt: Date.now(), executor: 'remote', runner: attempt.runner });
        return;
      }
      written = buildResult();
      atomicWriteJson(resultPath, written);
      appendHistory(root, { ...terminalRowDefaults(attempt), ...written });
    },
    {
      expect: (cur) =>
        cur.executor === 'remote' &&
        cur.runner === attempt.runner &&
        cur.generation === attempt.generation &&
        cur.startedAt === attempt.startedAt &&
        !cur.moving &&
        !supervisorAlive(cur),
    },
  );
  if (published.alreadyPublished) return { settled: true, state: 'already-settled' };
  if (!published.ok) return skipped('attempt changed');
  if (published.cancelled) return { settled: true, state: 'cancelled', exit: written.exit };
  writeBrokerLog(
    root,
    kind === 'adopted' ? `lane: reconcile: ${id}: ${runner.name}: adopted exit ${written.exit}\n` : `lane: reconcile: ${id}: ${runner.name}: lost (runner-lost)\n`,
  );
  return { settled: true, state: kind, exit: written.exit };
}
