import fs from 'node:fs';
import path from 'node:path';
import { paths, atomicWriteJson, appendHistory, LockTimeoutError, RUNNER_SHUTDOWN_REASON } from './state.js';
import { readUnreachable, recordContact } from './runner-unreachable.js';
import { readAttempt, supervisorAlive, publishTerminal, remoteCancelledResult } from './attempts.js';
import { terminalRowDefaults } from './resources.js';
import { fetchRemoteResult, classifyRemoteResult, provesNeverStarted, provesStopped, remoteCancel } from './remote-client.js';
import { relayedUsage, relayedDeps } from './remote-relay.js';
import { writeBrokerLog } from './admission.js';

/**
 * BRAIN-515: settle an ORPHANED-REMOTE attempt (remote executor, supervisor dead) from the runner's own answer. This NEVER
 * runs anything again: a job that may still be running on a runner is never re-run by the broker (BRAIN-437/363). It settles
 * only on
 *  - a runner result record that `classifyRemoteResult` CONFIRMS against the binding stored at dispatch (adopted: the
 *    runner's exit is kept), or
 *  - a runner-CONFIRMED `remote-cancel` of a job the runner reports gone with no result (lost: exit 75, `runner-lost`), or
 *  - (BRAIN-570) a binding-matched runner record that itself proves the job never started (`provesNeverStarted`: rejected,
 *    queue-timeout, withdrawn; lost: exit 75, `runner-never-started`), or
 *  - (BRAIN-350) a binding-matched runner record that proves its host shut down and stopped the job (`provesStopped`; lost: exit 75,
 *    `runner-shutdown`), or
 *  - a probe-phase attempt that never reached a runner (no runner, no dispatchedRunners, no binding: the supervisor persists all
 *    three before its first ssh submit, so nothing was ever sent; lost: exit 75, `never-dispatched`, no ssh at all).
 *  - (BRAIN-594) a runner the operator marked `offline: true` (lost: exit 75, `runner-offline`, settled WITHOUT contacting it), or a
 *    runner that has given no usable answer for longer than `runnerLostAfterMs`, measured from the first failed fetch and persisted
 *    in `runner-unreachable.json` (lost: exit 75, `runner-lost`). Any answer from the runner, `running` included, clears that clock.
 * Anything else leaves the attempt untouched, with the state saying why: `running` (the runner still has it), `unreachable`
 * (no usable answer), `ambiguous` (a record that cannot be trusted), `skipped` (not an orphan, or already settled).
 *
 * `deadlineMs` bounds each ssh call; `deadlineAt` (ms epoch, optional) bounds the whole reconcile: every call gets at most what is
 * left of it, and the next call is not made when under `MIN_CALL_MS` remain (`{ state: 'unreachable', timedOut: true }`).
 *
 * `now` (ms epoch) is the clock for the unreachable bound; `assumeDown` skips contacting the runner because this sweep already found it
 * unreachable (only the offline flag and the bound can settle then).
 *
 * `self` (BRAIN-350 S3): the caller is the supervisor that OWNS the attempt (`attempt.supervisor.pid === process.pid`), waiting for proof
 * before it re-dispatches. It is not skipped for being alive, its publish fence accepts itself, an `adopted` outcome is published as
 * usual, but a PROVEN `lost` outcome (runner-offline, a runner-confirmed gone, runner-shutdown, runner-never-started) is NOT published: it returns
 * `{ settled: false, state: 'lost', reason, lostDetail }` for the caller to re-dispatch on. The `runnerLostAfterMs` bound is no proof (a partitioned
 * runner may still run the job, BRAIN-437), so it is published like any caller's (exit 75, `runner-lost`) and comes back settled. `runner` pins the
 * runner object captured at dispatch (self mode only), so a config reload cannot swap the host asked; only its `offline` flag is read from `globalCfg`.
 *
 * Returns `{ settled, state, exit?, reason?, transport?, timedOut? }` (`transport: true`: the runner gave no usable answer at all, so
 * asking it about another attempt would fail the same way); never throws.
 */
export async function reconcileRemoteAttempt(root, id, { globalCfg, sshBin = 'ssh', env, deadlineMs = 30_000, deadlineAt, now = Date.now(), assumeDown = false, self = false, runner } = {}) {
  try {
    return await reconcile(root, id, { globalCfg, sshBin, env, deadlineMs, deadlineAt, now, assumeDown, self, pinnedRunner: self ? runner : undefined });
  } catch (err) {
    // the clock lock stayed held past the caller's deadline: the same outcome as an ssh call that ran out of time
    if (err instanceof LockTimeoutError && deadlineAt !== undefined) return waitTimedOut();
    return { settled: false, state: 'error', reason: err.message };
  }
}

/** The attempt's supervisor is this very process and the caller said it is the one asking. */
const ownedBySelf = (attempt, self) => self === true && attempt.supervisor?.pid === process.pid;

const skipped = (reason) => ({ settled: false, state: 'skipped', reason });

const MIN_CALL_MS = 250;
const waitTimedOut = () => ({ settled: false, state: 'unreachable', reason: 'wait timeout reached', timedOut: true });

async function reconcile(root, id, { globalCfg, sshBin, env, deadlineMs, deadlineAt, now, assumeDown, self, pinnedRunner }) {
  const resultPath = path.join(paths(root).results, `${id}.json`);
  const attempt = readAttempt(root, id);
  if (!attempt) return skipped('no attempt record');
  if (attempt.executor !== 'remote') return skipped('not a remote attempt');
  if (supervisorAlive(attempt) && !ownedBySelf(attempt, self)) return skipped('its supervisor is alive');
  if (attempt.moving) return skipped('a runner-to-runner move was interrupted');
  if (fs.existsSync(resultPath)) return skipped('already settled');

  if (neverDispatched(attempt)) {
    return publishSettled(root, id, attempt, resultPath, 'lost', () => lostResult(id, attempt, 'never-dispatched', 'supervisor died before dispatching to any runner'), { stillEligible: neverDispatched, self });
  }

  const configured = (globalCfg?.runners ?? []).find((r) => r.name === attempt.runner);
  const runner = pinnedRunner ?? configured;
  if (!runner) return { settled: false, state: 'unreachable', reason: 'runner not configured' };

  if (pinnedRunner ? configured?.offline : runner.offline) {
    return publishSettled(root, id, attempt, resultPath, 'lost', () => lostResult(id, attempt, 'runner-offline', `runner ${runner.name} is marked offline in the config; not contacted`), { self });
  }
  const publishUnreachable = (since) => {
    const unreachableMs = now - since;
    if (!(globalCfg.runnerLostAfterMs > 0) || unreachableMs <= globalCfg.runnerLostAfterMs) return null;
    return publishSettled(root, id, attempt, resultPath, 'lost', () =>
      lostResult(id, attempt, 'runner-lost', `runner ${runner.name} unreachable for ${Math.round(unreachableMs / 60_000)} min (limit ${Math.round(globalCfg.runnerLostAfterMs / 60_000)} min)`),
      { self, boundOnly: true },
    );
  };
  const unreachable = async (outcome) => {
    const since = readUnreachable(root)[runner.name];
    return (since === undefined ? null : await publishUnreachable(since)) ?? outcome;
  };
  if (assumeDown) return unreachable({ settled: false, state: 'unreachable', transport: true, reason: `no usable answer from ${runner.name}` });

  /** The time the next ssh call may take, or null when too little of the overall deadline is left to make it. */
  const callBudget = () => {
    const ms = deadlineAt === undefined ? deadlineMs : Math.min(deadlineMs, deadlineAt - Date.now());
    return ms < MIN_CALL_MS ? null : ms;
  };

  // the runner's ticket id: the lane id, except after a cache repair retry (BRAIN-642), which dispatched under a fresh one
  const remoteId = attempt.binding?.ticketId ?? id;
  const firstBudget = callBudget();
  if (firstBudget === null) return waitTimedOut();
  const fetched = await fetchRemoteResult(runner, remoteId, sshBin, firstBudget, env);
  if (fetched === null) {
    const since = await recordContact(root, runner.name, false, now, { deadlineAt });
    return (await publishUnreachable(since)) ?? { settled: false, state: 'unreachable', transport: true, reason: `no usable answer from ${runner.name}` };
  }
  await recordContact(root, runner.name, true, now, { deadlineAt });

  const publish = (kind, build) => publishSettled(root, id, attempt, resultPath, kind, build, { self });

  if (!fetched.missing) return await adoptOrAmbiguous(attempt, id, fetched, publish);
  if (fetched.state === 'queued' || fetched.state === 'running') return { settled: false, state: 'running' };
  if (fetched.state !== 'gone') return { settled: false, state: 'ambiguous', reason: 'the runner reported no job state' };

  // The runner's job is gone with no result. Only a runner-confirmed cancel makes that safe to settle: the job can no longer
  // start or still be running there.
  const cancelBudget = callBudget();
  if (cancelBudget === null) return waitTimedOut();
  if (!(await remoteCancel(runner, remoteId, { sshBin, deadlineMs: cancelBudget, env }))) {
    // an unconfirmed cancel is no answer to build on: the rest of a sweep treats this runner as down, like a failed fetch
    return { settled: false, state: 'unreachable', transport: true, reason: 'remote cancel not confirmed' };
  }
  // The job may have published a result between the first fetch and the cancel; that real outcome wins over "lost".
  const lateBudget = callBudget();
  if (lateBudget === null) return waitTimedOut();
  const late = await fetchRemoteResult(runner, remoteId, sshBin, lateBudget, env);
  if (late && !late.missing) {
    const adopted = await adoptOrAmbiguous(attempt, id, late, publish);
    if (adopted.state !== 'ambiguous') return adopted;
  }
  return publish('lost', () => lostResult(id, attempt, 'runner-lost', attempt.unresolved ?? 'supervisor gone; runner reported the job gone'));
}

const lostResult = (id, attempt, reason, lostDetail) => ({
  id,
  exit: 75,
  signal: null,
  executor: 'remote',
  runner: attempt.runner ?? null,
  reason,
  lostDetail,
  startedAt: attempt.startedAt,
  endedAt: Date.now(),
  reconciled: true,
});

/** A probe-phase attempt that nothing was ever sent for: the supervisor writes `runner`, `dispatchedRunners` and `binding` to the
 *  attempt BEFORE the ssh submit (supervisor.js `updateAttempt` ahead of `dispatchRemote`, whose `onBinding` precedes its spawn). */
const neverDispatched = (attempt) => attempt.phase === 'probe' && !attempt.runner && !attempt.binding && !(attempt.dispatchedRunners?.length > 0);

/** A runner record is adopted only when it is confirmed against the binding stored at dispatch; a legacy attempt has none. */
async function adoptOrAmbiguous(attempt, id, record, publish) {
  const { binding } = attempt;
  if (!binding) return { settled: false, state: 'ambiguous', reason: 'legacy attempt has no dispatch binding' };
  const classified = classifyRemoteResult(record, { protocol: binding.protocol, ticketId: binding.ticketId ?? id, generation: binding.generation, manifestHash: binding.manifestHash });
  if (classified.outcome !== 'confirmed') {
    const expected = { protocol: binding.protocol, ticketId: binding.ticketId ?? id, generation: binding.generation, manifestHash: binding.manifestHash };
    if (provesNeverStarted(record, expected)) {
      const kind = record.reason ? `${record.kind}: ${record.reason}` : record.kind;
      return publish('lost', () => lostResult(id, attempt, 'runner-never-started', `runner ${attempt.runner} reported ${kind}`));
    }
    if (provesStopped(record, expected)) return publish('lost', () => lostResult(id, attempt, RUNNER_SHUTDOWN_REASON, `runner ${attempt.runner} stopped the job: host shutting down`));
    return { settled: false, state: 'ambiguous', reason: classified.reason };
  }
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
async function publishSettled(root, id, attempt, resultPath, kind, buildResult, { stillEligible = () => true, self = false, boundOnly = false } = {}) {
  if (self && kind === 'lost' && !boundOnly) {
    const { reason, lostDetail } = buildResult();
    return { settled: false, state: 'lost', reason, lostDetail };
  }
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
        (!supervisorAlive(cur) || ownedBySelf(cur, self)) &&
        stillEligible(cur),
    },
  );
  if (published.alreadyPublished) return { settled: true, state: 'already-settled' };
  if (!published.ok) return skipped('attempt changed');
  if (published.cancelled) return { settled: true, state: 'cancelled', exit: written.exit };
  writeBrokerLog(
    root,
    kind === 'adopted' ? `lane: reconcile: ${id}: ${attempt.runner}: adopted exit ${written.exit}\n` : `lane: reconcile: ${id}: ${attempt.runner ?? 'no runner'}: lost (${written.reason})\n`,
  );
  return { settled: true, state: kind, exit: written.exit };
}
