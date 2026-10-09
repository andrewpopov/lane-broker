import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { paths, stateHome } from './state.js';
import { isCanonicalRelPath } from './remote-manifest.js';
import { unsupportedHookKeys, hookRefusalMessage } from './exclusive.js';
import { PRIORITY_TIERS, DEFAULT_PRIORITY, isPriorityTier } from './priority.js';
import { DEFAULT_DEPS_CACHE_MAX_BYTES } from './deps-cache.js';

export const DEFAULT_GLOBAL_CONFIG = {
  version: 1,
  capacity: 'auto',
  loadClose: 15,
  loadOpen: 11,
  loadOpenSamples: 3,
  sampleMs: 5000,
  // Phase 1 (ZIRK scheduler project) additions — see src/admission.js. All
  // optional-with-defaults so a version-1 config written before this phase
  // still loads unchanged.
  cpuClosePercent: 90,
  cpuOpenPercent: 70,
  cpuAdmissionPercent: 75,
  cpuOpenSamples: 3,
  admissionCooldownMs: 5000,
  memoryCloseBytes: 4294967296,
  memoryOpenBytes: 8589934592,
  cpuReserveCores: 1,
  // BRAIN-428: processes at nice >= preemptibleNiceMin are preemptible (the kernel runs a nice-0 lane ahead
  // of them), so admission counts preemptibleShare of that load as available. 0 disables the feature.
  preemptibleNiceMin: 1,
  preemptibleShare: 0.8,
  memoryReserveBytes: 2147483648,
  defaultMemoryBytesPerWeight: 1073741824,
  schedulerMode: 'active',
  // BRAIN-354: once a lease is settled, admission charges its measured CPU
  // (trailing-window peak * headroom, floored at floorFraction of its booking
  // and capped at the booking) instead of the full booking. Settled = admitted
  // at least settleMs ago with >= 2 fresh observations in the last windowMs.
  // false restores the pre-BRAIN-354 max(observed, booking) charge.
  settledDemandEnabled: true,
  settledDemandSettleMs: 120_000,
  settledDemandWindowMs: 180_000,
  settledDemandHeadroom: 1.25,
  settledDemandFloorFraction: 0.5,
  // BRAIN-433: charge a lane its history-informed CPU (p90 of its recent runs' mean cores, capped at its
  // declaration) instead of the declaration, once it has historyDemandMinRuns successful runs.
  historyDemandEnabled: true,
  historyDemandMinRuns: 5,
  // BRAIN-454: history may also RAISE a charge for an under-declared lane (p90 above its declaration), capped at the host CPU budget.
  historyDemandRaise: true,
  // BRAIN-452: the heaviest weight a non-exclusive lane may declare; identical on every host so a submission valid on one is valid on all.
  maxLaneWeight: 4,
  // Second, separate body of work (conflict-skip starvation bound — see
  // selectCandidate() in src/scheduler.js): how many times a conflict-blocked
  // FIFO head may be skipped in favor of a later, non-conflicting ticket
  // before the skip is refused and the head is left to block until it can
  // run itself. Without this, three or more conflicting keys can starve a
  // head forever by alternating which other ticket is held.
  conflictSkipLimit: 3,
  // BRAIN-249: conflictSkipLimit bounds how many times a conflict-blocked
  // head may be SKIPPED, but nothing previously bounded how LONG it could
  // then sit blocked once that allowance ran out -- if the lease holding the
  // head's key is a legitimately long-running job (hours, not a crash),
  // skip-exhaustion would refuse backfill forever, turning one blocked
  // ticket into every ticket behind it also blocked, machine-wide. Once the
  // head has been sitting blocked for at least this long, resume
  // backfilling past it even with the skip allowance exhausted -- see
  // headBlockedMs/resolveHeadBlock in src/scheduler.js. A reclaim-by-time
  // that steals the held KEY itself (killing the long-running job) was
  // explicitly rejected for the same incident this exists to fix: it would
  // have killed a genuine multi-hour run just because it outlasted a
  // timer. This only changes who else is allowed to start; it never touches
  // the lease already holding the key.
  headBlockGraceMs: 600_000,
  // BRAIN-355: once a conflict-blocked head has used its skip allowance, still
  // start a later ticket that cannot delay it -- one that conflicts with the
  // head in neither direction and fits capacity, projected CPU and memory
  // together with the head's own claim. Such a start is not counted as a skip.
  // false restores the BRAIN-249 refusal of all backfill for headBlockGraceMs.
  conflictSafeBackfill: true,
  // BRAIN-346: how many tickets may start behind a FIFO head denied only by
  // projected-over-budget CPU before the head is reserved (nothing else is
  // admitted past it). 0 restores strict FIFO for resource denials.
  resourceSkipLimit: 3,
  // BRAIN-346: a reserved head on a fully idle broker may start when its
  // projection overshoots the CPU budget by at most this many cores (ambient
  // load can make a big lane unsatisfiable even with nothing running).
  // 0 disables the exemption.
  resourceIdleOvershootCores: 1,
  // BRAIN-338: how many tickets may already be queued on the least-loaded
  // runner for a remote-eligible ticket to be queued there instead of
  // running locally -- only when this machine could not admit it right now
  // either. 0 disables (a runner with any queue is skipped).
  maxRemoteQueue: 2,
  // BRAIN-405: how often a remote-eligible ticket that fell back to this machine's queue (no runner had room) re-probes the
  // runners and moves to one that now has real headroom. 0 disables rebinding.
  remoteRebindIntervalMs: 30_000,
  // BRAIN-442: a `remotePolicy: "local-first"` lane queues locally and becomes rebind-eligible only after waiting this long
  // there (a lane may override it with its own `localFirstWaitMs`). Rebinding still needs `remoteRebindIntervalMs` > 0.
  localFirstWaitMs: 90_000,
  // BRAIN-436: a remote ticket that has sat QUEUED on its runner this long is withdrawn and moved to another runner that has
  // real headroom now (needs the runner's `remote-withdraw/1`). 0 disables. The interval is how often the submitter looks, the
  // cooldown is the minimum gap between two moves of one ticket, and the cap is how many times one ticket may move.
  remoteRebalanceMinQueuedMs: 300_000,
  remoteRebalanceIntervalMs: 60_000,
  remoteRebalanceCooldownMs: 600_000,
  remoteRebalanceMaxMoves: 1,
  // BRAIN-207 (forgiving admission): whether the load gate is allowed to
  // deny a start at all. Default false — the gate is informational-only
  // (still sampled every poll, still logged) until an operator opts back
  // into hard denial. `true` reproduces every byte of the pre-BRAIN-207
  // behaviour, idle-exemption included.
  admissionLoadGate: false,
  // Lanes are spawned under `nice -n <laneNice>` by default so a heavy test
  // run doesn't starve interactive work on a shared machine. 0 disables
  // niceing (spawns the bare command). A `.lane-broker.json` lane's own
  // `nice` overrides this per-lane.
  laneNice: 10,
  // BRAIN-431: a run idle (no output, no CPU, no process churn) for this long is killed (exit 124, reason 'no-progress').
  // 0 (the default) never kills: quiet work is legitimate, so the kill is opted into per lane (`noProgressTimeoutMs`).
  // The stall is recorded either way.
  noProgressTimeoutMs: 0,
  // BRAIN-379 (balancer P2, slice 2): record, in admission-decisions.log and `lane status`, what
  // class-aware test/sim allocation WOULD decide. Never changes a live admission decision.
  allocationShadow: false,
  // BRAIN-379: how long after the last sim demand (a sim ticket queued or a sim lease charged) the
  // sim soft lock stays armed.
  simArmWindowMs: 300_000,
  // BRAIN-380: priority tiers. Each `priorityAgingMs` of waiting is worth one tier, and aging stops at
  // `priorityAgeMaxMs` (default 2 x agingMs, derived at load when unset). Score = min(W_tier,
  // W_tier*tierFactor + W_age*ageFactor); `fairshare` is a reserved slot and must stay 0.
  priorityAgingMs: 600_000,
  priorityAgeMaxMs: 1_200_000,
  priorityWeights: { tier: 2, age: 2, fairshare: 0 },
  // At most this many queued high tickets per repo per broker (0 disables high). Enforced in a later slice.
  maxQueuedHighPerRepo: 1,
  // BRAIN-389: on a remote RUNNER, reuse an installed node_modules tree (keyed by lockfile + environment) instead of
  // a fresh `npm ci` per run, within this many bytes of store (least recently used evicted). A lane's own
  // `remoteDepsCache: false` opts that lane out.
  remoteDepsCache: true,
  remoteDepsCacheMaxBytes: DEFAULT_DEPS_CACHE_MAX_BYTES,
  // BRAIN-398: caps on the files a remote run returns (`remoteArtifacts`), enforced by the runner that collects them and
  // again by the submitter that receives them, each from its own config.
  remoteArtifactMaxFileBytes: 16 * 1024 * 1024,
  remoteArtifactMaxTotalBytes: 64 * 1024 * 1024,
  remoteArtifactMaxCount: 200,
  // BRAIN-438: garbage collection of what a run leaves behind (src/gc.js). A runner's whole ticket directory (and a
  // tombstone) is kept this long, so it also bounds how late a `remote-exec` for a cancelled id can still be refused.
  remoteTicketRetentionMs: 7 * 24 * 3_600_000,
  // At most this many ticket directories are cleaned per run, so `remote-exec` startup stays fast.
  remoteGcMaxTicketsPerRun: 10,
  // ... and at most this many are even examined per run, so the scan under the broker lock stays short.
  remoteGcMaxExaminedPerRun: 50,
  // logs/ and results/ files older than this are pruned (never a live lease's or a queued ticket's), at most
  // logGcMaxFilesPerRun per run.
  logRetentionMs: 14 * 24 * 3_600_000,
  logGcMaxFilesPerRun: 5000,
};

export const DEFAULT_REPO_CONFIG = {
  version: 1,
  lanes: { default: { weight: 2 } },
  conflicts: [],
};

export function brokerHome() {
  return process.env.LANE_BROKER_HOME || path.join(os.homedir(), '.config', 'lane-broker');
}

/** Sanitize a repo/lane identifier down to [A-Za-z0-9_.-]. */
export function sanitizeKey(raw) {
  const s = String(raw).replace(/[^A-Za-z0-9_.-]/g, '_');
  return s.length ? s : '_';
}

function assert(cond, msg) {
  if (!cond) throw new ConfigError(msg);
}

export class ConfigError extends Error {}

/** A broken `classes` block (thrown by validateClasses; loadGlobalConfig turns it into `classesInvalid`). */
export class ClassesConfigError extends ConfigError {}

export const CLASS_MODES = ['off', 'shadow', 'active'];
const CLASSES_KEYS = ['mode', 'test', 'sim'];
const CLASSES_TEST_KEYS = ['reserveCores'];
const CLASSES_SIM_KEYS = ['capCores', 'maxTickets', 'capMemoryBytes'];

/**
 * BRAIN-321: the per-machine, per-class reservation caps. Absent = today's behaviour. `shadow` and `active` need every
 * number spelled out: a cap that silently defaults is a cap the operator never chose. Unknown keys are refused (a typo'd
 * cap must not read as "no cap").
 */
export function validateClasses(classes, sourcePath) {
  const fail = (msg) => {
    throw new ClassesConfigError(`${sourcePath}: "classes" ${msg}`);
  };
  const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
  if (!isObject(classes)) fail('must be an object');
  for (const key of Object.keys(classes)) if (!CLASSES_KEYS.includes(key)) fail(`has unknown key "${key}"`);
  if (!CLASS_MODES.includes(classes.mode)) fail(`"mode" must be one of ${CLASS_MODES.join(', ')}`);
  const needed = classes.mode !== 'off';
  const group = (name, allowed, check) => {
    const value = classes[name];
    if (value === undefined) {
      if (needed) fail(`"${name}" is required when mode is "${classes.mode}"`);
      return;
    }
    if (!isObject(value)) fail(`"${name}" must be an object`);
    for (const key of Object.keys(value)) if (!allowed.includes(key)) fail(`"${name}" has unknown key "${key}"`);
    for (const key of allowed) {
      if (value[key] === undefined) {
        if (needed) fail(`"${name}.${key}" is required when mode is "${classes.mode}"`);
      } else check(key, value[key]);
    }
  };
  const nonNegative = (name) => (key, v) => {
    if (!(Number.isFinite(v) && v >= 0)) fail(`"${name}.${key}" must be a non-negative number`);
  };
  group('test', CLASSES_TEST_KEYS, nonNegative('test'));
  group('sim', CLASSES_SIM_KEYS, (key, v) => {
    if (key === 'maxTickets' ? !(Number.isInteger(v) && v >= 0) : !(Number.isFinite(v) && v >= 0)) {
      fail(`"sim.${key}" must be a non-negative ${key === 'maxTickets' ? 'integer' : 'number'}`);
    }
  });
}

// A retention below a day could expire a tombstone while its submitter's dispatch is still in flight (see src/gc.js).
const MIN_RETENTION_MS = 86_400_000;

function validateGlobalConfig(cfg, sourcePath) {
  assert(cfg && typeof cfg === 'object', `${sourcePath}: config must be an object`);
  assert(Number.isInteger(cfg.version), `${sourcePath}: "version" must be an integer`);
  assert(
    cfg.capacity === 'auto' || (Number.isFinite(cfg.capacity) && cfg.capacity > 0),
    `${sourcePath}: "capacity" must be "auto" or a positive number`,
  );
  assert(Number.isFinite(cfg.loadClose) && cfg.loadClose > 0, `${sourcePath}: "loadClose" must be a positive number`);
  assert(Number.isFinite(cfg.loadOpen) && cfg.loadOpen > 0, `${sourcePath}: "loadOpen" must be a positive number`);
  assert(cfg.loadOpen < cfg.loadClose, `${sourcePath}: "loadOpen" must be less than "loadClose"`);
  assert(Number.isInteger(cfg.loadOpenSamples) && cfg.loadOpenSamples > 0, `${sourcePath}: "loadOpenSamples" must be a positive integer`);
  assert(Number.isFinite(cfg.sampleMs) && cfg.sampleMs > 0, `${sourcePath}: "sampleMs" must be a positive number`);
  assert(
    Number.isFinite(cfg.cpuClosePercent) && cfg.cpuClosePercent > 0 && cfg.cpuClosePercent <= 100,
    `${sourcePath}: "cpuClosePercent" must be a number in (0, 100]`,
  );
  assert(
    Number.isFinite(cfg.cpuOpenPercent) && cfg.cpuOpenPercent > 0 && cfg.cpuOpenPercent <= 100,
    `${sourcePath}: "cpuOpenPercent" must be a number in (0, 100]`,
  );
  assert(cfg.cpuOpenPercent < cfg.cpuClosePercent, `${sourcePath}: "cpuOpenPercent" must be less than "cpuClosePercent"`);
  assert(
    Number.isFinite(cfg.cpuAdmissionPercent) && cfg.cpuAdmissionPercent > 0 && cfg.cpuAdmissionPercent <= 100,
    `${sourcePath}: "cpuAdmissionPercent" must be a number in (0, 100]`,
  );
  assert(Number.isInteger(cfg.cpuOpenSamples) && cfg.cpuOpenSamples > 0, `${sourcePath}: "cpuOpenSamples" must be a positive integer`);
  assert(Number.isFinite(cfg.admissionCooldownMs) && cfg.admissionCooldownMs >= 0, `${sourcePath}: "admissionCooldownMs" must be a non-negative number`);
  assert(Number.isFinite(cfg.memoryCloseBytes) && cfg.memoryCloseBytes > 0, `${sourcePath}: "memoryCloseBytes" must be a positive number`);
  assert(Number.isFinite(cfg.memoryOpenBytes) && cfg.memoryOpenBytes > 0, `${sourcePath}: "memoryOpenBytes" must be a positive number`);
  assert(cfg.memoryCloseBytes < cfg.memoryOpenBytes, `${sourcePath}: "memoryCloseBytes" must be less than "memoryOpenBytes"`);
  assert(Number.isFinite(cfg.cpuReserveCores) && cfg.cpuReserveCores >= 0, `${sourcePath}: "cpuReserveCores" must be a non-negative number`);
  assert(Number.isInteger(cfg.preemptibleNiceMin) && cfg.preemptibleNiceMin >= 0 && cfg.preemptibleNiceMin <= 19, `${sourcePath}: "preemptibleNiceMin" must be an integer in [0, 19]`);
  assert(Number.isFinite(cfg.preemptibleShare) && cfg.preemptibleShare >= 0 && cfg.preemptibleShare <= 1, `${sourcePath}: "preemptibleShare" must be a number in [0, 1]`);
  assert(Number.isFinite(cfg.memoryReserveBytes) && cfg.memoryReserveBytes >= 0, `${sourcePath}: "memoryReserveBytes" must be a non-negative number`);
  assert(
    Number.isFinite(cfg.defaultMemoryBytesPerWeight) && cfg.defaultMemoryBytesPerWeight > 0,
    `${sourcePath}: "defaultMemoryBytesPerWeight" must be a positive number`,
  );
  assert(
    cfg.schedulerMode === 'shadow' || cfg.schedulerMode === 'active',
    `${sourcePath}: "schedulerMode" must be "shadow" or "active"`,
  );
  assert(typeof cfg.settledDemandEnabled === 'boolean', `${sourcePath}: "settledDemandEnabled" must be a boolean`);
  assert(Number.isFinite(cfg.settledDemandSettleMs) && cfg.settledDemandSettleMs >= 0, `${sourcePath}: "settledDemandSettleMs" must be a non-negative number`);
  assert(Number.isFinite(cfg.settledDemandWindowMs) && cfg.settledDemandWindowMs > 0, `${sourcePath}: "settledDemandWindowMs" must be a positive number`);
  assert(Number.isFinite(cfg.settledDemandHeadroom) && cfg.settledDemandHeadroom >= 1, `${sourcePath}: "settledDemandHeadroom" must be a number >= 1`);
  assert(
    Number.isFinite(cfg.settledDemandFloorFraction) && cfg.settledDemandFloorFraction >= 0 && cfg.settledDemandFloorFraction <= 1,
    `${sourcePath}: "settledDemandFloorFraction" must be a number in [0, 1]`,
  );
  assert(typeof cfg.historyDemandEnabled === 'boolean', `${sourcePath}: "historyDemandEnabled" must be a boolean`);
  assert(Number.isInteger(cfg.maxLaneWeight) && cfg.maxLaneWeight >= 1, `${sourcePath}: "maxLaneWeight" must be a positive integer`);
  assert(typeof cfg.historyDemandRaise === 'boolean', `${sourcePath}: "historyDemandRaise" must be a boolean`);
  assert(Number.isInteger(cfg.historyDemandMinRuns) && cfg.historyDemandMinRuns >= 1, `${sourcePath}: "historyDemandMinRuns" must be a positive integer`);
  assert(
    Number.isInteger(cfg.conflictSkipLimit) && cfg.conflictSkipLimit >= 0,
    `${sourcePath}: "conflictSkipLimit" must be a non-negative integer`,
  );
  assert(
    Number.isInteger(cfg.headBlockGraceMs) && cfg.headBlockGraceMs >= 0,
    `${sourcePath}: "headBlockGraceMs" must be a non-negative integer`,
  );
  assert(typeof cfg.conflictSafeBackfill === 'boolean', `${sourcePath}: "conflictSafeBackfill" must be a boolean`);
  assert(
    Number.isInteger(cfg.resourceSkipLimit) && cfg.resourceSkipLimit >= 0,
    `${sourcePath}: "resourceSkipLimit" must be a non-negative integer`,
  );
  assert(
    Number.isFinite(cfg.resourceIdleOvershootCores) && cfg.resourceIdleOvershootCores >= 0,
    `${sourcePath}: "resourceIdleOvershootCores" must be a non-negative number`,
  );
  assert(
    Number.isInteger(cfg.maxRemoteQueue) && cfg.maxRemoteQueue >= 0,
    `${sourcePath}: "maxRemoteQueue" must be a non-negative integer`,
  );
  assert(
    Number.isInteger(cfg.remoteRebindIntervalMs) && cfg.remoteRebindIntervalMs >= 0,
    `${sourcePath}: "remoteRebindIntervalMs" must be a non-negative integer`,
  );
  assert(
    Number.isInteger(cfg.localFirstWaitMs) && cfg.localFirstWaitMs >= 0,
    `${sourcePath}: "localFirstWaitMs" must be a non-negative integer`,
  );
  for (const field of ['remoteRebalanceMinQueuedMs', 'remoteRebalanceCooldownMs', 'remoteRebalanceMaxMoves']) {
    assert(Number.isInteger(cfg[field]) && cfg[field] >= 0, `${sourcePath}: "${field}" must be a non-negative integer`);
  }
  assert(Number.isInteger(cfg.remoteRebalanceIntervalMs) && cfg.remoteRebalanceIntervalMs > 0, `${sourcePath}: "remoteRebalanceIntervalMs" must be a positive integer`);
  assert(typeof cfg.admissionLoadGate === 'boolean', `${sourcePath}: "admissionLoadGate" must be a boolean`);
  assert(typeof cfg.allocationShadow === 'boolean', `${sourcePath}: "allocationShadow" must be a boolean`);
  assert(Number.isInteger(cfg.simArmWindowMs) && cfg.simArmWindowMs > 0, `${sourcePath}: "simArmWindowMs" must be a positive integer`);
  assert(
    Number.isInteger(cfg.laneNice) && cfg.laneNice >= 0 && cfg.laneNice <= 19,
    `${sourcePath}: "laneNice" must be an integer in [0, 19]`,
  );
  assert(Number.isInteger(cfg.noProgressTimeoutMs) && cfg.noProgressTimeoutMs >= 0, `${sourcePath}: "noProgressTimeoutMs" must be a non-negative integer`);
  // BRAIN-320 S1a: how long a remote ticket may sit queued on the runner
  // before it is cancelled and treated as unconfirmed (fallback to local).
  // Absent by default (not read yet -- that is a later slice) so a config
  // written before this field exists still loads unchanged (I6).
  if (cfg.remoteQueueTimeoutMs !== undefined) {
    assert(
      Number.isInteger(cfg.remoteQueueTimeoutMs) && cfg.remoteQueueTimeoutMs > 0,
      `${sourcePath}: "remoteQueueTimeoutMs" must be a positive integer`,
    );
  }
  // BRAIN-339: how long the client keeps waiting for a remote job's result after its ssh
  // session drops while the runner still reports the job queued/running. Absent means the
  // dispatcher's 3h default.
  if (cfg.remoteResultWaitMs !== undefined) {
    assert(
      Number.isInteger(cfg.remoteResultWaitMs) && cfg.remoteResultWaitMs > 0,
      `${sourcePath}: "remoteResultWaitMs" must be a positive integer`,
    );
  }
  assert(typeof cfg.remoteDepsCache === 'boolean', `${sourcePath}: "remoteDepsCache" must be a boolean`);
  assert(
    Number.isInteger(cfg.remoteDepsCacheMaxBytes) && cfg.remoteDepsCacheMaxBytes > 0,
    `${sourcePath}: "remoteDepsCacheMaxBytes" must be a positive integer`,
  );
  for (const field of ['remoteArtifactMaxFileBytes', 'remoteArtifactMaxTotalBytes', 'remoteArtifactMaxCount']) {
    assert(Number.isInteger(cfg[field]) && cfg[field] > 0, `${sourcePath}: "${field}" must be a positive integer`);
  }
  for (const field of ['remoteTicketRetentionMs', 'logRetentionMs']) {
    assert(Number.isInteger(cfg[field]) && cfg[field] >= MIN_RETENTION_MS, `${sourcePath}: "${field}" must be an integer of at least ${MIN_RETENTION_MS} (one day)`);
  }
  for (const field of ['remoteGcMaxTicketsPerRun', 'remoteGcMaxExaminedPerRun', 'logGcMaxFilesPerRun']) {
    assert(Number.isInteger(cfg[field]) && cfg[field] > 0, `${sourcePath}: "${field}" must be a positive integer`);
  }
  if (cfg.runners !== undefined) validateRunners(cfg.runners, sourcePath);
  validatePriorityConfig(cfg, sourcePath);
}

function validatePriorityConfig(cfg, sourcePath) {
  assert(
    Number.isInteger(cfg.priorityAgingMs) && cfg.priorityAgingMs >= 60_000 && cfg.priorityAgingMs <= 3_600_000,
    `${sourcePath}: "priorityAgingMs" must be an integer in [60000, 3600000]`,
  );
  assert(
    Number.isInteger(cfg.priorityAgeMaxMs) && cfg.priorityAgeMaxMs >= cfg.priorityAgingMs && cfg.priorityAgeMaxMs <= 86_400_000,
    `${sourcePath}: "priorityAgeMaxMs" must be an integer in [priorityAgingMs, 86400000]`,
  );
  const weights = cfg.priorityWeights;
  assert(weights && typeof weights === 'object' && !Array.isArray(weights), `${sourcePath}: "priorityWeights" must be an object`);
  for (const name of ['tier', 'age']) {
    assert(
      Number.isFinite(weights[name]) && weights[name] > 0 && weights[name] <= 100,
      `${sourcePath}: "priorityWeights.${name}" must be a number in (0, 100]`,
    );
  }
  assert(weights.age >= weights.tier, `${sourcePath}: "priorityWeights.age" must be >= "priorityWeights.tier" (a low ticket must be able to reach the ceiling)`);
  assert(weights.fairshare === 0, `${sourcePath}: "priorityWeights.fairshare" must be exactly 0 until its factor exists`);
  assert(
    Number.isInteger(cfg.maxQueuedHighPerRepo) && cfg.maxQueuedHighPerRepo >= 0,
    `${sourcePath}: "maxQueuedHighPerRepo" must be a non-negative integer`,
  );
}

const RUNNER_NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

/**
 * BRAIN-319 T3a: `runners` is optional and absent by default, so a config
 * written before this field exists still loads unchanged (I6). Each entry's
 * `ssh` is a destination string handed to the ssh binary as its OWN argv
 * element (never through a shell) -- rejecting a leading `-` here is the
 * only line of defense against it being read as an ssh option instead of a
 * destination.
 */
function validateRunners(runners, sourcePath) {
  assert(Array.isArray(runners), `${sourcePath}: "runners" must be an array`);
  const seenNames = new Set();
  for (const runner of runners) {
    assert(runner && typeof runner === 'object', `${sourcePath}: each "runners" entry must be an object`);
    assert(
      typeof runner.name === 'string' && RUNNER_NAME_RE.test(runner.name),
      `${sourcePath}: runner "name" must match ${RUNNER_NAME_RE}`,
    );
    assert(!seenNames.has(runner.name), `${sourcePath}: duplicate runner name "${runner.name}"`);
    seenNames.add(runner.name);
    assert(
      typeof runner.ssh === 'string' && runner.ssh.length > 0 && !runner.ssh.startsWith('-'),
      `${sourcePath}: runner "${runner.name}".ssh must be a non-empty string not starting with "-"`,
    );
    if (runner.root !== undefined) {
      assert(typeof runner.root === 'string' && runner.root.length > 0, `${sourcePath}: runner "${runner.name}".root must be a non-empty string`);
    }
    if (runner.shell !== undefined) {
      assert(typeof runner.shell === 'string' && runner.shell.length > 0, `${sourcePath}: runner "${runner.name}".shell must be a non-empty string`);
    }
    // BRAIN-436: whether a ticket withdrawn from another runner may be sent here. Free cores say little about a runner whose
    // other workloads outweigh lanes (e.g. sims running at a higher CPUWeight), so such a runner opts out.
    if (runner.rebalanceTarget !== undefined) {
      assert(typeof runner.rebalanceTarget === 'boolean', `${sourcePath}: runner "${runner.name}".rebalanceTarget must be a boolean`);
    }
    if (runner.speedFactor !== undefined) {
      assert(
        Number.isFinite(runner.speedFactor) && runner.speedFactor > 0,
        `${sourcePath}: runner "${runner.name}".speedFactor must be a positive number`,
      );
    }
  }
}

/**
 * BRAIN-320 S1b: the shape rules for a lane's `remoteDeps` -- non-empty
 * array of "." or a canonical relative path, no duplicates -- shared between
 * `.lane-broker.json` validation (below) and the runner's own re-validation
 * of a protocol-2 header's `remoteDeps` field (remote-runner.js), so the two
 * ends never drift apart on what counts as a valid value.
 */
export function isValidRemoteDepsShape(dirs) {
  if (!Array.isArray(dirs) || dirs.length === 0) return false;
  const seen = new Set();
  for (const dir of dirs) {
    if (dir !== '.' && !(typeof dir === 'string' && isCanonicalRelPath(dir))) return false;
    if (seen.has(dir)) return false;
    seen.add(dir);
  }
  return true;
}

/** Same sharing rationale as `isValidRemoteDepsShape` above, for `remoteSetup`. */
export function isValidRemoteSetupShape(setup) {
  if (!Array.isArray(setup) || setup.length === 0) return false;
  return setup.every((argv) => Array.isArray(argv) && argv.length > 0 && argv.every((a) => typeof a === 'string' && a.length > 0));
}

/** Sanity bound on a lane's declared `cpuCores`/`minCpuCores`: no machine has more, and an absurd claim is a typo. */
export const MAX_LANE_CPU_CORES = 1024;

/** BRAIN-398: a glob segment is plain text plus `*` (any run within the segment), or exactly `**` (any number of segments). */
function isValidArtifactSegment(seg) {
  return !seg.includes('**') || seg === '**';
}

/** BRAIN-398: the glob-free directory a pattern lives under (`out/*.json` -> `out`; `*.json` and `**\/x` -> ''). */
export function literalDirPrefix(pattern) {
  const segs = pattern.split('/').slice(0, -1);
  const end = segs.findIndex((seg) => seg.includes('*'));
  return (end === -1 ? segs : segs.slice(0, end)).join('/');
}

export const REMOTE_ARTIFACTS_ON = ['success', 'always'];
const REMOTE_POLICIES = ['remote-first', 'local-first'];
export const MAX_REMOTE_ARTIFACT_PATTERNS = 50;

/**
 * BRAIN-398: the shape rules for a lane's `remoteArtifacts` -- a non-empty array of canonical relative paths or simple
 * globs, no duplicates -- shared between `.lane-broker.json` validation and the runner's own re-validation of the
 * dispatch header, same as `isValidRemoteDepsShape`.
 */
export function isValidRemoteArtifactsShape(patterns) {
  if (!Array.isArray(patterns) || patterns.length === 0 || patterns.length > MAX_REMOTE_ARTIFACT_PATTERNS) return false;
  const seen = new Set();
  for (const pattern of patterns) {
    if (typeof pattern !== 'string' || !isCanonicalRelPath(pattern)) return false;
    if (!pattern.split('/').every(isValidArtifactSegment)) return false;
    if (literalDirPrefix(pattern) === '') return false;
    if (seen.has(pattern)) return false;
    seen.add(pattern);
  }
  return true;
}

/** BRAIN-379: a lane's allocation class; an undeclared class is 'test'. */
export const LANE_CLASSES = ['test', 'sim'];

/** BRAIN-379: the largest CPU claim a sim lane may resolve to (an unset cpuCores resolves to weight). */
export const MAX_SIM_CPU_CORES = 2;

function assertNoHookKeys(config, where) {
  const keys = unsupportedHookKeys(config);
  assert(keys.length === 0, hookRefusalMessage(where, keys));
}

function validateRepoConfig(cfg, sourcePath) {
  assert(cfg && typeof cfg === 'object', `${sourcePath}: config must be an object`);
  assert(Number.isInteger(cfg.version), `${sourcePath}: "version" must be an integer`);
  assert(cfg.lanes && typeof cfg.lanes === 'object' && !Array.isArray(cfg.lanes), `${sourcePath}: "lanes" must be an object`);
  assertNoHookKeys(cfg, sourcePath);
  for (const [name, lane] of Object.entries(cfg.lanes)) {
    assert(lane && typeof lane === 'object', `${sourcePath}: lane "${name}" must be an object`);
    assert(Number.isFinite(lane.weight) && lane.weight > 0, `${sourcePath}: lane "${name}".weight must be a positive number`);
    if (lane.cpuCores !== undefined) {
      assert(Number.isFinite(lane.cpuCores) && lane.cpuCores > 0, `${sourcePath}: lane "${name}".cpuCores must be a positive number`);
      assert(lane.cpuCores <= MAX_LANE_CPU_CORES, `${sourcePath}: lane "${name}".cpuCores must be <= ${MAX_LANE_CPU_CORES}`);
    }
    if (lane.minCpuCores !== undefined) {
      assert(Number.isFinite(lane.minCpuCores) && lane.minCpuCores > 0, `${sourcePath}: lane "${name}".minCpuCores must be a positive number`);
      // an unset cpuCores resolves to weight (resolveTicketResources), so that is the claim the floor sits under
      assert(lane.minCpuCores <= MAX_LANE_CPU_CORES, `${sourcePath}: lane "${name}".minCpuCores must be <= ${MAX_LANE_CPU_CORES}`);
      const claim = lane.cpuCores ?? lane.weight;
      assert(lane.minCpuCores <= claim, `${sourcePath}: lane "${name}".minCpuCores (${lane.minCpuCores}) must be <= its cpuCores (${claim})`);
    }
    if (lane.memoryBytes !== undefined) {
      assert(Number.isFinite(lane.memoryBytes) && lane.memoryBytes > 0, `${sourcePath}: lane "${name}".memoryBytes must be a positive number`);
    }
    if (lane.localRefused !== undefined) {
      assert(typeof lane.localRefused === 'boolean', `${sourcePath}: lane "${name}".localRefused must be a boolean`);
    }
    if (lane.nice !== undefined) {
      assert(
        Number.isInteger(lane.nice) && lane.nice >= 0 && lane.nice <= 19,
        `${sourcePath}: lane "${name}".nice must be an integer in [0, 19]`,
      );
    }
    if (lane.noProgressTimeoutMs !== undefined) {
      assert(
        Number.isInteger(lane.noProgressTimeoutMs) && lane.noProgressTimeoutMs >= 0,
        `${sourcePath}: lane "${name}".noProgressTimeoutMs must be a non-negative integer (0 disables)`,
      );
    }
    if (lane.class !== undefined) {
      assert(LANE_CLASSES.includes(lane.class), `${sourcePath}: lane "${name}".class must be "test" or "sim"`);
      if (lane.class === 'sim') {
        const simClaim = lane.cpuCores ?? lane.weight;
        assert(simClaim <= MAX_SIM_CPU_CORES, `${sourcePath}: lane "${name}" is class "sim", so its CPU claim (${simClaim}) must be <= ${MAX_SIM_CPU_CORES} cores`);
      }
    }
    if (lane.priority !== undefined) {
      assert(isPriorityTier(lane.priority), `${sourcePath}: lane "${name}".priority must be one of ${PRIORITY_TIERS.join(', ')}`);
    }
    if (lane.aging !== undefined) {
      assert(typeof lane.aging === 'boolean', `${sourcePath}: lane "${name}".aging must be a boolean`);
    }
    if (lane.exclusive !== undefined) {
      assert(typeof lane.exclusive === 'boolean', `${sourcePath}: lane "${name}".exclusive must be a boolean`);
    }
    // BRAIN-403: hooks are host-only and not supported yet; a lane carrying them is refused rather than silently ignored
    assertNoHookKeys(lane, `${sourcePath}: lane "${name}"`);
    if (lane.classEnforcement !== undefined) {
      assert(lane.classEnforcement === 'required', `${sourcePath}: lane "${name}".classEnforcement must be "required"`);
    }
    if (lane.maxConcurrent !== undefined) {
      assert(
        Number.isInteger(lane.maxConcurrent) && lane.maxConcurrent >= 1,
        `${sourcePath}: lane "${name}".maxConcurrent must be an integer >= 1`,
      );
    }
    if (lane.remote !== undefined) {
      assert(typeof lane.remote === 'boolean', `${sourcePath}: lane "${name}".remote must be a boolean`);
    }
    if (lane.remoteDeps !== undefined) {
      assert(
        isValidRemoteDepsShape(lane.remoteDeps),
        `${sourcePath}: lane "${name}".remoteDeps must be a non-empty array of "." or canonical relative paths, no duplicates`,
      );
    }
    if (lane.remoteSetup !== undefined) {
      assert(
        isValidRemoteSetupShape(lane.remoteSetup),
        `${sourcePath}: lane "${name}".remoteSetup must be a non-empty array of non-empty arrays of non-empty strings`,
      );
    }
    if (lane.remoteArtifacts !== undefined) {
      assert(
        isValidRemoteArtifactsShape(lane.remoteArtifacts),
        `${sourcePath}: lane "${name}".remoteArtifacts must be a non-empty array (at most ${MAX_REMOTE_ARTIFACT_PATTERNS}) of canonical relative paths or globs ("*" within a segment, "**" as a whole segment), each under a literal directory (so "**/*.json" and "*.json" are refused), no duplicates`,
      );
    }
    if (lane.remoteArtifactsOn !== undefined) {
      assert(REMOTE_ARTIFACTS_ON.includes(lane.remoteArtifactsOn), `${sourcePath}: lane "${name}".remoteArtifactsOn must be "success" or "always"`);
    }
    if (lane.remotePolicy !== undefined) {
      assert(REMOTE_POLICIES.includes(lane.remotePolicy), `${sourcePath}: lane "${name}".remotePolicy must be "remote-first" or "local-first"`);
    }
    if (lane.localFirstWaitMs !== undefined) {
      assert(Number.isInteger(lane.localFirstWaitMs) && lane.localFirstWaitMs >= 0, `${sourcePath}: lane "${name}".localFirstWaitMs must be a non-negative integer`);
    }
    for (const field of ['remoteDepsCache', 'remoteDepsCacheRootScriptsSafe', 'remoteOmitEscapingSymlinks']) {
      if (lane[field] !== undefined) assert(typeof lane[field] === 'boolean', `${sourcePath}: lane "${name}".${field} must be a boolean`);
    }
  }
  if (cfg.conflicts !== undefined) {
    assert(Array.isArray(cfg.conflicts), `${sourcePath}: "conflicts" must be an array of pairs`);
    for (const pair of cfg.conflicts) {
      assert(Array.isArray(pair) && pair.length === 2, `${sourcePath}: each "conflicts" entry must be a 2-element array`);
    }
  }
  if (cfg.undeclaredLanes !== undefined) {
    if (cfg.undeclaredLanes === 'allow' || cfg.undeclaredLanes === 'refuse') {
      // valid string forms
    } else if (cfg.undeclaredLanes && typeof cfg.undeclaredLanes === 'object' && !Array.isArray(cfg.undeclaredLanes)) {
      // BRAIN-325: {"as": "<declared lane>"} -- an undeclared lane is allowed
      // and inherits sizing/remote settings from the named declared lane. See
      // resolveTicketConfig's own doc comment for what is/isn't inherited.
      const keys = Object.keys(cfg.undeclaredLanes);
      assert(
        keys.length === 1 && keys[0] === 'as',
        `${sourcePath}: "undeclaredLanes" object must have exactly the key "as"`,
      );
      assert(
        typeof cfg.undeclaredLanes.as === 'string' && cfg.undeclaredLanes.as.length > 0,
        `${sourcePath}: "undeclaredLanes.as" must be a non-empty string`,
      );
      assert(
        Object.prototype.hasOwnProperty.call(cfg.lanes, cfg.undeclaredLanes.as),
        `${sourcePath}: "undeclaredLanes.as" must name a declared lane`,
      );
      assert(
        cfg.lanes[cfg.undeclaredLanes.as].localRefused !== true,
        `${sourcePath}: "undeclaredLanes.as" template lane "${cfg.undeclaredLanes.as}" must not be localRefused`,
      );
    } else {
      assert(
        false,
        `${sourcePath}: "undeclaredLanes" must be "allow" or "refuse" (or an object {"as": "<declared lane>"})`,
      );
    }
  }
}

/**
 * BRAIN-380 precedence: `lane run --priority`, then env `LANE_BROKER_PRIORITY`, then the lane's
 * `.lane-broker.json` tier (the `undeclaredLanes` template is already folded into `configTier`),
 * then medium. A value is validated only when it is the one that applies; an invalid one throws
 * ConfigError, which `lane run` maps to exit 64.
 */
export function resolvePriority({ cli, env, configTier, exclusive = false }) {
  const choose = (value, source) => {
    assert(isPriorityTier(value), `${source} must be one of ${PRIORITY_TIERS.join(', ')} (got "${value}")`);
    return value;
  };
  if (cli !== undefined) return choose(cli, '--priority');
  if (env !== undefined) return choose(env, 'LANE_BROKER_PRIORITY');
  // BRAIN-403: an exclusive defaults to high, still under cli > env > lane config
  return configTier ?? (exclusive ? 'high' : DEFAULT_PRIORITY);
}

export function loadGlobalConfig() {
  const home = brokerHome();
  const file = path.join(home, 'config.json');
  let text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch (err) {
    // BRAIN-321: only a genuinely ABSENT file is "no config". An inaccessible one (EACCES, a parent dir we cannot enter, ...) leaves the
    // class policy unknown (`ConfigError`), and a file that disappeared after a `classes` block was in force keeps the fence; either way sims are denied.
    if (err.code === 'ENOENT' || err.code === 'ENOTDIR') {
      return classesFenceHeld() ? { ...DEFAULT_GLOBAL_CONFIG, classesInvalid: `${file} is gone but a classes block was in force; restore it (or remove ${paths(stateHome()).classesConfigured})` } : { ...DEFAULT_GLOBAL_CONFIG };
    }
    // thrown, so `reloadGlobalConfig` keeps the PREVIOUS config's other settings (capacity, reserves, runners) and marks only the class policy invalid
    throw new ConfigError(`${file}: unreadable (${err.code ?? err.message})`);
  }
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new ConfigError(`${file}: invalid JSON (${err.message})`);
  }
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, ...parsed };
  // A partial `priorityWeights` overrides only the weights it names; the age cap follows the aging step unless set.
  if (parsed.priorityWeights && typeof parsed.priorityWeights === 'object' && !Array.isArray(parsed.priorityWeights)) {
    cfg.priorityWeights = { ...DEFAULT_GLOBAL_CONFIG.priorityWeights, ...parsed.priorityWeights };
  }
  if (parsed.priorityAgeMaxMs === undefined && Number.isInteger(cfg.priorityAgingMs)) {
    cfg.priorityAgeMaxMs = 2 * cfg.priorityAgingMs;
  }
  validateGlobalConfig(cfg, file);
  // BRAIN-321: a broken `classes` block must not take the whole config (and every lane) down with it, nor read as "no caps":
  // it is set aside and the sim class is ineligible until it is fixed (see resolveClasses).
  if (cfg.classes !== undefined) {
    try {
      validateClasses(cfg.classes, file);
    } catch (err) {
      if (!(err instanceof ClassesConfigError)) throw err;
      delete cfg.classes;
      cfg.classesInvalid = err.message;
    }
  }
  setClassesFence(cfg.classes !== undefined || cfg.classesInvalid !== undefined);
  return cfg;
}

const classesFenceFile = () => paths(stateHome()).classesConfigured;
const classesFenceHeld = () => fs.existsSync(classesFenceFile());

/** Durably record (in the broker's state dir) whether a config with a `classes` block has been loaded; written only on change. */
function setClassesFence(inForce) {
  if (inForce === classesFenceHeld()) return;
  try {
    if (inForce) {
      fs.mkdirSync(path.dirname(classesFenceFile()), { recursive: true });
      fs.writeFileSync(classesFenceFile(), `${new Date().toISOString()}\n`);
    } else fs.rmSync(classesFenceFile(), { force: true });
  } catch {
    // an unwritable state dir cannot hold a fence; admission fails there on its own
  }
}

/**
 * Re-read the global config for a long-running process (the supervisor's
 * polling loop). A supervisor holding a stale snapshot forever is the bug
 * this exists to fix: it must notice a threshold change within one
 * `sampleMs`, not never. But it must also never crash, or silently change
 * behaviour, because someone saved a broken config while it's mid-run — an
 * operator mid-edit, a half-written file, or a typo should degrade to "keep
 * doing what you were doing" rather than take down every running lane. So
 * any read/parse/validation failure here falls back to `previous` (or
 * `DEFAULT_GLOBAL_CONFIG` if there is no previous yet) instead of throwing.
 */
export function reloadGlobalConfig(previous, { onError } = {}) {
  const fallback = previous === undefined ? { ...DEFAULT_GLOBAL_CONFIG } : previous;
  try {
    return loadGlobalConfig();
  } catch (err) {
    if (onError) onError(err);
    // BRAIN-321: an unreadable or invalid config leaves the class policy UNKNOWN, whether or not a block was ever loaded: a fresh
    // supervisor must not read it as "off" and admit sims, nor keep old caps. The fallback keeps everything else but marks the
    // sim class ineligible until a good reload.
    return { ...fallback, classesInvalid: err.message };
  }
}

/**
 * BRAIN-403: `lane run` refuses a host config that carries the unsupported exclusive-hook keys. This reads the file
 * itself rather than going through `reloadGlobalConfig`, whose fallback turns every validation failure into the previous
 * or default configuration and so would swallow the refusal. A missing or unparseable file is not this check's concern.
 */
export function assertHostConfigHookless() {
  const file = path.join(brokerHome(), 'config.json');
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return;
  }
  const keys = unsupportedHookKeys(parsed);
  if (keys.length > 0) throw new ConfigError(hookRefusalMessage(file, keys));
}

const repoIdentityCache = new Map();

/** Resolve `<gitFileDir>/gitdir: <path>` style pointer contents to an absolute path. */
function resolveGitFilePointer(gitFilePath, gitFileDir) {
  const contents = fs.readFileSync(gitFilePath, 'utf8');
  const match = contents.match(/^gitdir:\s*(.+)$/m);
  if (!match) return null;
  const raw = match[1].trim();
  return path.isAbsolute(raw) ? raw : path.resolve(gitFileDir, raw);
}

/** Walk the filesystem up from `cwd` looking for a `.git` dir or file, no subprocess. */
function repoIdentityFromFilesystem(cwd) {
  let dir = path.resolve(cwd);
  for (;;) {
    const gitPath = path.join(dir, '.git');
    let stat;
    try {
      stat = fs.statSync(gitPath);
    } catch {
      stat = null;
    }
    if (stat) {
      let commonDir;
      if (stat.isDirectory()) {
        commonDir = gitPath;
      } else {
        const gitDir = resolveGitFilePointer(gitPath, dir);
        if (!gitDir) return null;
        commonDir = gitDir;
        try {
          const commondirContents = fs.readFileSync(path.join(gitDir, 'commondir'), 'utf8').trim();
          if (commondirContents) {
            commonDir = path.isAbsolute(commondirContents)
              ? commondirContents
              : path.resolve(gitDir, commondirContents);
          }
        } catch {
          // no commondir file; a linked-worktree gitdir without one is its own common dir.
        }
      }
      try {
        return fs.realpathSync(commonDir);
      } catch {
        return commonDir;
      }
    }
    const parent = path.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}

/** Resolve the repo identity via `git rev-parse`, only as a fallback for the filesystem walk. */
function repoIdentityFromGit(cwd) {
  let commonDir;
  try {
    commonDir = execFileSync('git', ['rev-parse', '--path-format=absolute', '--git-common-dir'], {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5000,
    }).trim();
  } catch {
    try {
      const rel = execFileSync('git', ['rev-parse', '--git-common-dir'], {
        cwd,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: 5000,
      }).trim();
      commonDir = path.isAbsolute(rel) ? rel : path.join(cwd, rel);
    } catch {
      return null;
    }
  }
  try {
    return fs.realpathSync(commonDir);
  } catch {
    return commonDir;
  }
}

/** Resolve the repo identity as the realpath of the git common dir (shared by worktrees). */
export function repoIdentity(cwd) {
  const resolvedCwd = path.resolve(cwd);
  if (repoIdentityCache.has(resolvedCwd)) return repoIdentityCache.get(resolvedCwd);
  const result = repoIdentityFromFilesystem(resolvedCwd) ?? repoIdentityFromGit(resolvedCwd);
  repoIdentityCache.set(resolvedCwd, result);
  return result;
}

/**
 * Walk up from `cwd` looking for .lane-broker.json, stopping at `configRoot`
 * when given (BRAIN-319: a `lane remote-exec` snapshot work dir has no
 * `.git`, so the git-common-dir-derived stop point below is unavailable —
 * without an explicit floor the walk would continue past the snapshot root
 * into whatever happens to sit above it on the runner's filesystem), or
 * otherwise at the git common dir's worktree root, same as before.
 */
export function findRepoConfigPath(cwd, gitCommonDir, configRoot) {
  const stopAt = configRoot ? path.resolve(configRoot) : gitCommonDir ? path.dirname(gitCommonDir) : null;
  let dir = path.resolve(cwd);
  for (;;) {
    const candidate = path.join(dir, '.lane-broker.json');
    if (fs.existsSync(candidate)) return candidate;
    if (stopAt && dir === stopAt) return null;
    const parent = path.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}

export function loadRepoConfig(cwd, configRoot) {
  const commonDir = repoIdentity(cwd);
  const configPath = findRepoConfigPath(cwd, commonDir, configRoot);
  if (!configPath) return { ...DEFAULT_REPO_CONFIG, fileLanes: DEFAULT_REPO_CONFIG.lanes, declared: false };
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  } catch (err) {
    throw new ConfigError(`${configPath}: invalid JSON (${err.message})`);
  }
  const cfg = {
    version: 1,
    conflicts: [],
    undeclaredLanes: 'refuse',
    ...parsed,
    lanes: { ...DEFAULT_REPO_CONFIG.lanes, ...(parsed.lanes || {}) },
  };
  validateRepoConfig(cfg, configPath);
  // `lanes` has the built-in `default` merged in; `fileLanes` is only what the file itself declared (BRAIN-448).
  return { ...cfg, fileLanes: parsed.lanes || {}, declared: true };
}

/** Expand conflict pairs (with `*` wildcard) into a symmetric adjacency map of laneName -> Set<laneName>. */
export function expandConflicts(conflicts, laneNames) {
  const adj = new Map(laneNames.map((n) => [n, new Set()]));
  for (const [a, b] of conflicts) {
    const as = a === '*' ? laneNames : [a];
    const bs = b === '*' ? laneNames : [b];
    for (const x of as) {
      for (const y of bs) {
        if (x === y) continue;
        if (!adj.has(x)) adj.set(x, new Set());
        if (!adj.has(y)) adj.set(y, new Set());
        adj.get(x).add(y);
        adj.get(y).add(x);
      }
    }
  }
  return adj;
}

/**
 * Resolve everything a `lane run` invocation needs: the fully-qualified key,
 * weight, whether this is a refused local sim, and the set of conflicting
 * fully-qualified keys within this repo.
 */
export function resolveTicketConfig({ cwd, repo, lane, configRoot, repoIdentityOverride }) {
  const repoConfig = loadRepoConfig(cwd, configRoot);
  const commonDir = repoIdentity(cwd);
  // The git identity wins whenever it can be determined, so the same repo
  // always resolves to the same key regardless of whether --repo is passed
  // (or passed inconsistently across invocations); --repo is a fallback
  // label for cwds with no git identity to key on.
  //
  // BRAIN-319 T3 (C7): `repoIdentityOverride` pins the key to a caller-given
  // identity unconditionally, bypassing git entirely -- needed once
  // `lane remote-exec`'s snapshot work dir gets its OWN synthetic `.git`
  // (so real consumers can run plain git commands against it): without
  // this, `repoIdentity(cwd)` would find that synthetic repo and derive a
  // key unique to THIS ticket's throwaway `.git`, breaking the invariant
  // that the same repo+lane always maps to the same lease key.
  const repoId = repoIdentityOverride ? sanitizeKey(repoIdentityOverride) : sanitizeKey(commonDir || repo || cwd);
  const laneName = lane || 'default';
  const hasLane = (lanes) => Object.prototype.hasOwnProperty.call(lanes, laneName);
  // BRAIN-325: {"as": "<declared lane>"} is an allow form that additionally
  // names a template lane to inherit sizing/remote settings from.
  const undeclaredTemplateName =
    repoConfig.undeclaredLanes && typeof repoConfig.undeclaredLanes === 'object'
      ? repoConfig.undeclaredLanes.as
      : null;
  const undeclaredAllowed = repoConfig.undeclaredLanes === 'allow' || undeclaredTemplateName !== null;
  // BRAIN-448: the built-in `default` counts as declared unless the file has a (different) template to resolve it through.
  const isDeclaredLane =
    hasLane(repoConfig.fileLanes) ||
    (hasLane(repoConfig.lanes) && (undeclaredTemplateName === null || undeclaredTemplateName === laneName));
  if (repoConfig.declared && !isDeclaredLane && !undeclaredAllowed) {
    const declared = Object.keys(repoConfig.lanes).sort().join(', ');
    throw new ConfigError(`unknown lane "${laneName}"; declared: ${declared}`);
  }
  let laneCfg;
  if (isDeclaredLane) {
    laneCfg = repoConfig.lanes[laneName];
  } else if (undeclaredTemplateName) {
    // Inherit weight/cpuCores/minCpuCores/memoryBytes/nice/noProgressTimeoutMs/remote/remoteDeps/remoteSetup/remoteDepsCache/remoteDepsCacheRootScriptsSafe/remoteOmitEscapingSymlinks/remoteArtifacts/remoteArtifactsOn/class/priority/aging
    // from the named declared lane, keeping this lane's OWN key/name. Not
    // inherited: `localRefused` (stays default false), the template's named
    // conflicts (only the `*` wildcard universe below reaches this lane, same
    // as any other undeclared-but-allowed lane), and `maxConcurrent` (stays
    // the default of 1, below).
    const templateCfg = repoConfig.lanes[undeclaredTemplateName];
    laneCfg = {
      weight: templateCfg.weight,
      cpuCores: templateCfg.cpuCores,
      minCpuCores: templateCfg.minCpuCores,
      memoryBytes: templateCfg.memoryBytes,
      nice: templateCfg.nice,
      noProgressTimeoutMs: templateCfg.noProgressTimeoutMs,
      remote: templateCfg.remote,
      remoteDeps: templateCfg.remoteDeps,
      remoteSetup: templateCfg.remoteSetup,
      remoteDepsCache: templateCfg.remoteDepsCache,
      remotePolicy: templateCfg.remotePolicy,
      localFirstWaitMs: templateCfg.localFirstWaitMs,
      remoteDepsCacheRootScriptsSafe: templateCfg.remoteDepsCacheRootScriptsSafe,
      remoteOmitEscapingSymlinks: templateCfg.remoteOmitEscapingSymlinks,
      remoteArtifacts: templateCfg.remoteArtifacts,
      remoteArtifactsOn: templateCfg.remoteArtifactsOn,
      class: templateCfg.class,
      priority: templateCfg.priority,
      aging: templateCfg.aging,
      classEnforcement: templateCfg.classEnforcement,
    };
  } else {
    laneCfg = { weight: DEFAULT_REPO_CONFIG.lanes.default.weight };
  }
  const declaredLaneNames = Object.keys(repoConfig.lanes);
  // BRAIN-319 (undeclaredLanes: "allow"): an allowed undeclared lane resolves
  // like the no-config-file case in every other respect, but a declared
  // lane's `["*", "other"]` conflict must still reach it -- so it joins the
  // conflict-expansion universe (never `repoConfig.lanes` itself, which
  // stays declared-only) purely so `expandConflicts`'s `*` wildcard sees it.
  const laneNames = isDeclaredLane ? declaredLaneNames : [...declaredLaneNames, laneName];
  const adj = expandConflicts(repoConfig.conflicts || [], laneNames);
  const conflictingLaneNames = adj.has(laneName) ? [...adj.get(laneName)] : [];
  const key = `${repoId}:${sanitizeKey(laneName)}`;
  const conflicts = conflictingLaneNames.map((n) => `${repoId}:${sanitizeKey(n)}`);
  return {
    repoId,
    lane: laneName,
    // BRAIN-361: the declared lane an ad-hoc name inherited from; present only when it differs from `lane`
    ...(!isDeclaredLane && undeclaredTemplateName ? { configLane: undeclaredTemplateName } : {}),
    key,
    weight: laneCfg.weight,
    cpuCores: Number.isFinite(laneCfg.cpuCores) ? laneCfg.cpuCores : null,
    minCpuCores: Number.isFinite(laneCfg.minCpuCores) ? laneCfg.minCpuCores : null,
    memoryBytes: Number.isFinite(laneCfg.memoryBytes) ? laneCfg.memoryBytes : null,
    localRefused: Boolean(laneCfg.localRefused),
    // null (not defaulted here) when the lane doesn't declare its own nice:
    // the caller (run.js) applies the global `laneNice` fallback, the same
    // "per-lane overrides global" shape as everything else in this file.
    nice: Number.isInteger(laneCfg.nice) ? laneCfg.nice : null,
    // BRAIN-431: null falls through to the global `noProgressTimeoutMs`; 0 disables the watchdog for this lane.
    noProgressTimeoutMs: Number.isInteger(laneCfg.noProgressTimeoutMs) ? laneCfg.noProgressTimeoutMs : null,
    // BRAIN-255: how many same-key holders may run at once. Defaulted HERE
    // (not left null for a caller fallback like `nice` above) because 1 is
    // the load-bearing compatibility default for every lane that predates
    // this field, not an operator-configurable global — there is no
    // equivalent of `laneNice` to fall back to.
    maxConcurrent: Number.isInteger(laneCfg.maxConcurrent) ? laneCfg.maxConcurrent : 1,
    // BRAIN-379: allocation class; defaulted here so every lane predating the field is a 'test'.
    class: laneCfg.class === 'sim' ? 'sim' : 'test',
    // BRAIN-380: the lane's declared tier (validated above); null lets the caller fall through to the default.
    priority: isPriorityTier(laneCfg.priority) ? laneCfg.priority : null,
    // ROG-2181: false stops this lane's tickets accruing priority age; every lane predating the field ages.
    aging: laneCfg.aging !== false,
    // BRAIN-403: never inherited through an undeclaredLanes template (the template literal above does not copy it)
    exclusive: laneCfg.exclusive === true,
    // BRAIN-321: "required" = granted only under live class caps (`classes.mode` active and valid); absent for every other lane
    ...(laneCfg.classEnforcement === 'required' ? { classEnforcement: 'required' } : {}),
    conflicts,
    // BRAIN-319 T3a: opt-in per lane, defaulted false so a repo config
    // written before this field exists resolves identically (I6).
    remote: laneCfg.remote === true,
    // BRAIN-442: "local-first" queues locally and goes remote only after `localFirstWaitMs`; anything else is today's remote-first.
    remotePolicy: laneCfg.remotePolicy === 'local-first' ? 'local-first' : 'remote-first',
    localFirstWaitMs: Number.isInteger(laneCfg.localFirstWaitMs) ? laneCfg.localFirstWaitMs : null,
    // BRAIN-320 S1a: honoured only when this lane runs remotely (1a); a lane
    // with neither declared behaves exactly as in 0.6.0 (I6), including the
    // protocol it speaks (1d).
    remoteDeps: Array.isArray(laneCfg.remoteDeps) ? laneCfg.remoteDeps : null,
    remoteSetup: Array.isArray(laneCfg.remoteSetup) ? laneCfg.remoteSetup : null,
    // BRAIN-389: false opts this lane out of the runner's installed-deps cache; anything else leaves it on.
    remoteDepsCache: laneCfg.remoteDepsCache !== false,
    // BRAIN-389: true declares this lane's root install/prepare scripts leave node_modules alone, so a cached tree is safe.
    remoteDepsCacheRootScriptsSafe: laneCfg.remoteDepsCacheRootScriptsSafe === true,
    // BRAIN-334: true leaves a tracked symlink that points outside the repo out of the remote snapshot instead of making the lane ineligible.
    remoteOmitEscapingSymlinks: laneCfg.remoteOmitEscapingSymlinks === true,
    // BRAIN-398: files a remote run returns to the submitter's worktree; ignored by a local run (they are already there).
    remoteArtifacts: Array.isArray(laneCfg.remoteArtifacts) ? laneCfg.remoteArtifacts : null,
    remoteArtifactsOn: laneCfg.remoteArtifactsOn === 'always' ? 'always' : 'success',
  };
}
