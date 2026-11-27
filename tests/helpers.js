import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeTmpDir } from './helpers/tmp.js';

export const BIN = fileURLToPath(new URL('../bin/lane.js', import.meta.url));

let counter = 0;

/** Fresh, isolated $LANE_BROKER_HOME / $LANE_BROKER_STATE for one test. */
// Ambient LANE_BROKER_* vars a test must never inherit. The suite itself runs INSIDE a lane --
// the committed pre-push hook invokes `lane run ... npm test` -- so the broker exports its own
// lease/ticket/key into this process, and `...process.env` handed them straight to every `lane
// run` a test spawns. A child seeing LANE_BROKER_LEASE takes the reentrancy path (reuse the
// inherited lease) instead of enqueueing, so an assertion like "no queue state is created" fails
// under the gate while passing standalone -- a red gate nobody can push past, on this repo of all
// of them. Same class of bug, and the same fix, as the git-env scrub the pre-push hook itself
// documents at the top of .githooks/pre-push.
//
// HOME and STATE are absent here on purpose: freshEnv overwrites both explicitly below.
export const INHERITED_BROKER_VARS = [
  'LANE_BROKER_LEASE',
  'LANE_BROKER_TICKET',
  'LANE_BROKER_KEY',
  'LANE_BROKER_BOOT_ID',
  'LANE_BROKER_CPU_BUSY_FILE',
  'LANE_BROKER_LOADAVG_FILE',
  'LANE_BROKER_MEMORY_FILE',
  'LANE_BROKER_LOCAL',
  'LANE_BROKER_LOCAL_REASON',
  'LANE_BROKER_EXECUTOR',
  'LANE_BROKER_RUNNER',
  'LANE_BROKER_RUNNER_SPEED_FACTOR',
  'LANE_BROKER_PRIORITY',
  'LANE_BROKER_NO_CACHE',
  'LANE_BROKER_RETRY_REASON',
  'LANE_BROKER_CONFIG_FROM',
];

/** `process.env` with every ambient broker variable removed, so a test's environment is decided by
 *  the test and never by whatever invoked the suite. */
function scrubbedProcessEnv() {
  const env = { ...process.env };
  for (const key of INHERITED_BROKER_VARS) delete env[key];
  return env;
}

export function freshEnv(extra = {}) {
  counter += 1;
  const base = makeTmpDir(`lane-broker-test-${counter}-`);
  const home = path.join(base, 'home');
  const state = path.join(base, 'state');
  fs.mkdirSync(home, { recursive: true });
  fs.mkdirSync(state, { recursive: true });
  return {
    base,
    home,
    state,
    env: {
      ...scrubbedProcessEnv(),
      LANE_BROKER_HOME: home,
      LANE_BROKER_STATE: state,
      ...extra,
    },
  };
}

export function writeGlobalConfig(home, cfg) {
  // Most integration tests predate resource admission and exercise another
  // scheduler dimension. Keep those fixtures independent of ambient host load;
  // active-admission tests opt in explicitly. cpuLimit 'off': no systemd-run scope racing nice checks.
  const deterministicCfg = { schedulerMode: 'shadow', cpuLimit: 'off', ...cfg };
  fs.writeFileSync(path.join(home, 'config.json'), JSON.stringify(deterministicCfg, null, 2));
}

export function writeRepoConfig(dir, cfg) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, '.lane-broker.json'), JSON.stringify(cfg, null, 2));
}

export function writeLoadFile(base, value) {
  const file = path.join(base, 'load');
  fs.writeFileSync(file, String(value));
  return file;
}

/** CPU-gate equivalent of writeLoadFile, for LANE_BROKER_CPU_BUSY_FILE. */
export function writeCpuBusyFile(base, hostBusyCores, cores) {
  const file = path.join(base, 'cpu-busy');
  fs.writeFileSync(file, `${hostBusyCores},${cores}`);
  return file;
}

/**
 * Memory-telemetry equivalent of writeLoadFile, for LANE_BROKER_MEMORY_FILE.
 * `availableBytes`/`totalMemoryBytes` (BRAIN-273) are optional -- omitting
 * them exercises the "no availability signal" -> unavailable path.
 */
export function writeMemoryFile(base, swapUsedBytes, swapTotalBytes, compressorBytes, availableBytes, totalMemoryBytes) {
  const file = path.join(base, 'memory');
  const fields = [swapUsedBytes, swapTotalBytes, compressorBytes];
  if (availableBytes !== undefined || totalMemoryBytes !== undefined) {
    fields.push(availableBytes, totalMemoryBytes);
  }
  fs.writeFileSync(file, fields.join(','));
  return file;
}

/** Spawn `lane <args>` and resolve with { code, stdout, stderr } once stdio is
 *  fully drained. Waits for 'close' rather than 'exit': 'exit' can fire
 *  before a piped chunk still in flight is delivered (BRAIN-308's foreground
 *  streaming made this observable -- a large forwarded stdout could resolve
 *  before its tail arrived), so an assertion on captured output must wait
 *  for 'close' the same way the supervisor's own child listener does. */
export function laneRun(args, { env, cwd, bin = BIN } = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [bin, ...args], { env, cwd: cwd || process.cwd() });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      stdout += d;
    });
    child.stderr.on('data', (d) => {
      stderr += d;
    });
    child.on('close', (code) => resolve({ code, stdout, stderr }));
  });
}

/** Spawn `lane <args>` without waiting — caller controls lifecycle (signals, timing). */
export function laneSpawn(args, { env, cwd, bin = BIN } = {}) {
  return spawn(process.execPath, [bin, ...args], { env, cwd: cwd || process.cwd() });
}

/** The pids a test's broker state names: supervisors (leased or still queued) and the leased commands' process groups. */
async function brokerProcesses(state) {
  const { paths, readJsonSafe } = await import('../src/state.js');
  const records = (dir) => {
    try {
      return fs.readdirSync(dir).filter((n) => n.endsWith('.json')).map((n) => readJsonSafe(path.join(dir, n))).filter(Boolean);
    } catch {
      return [];
    }
  };
  const { leases, queue } = paths(state);
  return {
    supervisors: [...records(leases), ...records(queue)].map((r) => r.supervisorPid).filter(Number.isInteger),
    groups: records(leases).map((r) => r.childPgid).filter(Number.isInteger),
  };
}

/**
 * Release a `lane run` held by a release file and wait, bounded, for it and the broker processes `state` names (its detached supervisor,
 * its command's group) to be gone. A deadline escalates to SIGTERM then SIGKILL on all of them; anything still alive after the last deadline throws.
 * Resolves `{ code, signal, escalated }` so a success path can assert the exit it released into.
 */
export async function releaseAndReap(child, releaseFile, { state, graceMs = scaledMs(15_000), killMs = scaledMs(5_000) } = {}) {
  const { isPidAlive, isGroupAlive } = await import('../src/lease.js');
  fs.writeFileSync(releaseFile, '');
  const childGone = () => child.exitCode !== null || child.signalCode !== null;
  // a lease vanishes when its supervisor does, so remember every pid seen: an orphaned command group must still be waited for and killed
  const known = { supervisors: new Set(), groups: new Set() };
  const remember = async () => {
    const seen = await brokerProcesses(state);
    seen.supervisors.forEach((pid) => known.supervisors.add(pid));
    seen.groups.forEach((pgid) => known.groups.add(pgid));
    return { supervisors: [...known.supervisors], groups: [...known.groups] };
  };
  const alive = async () => {
    const { supervisors, groups } = await remember();
    return [...(childGone() ? [] : [`cli ${child.pid}`]), ...supervisors.filter(isPidAlive).map((pid) => `supervisor ${pid}`), ...groups.filter(isGroupAlive).map((pgid) => `group ${pgid}`)];
  };
  const kill = async (signal) => {
    const { supervisors, groups } = await remember();
    if (!childGone()) child.kill(signal);
    for (const pid of supervisors) try { process.kill(pid, signal); } catch { /* gone */ }
    for (const pgid of groups) try { process.kill(-pgid, signal); } catch { /* gone */ }
  };
  const settle = async (ms) => {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      if ((await alive()).length === 0) return true;
      await sleep(50);
    }
    return (await alive()).length === 0;
  };
  let escalated = false;
  for (const [signal, ms] of [[null, graceMs], ['SIGTERM', killMs], ['SIGKILL', killMs]]) {
    if (signal) {
      escalated = true;
      await kill(signal);
    }
    if (await settle(ms)) return { code: child.exitCode, signal: child.signalCode, escalated };
  }
  throw new Error(`teardown left processes running: ${(await alive()).join(', ')}`);
}

let localEnvVarNames = null;

/**
 * Names of git's repository-local env vars (GIT_DIR, GIT_INDEX_FILE, ...), per
 * `git rev-parse --local-env-vars`. A hook invocation leaks these into every
 * child process it spawns; a fixture that shells out to git while one of them
 * is set operates on whatever repo they point at instead of its own temp dir.
 * That happened for real: a pre-push run leaked GIT_DIR/GIT_WORK_TREE into
 * `npm test`, and this suite's own git fixtures then mutated the live
 * lane-broker repo (set core.bare=true, added a stray worktree, committed a
 * tree deletion) instead of the throwaway directories they were given.
 */
function localEnvVarList() {
  if (!localEnvVarNames) {
    localEnvVarNames = execFileSync('git', ['rev-parse', '--local-env-vars'], {
      encoding: 'utf8',
    })
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return localEnvVarNames;
}

/**
 * Run a git fixture command with a sanitized environment and an explicit,
 * hermetic identity: no repository-local env leaked in from a caller (e.g. a
 * pre-push hook), and no dependence on the invoking user's git config.
 */
export function gitFixture(args, cwd) {
  if (!cwd) throw new Error('gitFixture requires an explicit cwd');
  const env = { ...process.env };
  for (const name of [...localEnvVarList(), 'GIT_QUARANTINE_PATH']) {
    delete env[name];
  }
  return execFileSync(
    'git',
    [
      '-c', 'user.name=lane-broker-test',
      '-c', 'user.email=test@example.invalid',
      '-c', 'commit.gpgsign=false',
      '-c', 'init.defaultBranch=main',
      ...args,
    ],
    { cwd, env, encoding: 'utf8' },
  );
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Budget multiplier: LANE_BROKER_TEST_TIME_SCALE over LANE_BROKER_RUNNER_SPEED_FACTOR (higher = faster, valid in (0,4]). A fast runner never shrinks a budget. */
export function timeScale(env = process.env) {
  const speed = Number(env.LANE_BROKER_RUNNER_SPEED_FACTOR);
  const override = Number(env.LANE_BROKER_TEST_TIME_SCALE);
  const speedDivisor = Number.isFinite(speed) && speed > 0 ? Math.min(speed, 1) : 1;
  const multiplier = Number.isFinite(override) && override > 0 ? override : 1;
  return multiplier / speedDivisor;
}

export function scaledMs(budgetMs, env = process.env) {
  return Math.ceil(budgetMs * timeScale(env));
}

/** A loaded runner can stall the probe subprocess past production's 10 s and skip the runner as "probe timed out". */
export const GENEROUS_PROBE_DEADLINE_MS = scaledMs(60_000);

export async function waitFor(predicate, { timeoutMs = 30_000, intervalMs = 20 } = {}) {
  const deadline = Date.now() + scaledMs(timeoutMs);
  for (;;) {
    const v = await predicate();
    if (v) return v;
    if (Date.now() > deadline) throw new Error('waitFor: timed out');
    await sleep(intervalMs);
  }
}

/** The admission log prefixes every line with an ISO timestamp (BRAIN-438); tests that match on a line's start read it without. */
export function stripLogTimestamps(text) {
  return text.replace(/^\d{4}-\d\d-\d\dT[\d:.]+Z /gm, '');
}
