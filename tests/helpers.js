import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

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
const INHERITED_BROKER_VARS = [
  'LANE_BROKER_LEASE',
  'LANE_BROKER_TICKET',
  'LANE_BROKER_KEY',
  'LANE_BROKER_BOOT_ID',
  'LANE_BROKER_CPU_BUSY_FILE',
  'LANE_BROKER_LOADAVG_FILE',
  'LANE_BROKER_MEMORY_FILE',
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
  const base = fs.mkdtempSync(path.join(os.tmpdir(), `lane-broker-test-${counter}-`));
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
  // active-admission tests opt in explicitly.
  const deterministicCfg = { schedulerMode: 'shadow', ...cfg };
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

/** Memory-telemetry equivalent of writeLoadFile, for LANE_BROKER_MEMORY_FILE. */
export function writeMemoryFile(base, swapUsedBytes, swapTotalBytes, compressorBytes) {
  const file = path.join(base, 'memory');
  fs.writeFileSync(file, `${swapUsedBytes},${swapTotalBytes},${compressorBytes}`);
  return file;
}

/** Spawn `lane <args>` and resolve with { code, stdout, stderr } on exit. */
export function laneRun(args, { env, cwd } = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [BIN, ...args], { env, cwd: cwd || process.cwd() });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      stdout += d;
    });
    child.stderr.on('data', (d) => {
      stderr += d;
    });
    child.on('exit', (code) => resolve({ code, stdout, stderr }));
  });
}

/** Spawn `lane <args>` without waiting — caller controls lifecycle (signals, timing). */
export function laneSpawn(args, { env, cwd } = {}) {
  return spawn(process.execPath, [BIN, ...args], { env, cwd: cwd || process.cwd() });
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

export async function waitFor(predicate, { timeoutMs = 5000, intervalMs = 20 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await predicate();
    if (v) return v;
    if (Date.now() > deadline) throw new Error('waitFor: timed out');
    await sleep(intervalMs);
  }
}
