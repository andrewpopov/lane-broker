import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

/**
 * BRAIN-379 slice 2: scripted admission scenarios, driven against ANY copy of the broker's modules. The
 * branch's tests run them on the branch (shadow on and off); the golden trace was produced by running the
 * very same scripts on main's code, so "shadow on reproduces main" is checked against main, not against
 * itself. Regenerate the committed trace (read-only git; the checkout is a throwaway):
 *
 *   rm -rf /tmp/lb-main && mkdir /tmp/lb-main && git archive origin/main | tar -x -C /tmp/lb-main
 *   node tests/allocation-scenarios.js /tmp/lb-main > tests/fixtures/allocation-golden-trace.json
 *
 * (run with cwd = a checkout of this branch: lease records carry the cwd). Needs a main that has none of this
 * slice; once main carries it, the trace is the new baseline and only a deliberate live change may alter it.
 */

const GIB = 1024 ** 3;

export async function loadModules(srcDir) {
  const load = (f) => import(pathToFileURL(path.join(srcDir, f)).href);
  const [scheduler, lease, state, config] = await Promise.all([load('scheduler.js'), load('lease.js'), load('state.js'), load('config.js')]);
  return {
    enqueue: scheduler.enqueue,
    tryStart: scheduler.tryStart,
    writeLease: lease.writeLease,
    LEASE_STATE: lease.LEASE_STATE,
    paths: state.paths,
    bootId: state.bootId,
    DEFAULT_GLOBAL_CONFIG: config.DEFAULT_GLOBAL_CONFIG,
  };
}

export function baseCfg(m, overrides = {}) {
  return {
    ...m.DEFAULT_GLOBAL_CONFIG,
    schedulerMode: 'active',
    capacity: 10,
    loadClose: 1000,
    loadOpen: 900,
    loadOpenSamples: 1,
    cpuAdmissionPercent: 100,
    cpuReserveCores: 1,
    admissionCooldownMs: 0,
    resourceSkipLimit: 3,
    resourceIdleOvershootCores: 0,
    ...overrides,
  };
}

export const sampler = (hostBusyCores) => () => ({ hostBusyCores, cores: 10, stale: false, sampledAt: Date.now() });
export const memory = () => ({ availableBytes: 64 * GIB, totalBytes: 64 * GIB, macPressure: 'normal', source: 'test' });

export const ticket = (id, overrides = {}) => ({
  id,
  key: `r:${id}`,
  weight: 1,
  cwd: process.cwd(),
  cmd: ['true'],
  supervisorPid: process.pid,
  supervisorStart: null,
  logPath: '/dev/null',
  resultPath: '/dev/null',
  ...overrides,
});
export const sim = (id, weight = 1, extra = {}) => ticket(id, { class: 'sim', weight, ...extra });

export const heldLease = (m, id, key, weight, extra = {}) => ({
  id,
  key,
  bootId: m.bootId(),
  supervisorPid: process.pid,
  supervisorStart: null,
  childPgid: null,
  heartbeatAt: Date.now(),
  admittedAt: Date.now(),
  weight,
  state: m.LEASE_STATE.RUNNING,
  ...extra,
});

export function scenarios(m) {
  const head = ticket('head', { weight: 4 });
  const chead = ticket('chead', { key: 'r:lock', conflicts: [] });
  const khead = ticket('khead', { weight: 6 });
  return {
    // A resource-denied head backfilled past (resource skip counter + reservation latch), mixed classes.
    resource: [
      { enqueue: head },
      { enqueue: sim('s1') },
      { enqueue: ticket('t1') },
      { enqueue: sim('s2', 2) },
      { enqueue: ticket('t2', { weight: 2 }) },
      { poll: sim('s1'), ext: 5.4 },
      { poll: head, ext: 5.4 },
      { poll: sim('s1'), ext: 5.4 },
      { poll: ticket('t1'), ext: 5.4 },
      { poll: head, ext: 5.4 },
      { poll: sim('s2', 2), ext: 5.4 },
      { poll: ticket('t2', { weight: 2 }), ext: 5.4 },
      { poll: head, ext: 5.4 },
      { release: 's1' },
      { release: 't1' },
      { poll: head, ext: 0 },
    ],
    // A conflict-blocked head that is skipped (conflict skip counter) until its allowance is exhausted, with a sim queued.
    conflict: [
      { lease: heldLease(m, 'holder', 'r:lock', 1) },
      { enqueue: chead },
      { enqueue: sim('cs1') },
      { enqueue: ticket('ct1') },
      { enqueue: sim('cs2') },
      { enqueue: ticket('ct2') },
      { poll: chead },
      { poll: sim('cs1') },
      { poll: ticket('ct1') },
      { poll: sim('cs2') },
      { poll: ticket('ct2') },
      { poll: chead },
    ],
    // A head that does not fit weight capacity (capacity skip counter), tests holding most of B so sim locks bite.
    capacity: [
      { lease: heldLease(m, 'big', 'r:big', 5, { resources: { cpuCores: 5, memoryBytes: GIB } }) },
      { enqueue: khead },
      { enqueue: sim('ks1') },
      { enqueue: ticket('kt1', { weight: 2 }) },
      { poll: khead },
      { poll: sim('ks1') },
      { poll: ticket('kt1', { weight: 2 }) },
      { poll: khead },
    ],
  };
}

// pids and boot ids differ between the run that made a trace and any later run; nothing else may
const VOLATILE_KEYS = new Set(['supervisorPid', 'bootId']);
const normalize = (value) => JSON.parse(JSON.stringify(value, (k, v) => (VOLATILE_KEYS.has(k) ? '<volatile>' : v)));

/**
 * Run one script on a frozen, scripted clock so every persisted timestamp is reproducible. Returns what LIVE
 * admission produced: each poll's result, the skip/reservation files, lease and queue files, the live log lines.
 * `liveOnly` drops fields only this slice adds (a lease's `class`), so a main run and a branch run compare.
 */
export async function runScript(m, script, { allocationShadow, freshState, evaluator } = {}) {
  const state = freshState ?? fs.mkdtempSync(path.join(os.tmpdir(), 'lane-broker-scenario-'));
  fs.mkdirSync(state, { recursive: true });
  let clock = 1_700_000_000_000;
  const realNow = Date.now;
  Date.now = () => clock;
  try {
    const cfg = baseCfg(m, { allocationShadow: allocationShadow === true });
    const results = [];
    for (const step of script) {
      clock += 1000;
      if (step.enqueue) await m.enqueue(state, step.enqueue);
      else if (step.lease) m.writeLease(state, { ...step.lease, heartbeatAt: clock, admittedAt: clock });
      else if (step.release) fs.unlinkSync(path.join(m.paths(state).leases, `${step.release}.json`));
      else {
        const r = await m.tryStart(state, step.poll, cfg, undefined, sampler(step.ext ?? 0), undefined, memory, undefined, evaluator);
        results.push({ poll: step.poll.id, started: r.started, reason: r.reason ?? null, cpuReason: r.cpuReason ?? null });
      }
    }
    const p = m.paths(state);
    const raw = (file) => {
      try {
        return fs.readFileSync(file, 'utf8');
      } catch {
        return null;
      }
    };
    const dirJson = (dir, strip = []) =>
      Object.fromEntries(
        fs
          .readdirSync(dir)
          .sort()
          .map((n) => {
            const parsed = JSON.parse(fs.readFileSync(path.join(dir, n), 'utf8'));
            for (const key of strip) delete parsed[key];
            return [n, normalize(parsed)];
          }),
      );
    const log = raw(p.admissionLog) ?? '';
    return {
      state,
      results,
      conflictSkip: raw(p.conflictSkipState),
      capacitySkip: raw(p.capacitySkipState),
      resourceSkip: raw(p.resourceSkipState),
      leases: dirJson(p.leases, ['class']),
      queue: dirJson(p.queue),
      liveLog: log.split('\n').filter((l) => l && !l.startsWith('lane-broker-allocation-shadow')),
      shadowLines: log.split('\n').filter((l) => l.startsWith('lane-broker-allocation-shadow ')),
    };
  } finally {
    Date.now = realNow;
  }
}

/** The live-only part of a run: what the golden trace records. */
export const liveTrace = ({ results, conflictSkip, capacitySkip, resourceSkip, leases, queue, liveLog }) => ({ results, conflictSkip, capacitySkip, resourceSkip, leases, queue, liveLog });

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const srcDir = process.argv[2];
  if (!srcDir) {
    process.stderr.write('usage: node tests/allocation-scenarios.js <broker checkout>\n');
    process.exit(2);
  }
  const m = await loadModules(path.resolve(srcDir, 'src'));
  const trace = {};
  for (const [name, script] of Object.entries(scenarios(m))) trace[name] = liveTrace(await runScript(m, script));
  process.stdout.write(`${JSON.stringify(trace, null, 2)}\n`);
}
