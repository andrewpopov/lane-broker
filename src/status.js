import fs from 'node:fs';
import { ensureStateDirs, paths, withLock, bootId, readJsonSafe } from './state.js';
import { listLeases, reapAll, LEASE_STATE } from './lease.js';
import { listQueue, HELD_STATES } from './scheduler.js';
import { readGateState } from './load.js';
import { loadGlobalConfig } from './config.js';

export async function collectStatus() {
  const root = ensureStateDirs().root;
  const cfg = loadGlobalConfig();
  await withLock(root, () => reapAll(root, bootId()));

  const leases = listLeases(root);
  const queue = listQueue(root);
  const gate = readGateState(root);
  const p = paths(root);
  const paused = fs.existsSync(p.pause) ? fs.readFileSync(p.pause, 'utf8').trim() : null;
  const configWarning = readJsonSafe(p.configWarning);

  const now = Date.now();
  const running = leases
    .filter((l) => l.state === LEASE_STATE.RUNNING || l.state === LEASE_STATE.ORPHANED)
    .map((l) => ({
      id: l.id,
      key: l.key,
      state: l.state,
      pid: l.childPgid,
      elapsedMs: l.startedAt ? now - l.startedAt : null,
      heartbeatAgeMs: l.heartbeatAt ? now - l.heartbeatAt : null,
      log: l.logPath,
      weight: l.weight,
    }));

  const runningWeight = leases.filter((l) => HELD_STATES.has(l.state)).reduce((s, l) => s + (l.weight || 0), 0);

  const queued = queue.map((t, i) => ({
    id: t.id,
    key: t.key,
    position: i + 1,
    waitedMs: now - t.createdAt,
  }));

  return {
    capacity: cfg.capacity,
    used: runningWeight,
    paused,
    configWarning: configWarning ? { message: configWarning.message, firstAt: configWarning.firstAt, lastAt: configWarning.lastAt } : null,
    loadGate: {
      closed: gate.closed,
      lastLoad: gate.lastLoad ?? null,
      sampleAgeMs: gate.lastSampleAt ? now - gate.lastSampleAt : null,
      consecutiveUnder: gate.consecutiveUnder || 0,
      loadClose: cfg.loadClose,
      loadOpen: cfg.loadOpen,
      loadOpenSamples: cfg.loadOpenSamples,
    },
    running,
    queued,
  };
}

function fmtMs(ms) {
  if (ms == null) return '-';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m${s % 60}s`;
}

export function renderStatusText(status) {
  const lines = [];
  lines.push(`capacity: ${status.used}/${status.capacity} used`);
  lines.push(
    `load gate: ${status.loadGate.closed ? 'CLOSED' : 'open'}` +
      ` (load ${status.loadGate.lastLoad ?? '?'}, sampled ${fmtMs(status.loadGate.sampleAgeMs)} ago` +
      `, close>${status.loadGate.loadClose}, open<${status.loadGate.loadOpen} x${status.loadGate.loadOpenSamples}` +
      `, consecutive-under ${status.loadGate.consecutiveUnder})`,
  );
  lines.push(`pause: ${status.paused ? `PAUSED — ${status.paused}` : 'not paused'}`);
  if (status.configWarning) {
    lines.push(
      `config: WARNING — some supervisor(s) cannot read the global config and are running on their last known-good ` +
        `values: ${status.configWarning.message} (since ${fmtMs(Date.now() - status.configWarning.firstAt)} ago)`,
    );
  }
  lines.push('');
  lines.push('RUNNING:');
  if (status.running.length === 0) {
    lines.push('  (none)');
  } else {
    for (const r of status.running) {
      const flag = r.state === 'ORPHANED' ? ' [ORPHANED]' : '';
      lines.push(
        `  ${r.id}  key=${r.key}  pid=${r.pid ?? '-'}  elapsed=${fmtMs(r.elapsedMs)}  ` +
          `heartbeat-age=${fmtMs(r.heartbeatAgeMs)}  log=${r.log}${flag}`,
      );
    }
  }
  lines.push('');
  lines.push('QUEUE (FIFO):');
  if (status.queued.length === 0) {
    lines.push('  (none)');
  } else {
    for (const q of status.queued) {
      lines.push(`  #${q.position} ${q.id}  key=${q.key}  waited=${fmtMs(q.waitedMs)}`);
    }
  }
  return lines.join('\n');
}

export async function statusCommand({ json } = {}) {
  const status = await collectStatus();
  if (json) {
    process.stdout.write(`${JSON.stringify(status, null, 2)}\n`);
  } else {
    process.stdout.write(`${renderStatusText(status)}\n`);
  }
  return { exitCode: 0 };
}
