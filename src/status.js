import fs from 'node:fs';
import path from 'node:path';
import { ensureStateDirs, paths, withLock, bootId, readJsonSafe } from './state.js';
import { listLeases, reapAll, LEASE_STATE } from './lease.js';
import { listQueue, HELD_STATES } from './scheduler.js';
import { readGateState } from './load.js';
import { readMemorySample, classifyMemorySample } from './memory.js';
import { loadGlobalConfig } from './config.js';

/** Holder pid of the global lock, read directly off disk — used to name the
 *  holder in the "couldn't take the lock" diagnostic without re-taking it. */
function currentLockHolderPid(root) {
  const owner = readJsonSafe(path.join(paths(root).lock, 'owner.json'));
  return owner ? owner.pid : null;
}

export async function collectStatus({ lockTimeoutMs = 5000 } = {}) {
  const root = ensureStateDirs().root;
  const cfg = loadGlobalConfig();
  let lockError = null;
  try {
    await withLock(root, () => reapAll(root, bootId()), { timeoutMs: lockTimeoutMs });
  } catch (err) {
    // The lock is only needed to reap stale leases before reporting; the
    // leases/queue/gate files underneath are readable without it. A timeout
    // here must be surfaced, never swallowed into a silent blank report.
    lockError = { message: err.message, holderPid: currentLockHolderPid(root) };
  }

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
      // BRAIN-207: whether a closed gate is actually allowed to deny a
      // start. false means the gate line below is informational only —
      // still sampled and reported, never enforced.
      admission: Boolean(cfg.admissionLoadGate),
    },
    // BRAIN-211: swap/compressor headroom, sampled fresh on every status
    // call. Purely informational — never an admission input; see memory.js.
    // null whenever the sample itself is unavailable (non-macOS host, or a
    // failed probe), never a fabricated reading.
    memory: classifyMemorySample(readMemorySample()),
    running,
    queued,
    lockError,
  };
}

function fmtMs(ms) {
  if (ms == null) return '-';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m${s % 60}s`;
}

function fmtMB(bytes) {
  if (bytes == null || !Number.isFinite(bytes)) return '?';
  return `${(bytes / (1024 * 1024)).toFixed(0)}MB`;
}

/** BRAIN-211: the memory line is informational-only, always — there is no
 *  admission flag to gate it on like loadGate.admission, since nothing ever
 *  reads this reading back for a start decision. */
function renderMemoryLine(memory) {
  if (!memory) {
    return 'memory: unavailable (no swap sample — see docs on BRAIN-211) [informational]';
  }
  const compressor = memory.compressorBytes == null ? '' : `, compressor ${fmtMB(memory.compressorBytes)}`;
  return (
    `memory: ${memory.level.toUpperCase()} (swap ${fmtMB(memory.swapUsedBytes)}/${fmtMB(memory.swapTotalBytes)}` +
    ` ${memory.swapUsedPct.toFixed(1)}%${compressor}) [informational]`
  );
}

export function renderStatusText(status) {
  const lines = [];
  lines.push(`capacity: ${status.used}/${status.capacity} used`);
  const informationalSuffix = status.loadGate.admission ? '' : ' [informational]';
  if (status.loadGate.lastLoad == null && status.loadGate.sampleAgeMs == null) {
    lines.push(`load gate: open (no sample yet — samples are taken when a ticket reaches the queue head)${informationalSuffix}`);
  } else {
    lines.push(
      `load gate: ${status.loadGate.closed ? 'CLOSED' : 'open'}` +
        ` (load ${status.loadGate.lastLoad ?? '?'}, sampled ${fmtMs(status.loadGate.sampleAgeMs)} ago` +
        `, close>${status.loadGate.loadClose}, open<${status.loadGate.loadOpen} x${status.loadGate.loadOpenSamples}` +
        `, consecutive-under ${status.loadGate.consecutiveUnder})${informationalSuffix}`,
    );
  }
  lines.push(renderMemoryLine(status.memory));
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

/** Write `text` to `stream` and resolve only once the write has actually
 *  been accepted by the underlying fd. `process.stdout.write` on a pipe is
 *  asynchronous and does not block the event loop on its own — if the
 *  process's exit code is already set and nothing else is keeping the loop
 *  alive, node can exit before a large or slow-draining write reaches the
 *  reader, so a caller under load can walk away with zero bytes written
 *  despite a clean exit code. Awaiting the write's own callback is what
 *  actually orders "reported" after "delivered". */
function writeAndFlush(stream, text) {
  return new Promise((resolve, reject) => {
    stream.write(text, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

export async function statusCommand({ json } = {}) {
  const root = ensureStateDirs().root;
  const status = await collectStatus();

  // The lock diagnostic goes to stderr in BOTH modes, before either branch:
  // a --json caller that only reads stdout still gets `lockError` in the
  // payload, but a human watching the terminal must see the warning too.
  if (status.lockError) {
    process.stderr.write(
      `lane status: could not take the broker lock within 5s (held by pid ${status.lockError.holderPid ?? 'unknown'}); ` +
        `showing the last persisted state, which may be stale\n`,
    );
  }

  if (json) {
    const text = `${JSON.stringify(status, null, 2)}\n`;
    if (status.capacity == null) {
      process.stderr.write(`lane status: internal error — rendered an empty report; state root ${root}\n`);
      return { exitCode: 1 };
    }
    await writeAndFlush(process.stdout, text);
    return { exitCode: status.lockError ? 1 : 0 };
  }

  const text = `${renderStatusText(status)}\n`;
  if (!text || !text.includes('capacity:')) {
    process.stderr.write(`lane status: internal error — rendered an empty report; state root ${root}\n`);
    return { exitCode: 1 };
  }
  await writeAndFlush(process.stdout, text);
  return { exitCode: status.lockError ? 1 : 0 };
}
