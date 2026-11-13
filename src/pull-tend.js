import fs from 'node:fs';
import path from 'node:path';
import { listAttempts, supervisorAlive } from './attempts.js';
import { DEFAULT_GLOBAL_CONFIG, loadGlobalConfigOrError } from './config.js';
import { ledgerDir, readLedger } from './pull-ledger.js';
import { pullServices } from './pull-services.js';
import { mapBounded } from './pull-snapshot.js';
import { stepPullTicket, urlsNeeded } from './pull-ticket.js';
import { ensureStateDirs, paths } from './state.js';

export const TEND_CONCURRENCY = 4;
export const TEND_STEP_BUDGET_MS = 10_000;
const MIN_TEND_DELAY_MS = 1_000;
const TICKET_FILE = /^([0-9a-f-]{36})\.json$/;
const TERMINAL_ROW_STATES = new Set(['finished', 'cancelled', 'lost']);

/** Lower is stepped first: a claimed row with no usable URLs is on the runner's 60 s clock, a settle can wait. */
export function tendRank(row, run) {
  if (row === undefined) return 4;
  const held = row.state === 'claimed' || row.state === 'running';
  if (row.state === 'claimed' && urlsNeeded(run, row)) return 0;
  if (held && urlsNeeded(run, row)) return 1;
  if (held) return 2;
  return TERMINAL_ROW_STATES.has(row.state) ? 3 : 4;
}

function resultlessLedgerTickets(root, skip) {
  let names;
  try {
    names = fs.readdirSync(ledgerDir(root));
  } catch {
    return [];
  }
  return names
    .map((name) => TICKET_FILE.exec(name)?.[1])
    .filter((id) => id && !skip.has(id) && !fs.existsSync(path.join(paths(root).results, `${id}.json`)))
    .map((id) => ({ id, generation: readLedger(root, id)?.generation }));
}

async function rowsOf(queue) {
  try {
    return new Map((await queue.listMine({ limit: 500 })).map((row) => [row.id, row]));
  } catch {
    return new Map();
  }
}

/**
 * One pass: every pull attempt whose supervisor is dead, plus every ledger entry with neither an attempt nor a result, stepped by rank with
 * `concurrency` at a time. A failure of one ticket is reported on its entry and never stops the others. `base` is a step context minus the ticket.
 */
export async function tendOnce({ root, concurrency = TEND_CONCURRENCY, stepBudgetMs = TEND_STEP_BUDGET_MS, ...base }) {
  const attempts = listAttempts(root).filter((a) => a.transport === 'pull' && !supervisorAlive(a));
  const tickets = [...attempts, ...resultlessLedgerTickets(root, new Set(attempts.map((a) => a.id)))];
  const rows = await rowsOf(base.queue);
  const run = { now: base.now ?? Date.now, cfg: { ...DEFAULT_GLOBAL_CONFIG.lanePull, ...base.cfg } };
  const ordered = tickets.map((t) => ({ id: t.id, generation: t.generation, rank: tendRank(rows.get(t.id), run) })).sort((a, b) => a.rank - b.rank);
  const results = [];
  await mapBounded(ordered, concurrency, async ({ id, generation }) => {
    try {
      const step = await stepPullTicket({ ...base, root, id, generation, stepBudgetMs, logPath: path.join(paths(root).logs, `${id}.log`), resultPath: path.join(paths(root).results, `${id}.json`) });
      results.push({ id, state: step.state, next: step.next });
    } catch (err) {
      results.push({ id, error: err.message });
    }
  });
  return results;
}

export function nextDelayMs(results, pollMs) {
  const waits = results.map((r) => (r.error ? pollMs : r.next ?? pollMs));
  return Math.min(pollMs, Math.max(MIN_TEND_DELAY_MS, waits.length ? Math.min(...waits) : pollMs));
}

const sleepUnlessAborted = (ms, signal) => new Promise((resolve) => {
  const timer = setTimeout(done, ms);
  signal?.addEventListener('abort', done, { once: true });
  function done() {
    clearTimeout(timer);
    signal?.removeEventListener('abort', done);
    resolve();
  }
});

export async function tendServe({ signal, log = () => {}, ...deps }) {
  const pollMs = { ...DEFAULT_GLOBAL_CONFIG.lanePull, ...deps.cfg }.pollMs;
  while (!signal?.aborted) {
    let results = [];
    try {
      results = await tendOnce(deps);
    } catch (err) {
      log(`lane pull tend: pass failed: ${err.message}\n`);
    }
    for (const r of results.filter((x) => x.error)) log(`lane pull tend: ${r.id}: ${r.error}\n`);
    await sleepUnlessAborted(nextDelayMs(results, pollMs), signal);
  }
}

export async function pullTendCommand(args = []) {
  const unknown = args.find((a) => a !== '--serve');
  if (unknown !== undefined) {
    process.stderr.write(`lane pull tend: unknown argument "${unknown}"\n`);
    return { exitCode: 2 };
  }
  const root = ensureStateDirs().root;
  const { config } = loadGlobalConfigOrError();
  const services = pullServices(config);
  if (!services.available) {
    process.stderr.write(`lane pull tend: ${services.reason}\n`);
    return { exitCode: 1 };
  }
  const deps = { root, queue: services.queue, s3: services.s3, presign: services.presign, cfg: config.lanePull };
  if (args.includes('--serve')) {
    const stop = new AbortController();
    for (const signalName of ['SIGTERM', 'SIGINT']) process.once(signalName, () => stop.abort());
    await tendServe({ ...deps, signal: stop.signal, log: (line) => process.stdout.write(line) });
    return { exitCode: 0 };
  }
  const results = await tendOnce(deps);
  for (const r of results) process.stdout.write(`${r.id} ${r.error ? `error: ${r.error}` : r.state}\n`);
  return { exitCode: results.some((r) => r.error) ? 1 : 0 };
}
