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
export const TEND_HARD_DEADLINE_MS = 30_000;
const MIN_TEND_DELAY_MS = 1_000;
const LEDGER_NAMES_PER_PASS = 500;
const LEDGER_TICKETS_PER_PASS = 50;
const TICKET_FILE = /^([0-9a-f-]{36})\.json$/;
const TERMINAL_ROW_STATES = new Set(['finished', 'cancelled', 'lost']);
const ledgerScanOffsets = new Map();

/** Lower is stepped first: a claimed row with no usable URLs is on the runner's 60 s clock, a settle can wait. */
export function tendRank(row, run) {
  if (row === undefined) return 4;
  const held = row.state === 'claimed' || row.state === 'running';
  if (row.state === 'claimed' && urlsNeeded(run, row)) return 0;
  if (held && urlsNeeded(run, row)) return 1;
  if (held) return 2;
  return TERMINAL_ROW_STATES.has(row.state) ? 3 : 4;
}

// a result `lane gc` has expired is deliberate: only a decision younger than the result retention is worth rebuilding
function resultlessLedgerTickets(root, skip, { now, resultRetentionMs, namesPerPass, ticketsPerPass }) {
  let names;
  try {
    names = fs.readdirSync(ledgerDir(root)).sort();
  } catch {
    return [];
  }
  const start = (ledgerScanOffsets.get(root) ?? 0) % Math.max(names.length, 1);
  const window = Array.from({ length: Math.min(names.length, namesPerPass) }, (_, i) => names[(start + i) % names.length]);
  ledgerScanOffsets.set(root, start + window.length);
  const found = [];
  for (const name of window) {
    const id = TICKET_FILE.exec(name)?.[1];
    if (!id || skip.has(id) || fs.existsSync(path.join(paths(root).results, `${id}.json`))) continue;
    const entry = readLedger(root, id);
    if (entry !== null && now - (entry.acceptedAt ?? 0) < resultRetentionMs) found.push({ id, generation: entry.generation });
    if (found.length >= ticketsPerPass) break;
  }
  return found;
}

const withDeadline = (promise, ms, onTimeout) => {
  let timer;
  const expiry = new Promise((resolve) => { timer = setTimeout(() => resolve(onTimeout()), ms); });
  return Promise.race([promise, expiry]).finally(() => clearTimeout(timer));
};

export const dueDelayMs = (dueAts, now, pollMs) => Math.min(pollMs, Math.max(MIN_TEND_DELAY_MS, dueAts.length > 0 ? Math.min(...dueAts) - now : pollMs));

/**
 * Tickets are scheduled independently: a slot is taken per ticket, released when its step ends or passes the hard deadline (a hung step keeps its
 * ticket marked in flight, so it is never stepped twice, but it no longer holds a slot), and the next `round` re-lists. `base` is a step context minus the ticket.
 */
export function createTender({ root, concurrency = TEND_CONCURRENCY, stepBudgetMs = TEND_STEP_BUDGET_MS, hardDeadlineMs = TEND_HARD_DEADLINE_MS, resultRetentionMs = DEFAULT_GLOBAL_CONFIG.logRetentionMs, ledgerNamesPerPass = LEDGER_NAMES_PER_PASS, ledgerTicketsPerPass = LEDGER_TICKETS_PER_PASS, ...base }) {
  const now = base.now ?? Date.now;
  const settings = { ...DEFAULT_GLOBAL_CONFIG.lanePull, ...base.cfg };
  const inFlight = new Set();
  const dueAt = new Map();
  const results = [];
  let free = concurrency;
  let released = () => {};
  let releasedSignal = new Promise((resolve) => { released = resolve; });

  async function heldRows() {
    const none = () => new Map();
    try {
      const rows = await withDeadline(base.queue.listMine({ states: ['claimed', 'running'], limit: 500 }), hardDeadlineMs, () => []);
      return new Map(rows.map((row) => [row.id, row]));
    } catch {
      return none();
    }
  }

  async function candidates() {
    const attempts = listAttempts(root).filter((a) => a.transport === 'pull' && !supervisorAlive(a));
    const tickets = [...attempts, ...resultlessLedgerTickets(root, new Set(attempts.map((a) => a.id)), { now: now(), resultRetentionMs, namesPerPass: ledgerNamesPerPass, ticketsPerPass: ledgerTicketsPerPass })];
    for (const id of dueAt.keys()) if (!tickets.some((t) => t.id === id)) dueAt.delete(id);
    const rows = await heldRows();
    const run = { now, cfg: settings };
    return tickets
      .filter((t) => !inFlight.has(t.id) && (dueAt.get(t.id) ?? 0) <= now())
      .map((t) => ({ id: t.id, generation: t.generation, rank: tendRank(rows.get(t.id), run) }))
      .sort((a, b) => a.rank - b.rank);
  }

  function record(id, entry) {
    results.push({ id, ...entry });
    dueAt.set(id, now() + Math.max(MIN_TEND_DELAY_MS, entry.error ? settings.pollMs : entry.next ?? settings.pollMs));
  }

  function launch({ id, generation }) {
    inFlight.add(id);
    const step = stepPullTicket({ ...base, root, id, generation, stepBudgetMs, logPath: path.join(paths(root).logs, `${id}.log`), resultPath: path.join(paths(root).results, `${id}.json`) })
      .then((out) => record(id, { state: out.state, next: out.next }), (err) => record(id, { error: err.message }))
      .finally(() => inFlight.delete(id));
    return withDeadline(step, hardDeadlineMs, () => record(id, { error: `no answer within the ${hardDeadlineMs}ms step deadline` })).finally(() => {
      free++;
      released();
      releasedSignal = new Promise((resolve) => { released = resolve; });
    });
  }

  return {
    results,
    nextRelease: () => releasedSignal,
    delayMs: () => dueDelayMs([...dueAt.values()], now(), settings.pollMs),
    async round() {
      const launched = [];
      for (const item of await candidates()) {
        if (free === 0) break;
        free--;
        launched.push(launch(item));
      }
      return launched;
    },
  };
}

/** One pass: every due ticket stepped once, `concurrency` at a time, by rank. A failure of one ticket is reported on its entry and never stops the others. */
export async function tendOnce(deps) {
  const tender = createTender(deps);
  for (let launched = await tender.round(); launched.length > 0; launched = await tender.round()) await Promise.all(launched);
  return tender.results;
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
  const tender = createTender(deps);
  while (!signal?.aborted) {
    try {
      await tender.round();
    } catch (err) {
      log(`lane pull tend: pass failed: ${err.message}\n`);
    }
    for (const r of tender.results.splice(0).filter((x) => x.error)) log(`lane pull tend: ${r.id}: ${r.error}\n`);
    await Promise.race([sleepUnlessAborted(tender.delayMs(), signal), tender.nextRelease()]);
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
  const deps = { root, queue: services.queue, s3: services.s3, presign: services.presign, cfg: config.lanePull, resultRetentionMs: config.logRetentionMs };
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
