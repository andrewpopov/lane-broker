import { budgetClient, ENV, PS_TIMEOUT_MS, requestSocket, usageRootReader } from '@andrewpopov/bosun/module';
import { clampMem } from './claims.js';
import { isPidAlive, processStartTimes } from '../process-liveness.js';
import { cpuBudgetCores, detectResourceCapacity } from '../resources.js';

export const USAGE_HEARTBEAT_MS = 20_000;

export const BUDGET_FIGURES = ['cpu_free', 'cpu_ceiling', 'mem_free', 'mem_ceiling', 'items'];

export function standaloneBudget({ cfg, host }) {
  const cpu = cpuBudgetCores(host, cfg);
  const mem = clampMem(host.memoryBytes - cfg.memoryReserveBytes);
  return { cpu: { free: cpu, ceiling: cpu }, mem: { free: mem, ceiling: mem } };
}

export const shadowSamples = ({ source, cpu, mem }) => ({ source, figures: { cpu_free: cpu.free, cpu_ceiling: cpu.ceiling, mem_free: mem.free, mem_ceiling: mem.ceiling, items: Math.floor(cpu.ceiling) } });

const isLive = (record) => record.subphase !== 'DONE';
const claimedOf = (record) => ({ cpu: record.claim?.cpuCores ?? record.claim?.header?.cpuCores ?? 0, mem: record.claim?.header?.memoryBytes ?? 0 });

export function usageOf(records) {
  const live = records.filter(isLive);
  const held = live.reduce((sum, record) => ({ cpu: sum.cpu + claimedOf(record).cpu, mem: sum.mem + claimedOf(record).mem }), { cpu: 0, mem: 0 });
  return { held: { ...held, items: live.length }, demand: held };
}

// Bosun matches a root by its own start identity, which differs from the `pidStart` lane-broker records. Both reads are one async `ps` each under one
// deadline (BRAIN-1368), so a stuck `ps` never holds the event loop. A lane whose start could not be read is kept, as `isProcessAlive` fails closed.
export function rootsReader({ startsOf, lstartsOf = processStartTimes, pidAlive = isPidAlive, deadlineMs = PS_TIMEOUT_MS } = {}) {
  const read = usageRootReader({ ...(startsOf ? { startsOf } : {}), deadlineMs });
  return async (records) => {
    const signal = AbortSignal.timeout(deadlineMs);
    const lanes = records.filter((r) => isLive(r) && Number.isInteger(r.pid) && typeof r.pidStart === 'string' && pidAlive(r.pid));
    const current = await lstartsOf(lanes.map((r) => r.pid), { timeoutMs: deadlineMs, signal });
    const unchanged = lanes.filter((r) => [undefined, r.pidStart].includes(current?.get(r.pid)));
    return read(unchanged.map(({ pid, pidStart }) => ({ pid, key: `${pid}:${pidStart}` })), { signal });
  };
}

/**
 * M6b: an enforced bosun grant caps what a claim offers. Bosun's `free` trails the last usage heartbeat, so each figure is also capped at the ceiling
 * less what this agent's live claims hold now, or a burst of claims would spend one grant several times. A standalone budget (no bosun, or an expired
 * grant) leaves the claim as admission computed it, so enforcement never fails closed.
 */
export function capByGrant(offered, budget, records = []) {
  if (!offered || budget?.source !== 'bosun') return offered;
  const { held } = usageOf(records);
  return {
    ...offered,
    freeCpu: Math.max(0, Math.min(offered.freeCpu, budget.cpu.free, budget.cpu.ceiling - held.cpu)),
    freeMem: Math.min(offered.freeMem, clampMem(budget.mem.free), clampMem(budget.mem.ceiling - held.mem)),
    budgetCpu: Math.min(offered.budgetCpu, budget.cpu.ceiling),
  };
}

export const budgetEnforced = (cfg) => cfg?.lanePull?.budgetEnforce === true;

export function openBudgetShadow({ readConfig, records, log, now = Date.now, env = process.env, host = detectResourceCapacity, call = requestSocket, heartbeatMs = USAGE_HEARTBEAT_MS, clientOptions = {}, rootOptions = {} }) {
  let capacity;
  const standalone = () => standaloneBudget({ cfg: readConfig(), host: capacity ??= host() });
  const client = budgetClient({ standalone: { get cpu() { return standalone().cpu; }, get mem() { return standalone().mem; } }, env, now, call, ...clientOptions });
  const rootsOf = rootsReader(rootOptions);
  const abort = new AbortController();
  const following = client.follow(abort.signal, (budget) => log(`budget: bosun grants ${JSON.stringify(shadowSamples(budget).figures)}${budget.gates.length ? ` gates ${budget.gates.map((g) => g.name).join(',')}` : ''}`)).catch((err) => log(`budget: ${err.message}`));
  let lastBeatAt = -Infinity;
  let inFlight = null;
  let lastSource = 'standalone';
  let lastError = null;
  const report = (problem) => {
    if (problem && problem !== lastError) log(`budget: ${problem}`);
    lastError = problem;
  };
  const beat = async () => {
    const [socket, module, token, incarnation] = [env[ENV.socket], env[ENV.module], env[ENV.token], Number(env[ENV.incarnation])];
    if (!socket || !module || !token || !Number.isInteger(incarnation)) return;
    try {
      const live = records();
      // A lane left out by the root limit counts as unmanaged until a later beat, which only under-grants.
      const { roots, omitted } = await rootsOf(live);
      const response = await call(socket, { method: 'PUT', path: `/v1/modules/${encodeURIComponent(module)}/usage`, authorization: `Bearer ${token}`, body: { incarnation, ...usageOf(live), roots } });
      report(response.status !== 204 ? `usage heartbeat refused: ${response.status}` : omitted ? `usage heartbeat declares ${roots.length} of ${roots.length + omitted} lane roots (bosun's limit)` : null);
    } catch (err) {
      report(`usage heartbeat failed: ${err.message}`);
    }
  };
  const shadow = () => {
    try {
      const budget = client.current();
      if (budget.source !== lastSource) log(`budget: now ${budget.source}`);
      lastSource = budget.source;
      return shadowSamples(budget);
    } catch (err) {
      report(`budget unreadable: ${err.message}`);
      return null;
    }
  };
  let lastEnforced = false;
  const enforced = (cfg) => {
    const on = budgetEnforced(cfg);
    if (on !== lastEnforced) log(`budget: claims ${on ? 'capped by the bosun grant' : 'no longer capped by bosun'}`);
    lastEnforced = on;
    if (!on) return null;
    try {
      return client.current();
    } catch (err) {
      report(`budget unreadable: ${err.message}`);
      return null;
    }
  };
  return {
    shadow,
    enforced,
    heartbeat() {
      if (inFlight || now() - lastBeatAt < heartbeatMs) return inFlight ?? Promise.resolve();
      lastBeatAt = now();
      inFlight = beat().finally(() => { inFlight = null; });
      return inFlight;
    },
    async close() {
      abort.abort();
      await following;
      await inFlight;
    },
  };
}
