import { budgetClient, ENV, processStart, requestSocket, usageRoots } from '@andrewpopov/bosun/module';
import { clampMem } from './claims.js';
import { isProcessAlive } from '../process-liveness.js';
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

// Bosun matches a root by its own start identity, which differs from the `pidStart` lane-broker records; the SDK reads it once per lane (keyed by pid and pidStart, so a
// reused pid reads afresh) and applies bosun's root limit.
export function rootsReader({ startOf = processStart, alive = isProcessAlive } = {}) {
  const read = usageRoots({ startOf });
  const lanes = (records) => records.filter((r) => isLive(r) && Number.isInteger(r.pid) && typeof r.pidStart === 'string' && alive(r.pid, r.pidStart));
  return (records) => read(lanes(records).map(({ pid, pidStart }) => ({ pid, key: `${pid}:${pidStart}` })));
}

// Shadow only (M6a): bosun's budget is exported beside the standalone one and `claimFigures` stays authoritative. It never fails closed.
export function openBudgetShadow({ readConfig, records, log, now = Date.now, env = process.env, host = detectResourceCapacity, call = requestSocket, heartbeatMs = USAGE_HEARTBEAT_MS, clientOptions = {}, rootStartOf, rootAlive }) {
  let capacity;
  const standalone = () => standaloneBudget({ cfg: readConfig(), host: capacity ??= host() });
  const client = budgetClient({ standalone: { get cpu() { return standalone().cpu; }, get mem() { return standalone().mem; } }, env, now, call, ...clientOptions });
  const rootsOf = rootsReader({ startOf: rootStartOf, alive: rootAlive });
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
      const { roots, omitted } = rootsOf(live);
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
  return {
    shadow,
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
