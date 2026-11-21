import fs from 'node:fs';
import path from 'node:path';
import { realSrc } from './mutant-module.js';
import { SLEEP, happens } from '../agent-run-harness.js';
import { paths, readJsonSafe } from '../../src/state.js';

/** Wait budget for a scenario: generous for the real tree, short for a canary, whose guard is broken and which is expected to fail. */
export const wait = (load) => (load === realSrc ? 20_000 : 3_500);

export const leaseOf = (rw) => {
  const dir = paths(rw.ctx.state).leases;
  const name = fs.existsSync(dir) ? fs.readdirSync(dir).find((n) => n.endsWith('.json')) : undefined;
  return name ? readJsonSafe(path.join(dir, name)) : null;
};

export const groupAlive = (pgid) => { try { process.kill(-pgid, 0); return true; } catch { return false; } };

export async function runningLane(w, rw, load, { code = SLEEP, ...agentOptions } = {}) {
  const ticket = await rw.submit({ code });
  const agent = await rw.agent({ load, ...agentOptions });
  const row = await rw.claimed(ticket);
  const execId = rw.execId(row);
  await happens('the lane is running', () => agent.core.table.get(execId)?.subphase === 'RUNNING' && leaseOf(rw)?.childPgid);
  return { ticket, agent, row, execId, lease: leaseOf(rw) };
}
