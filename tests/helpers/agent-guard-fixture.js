import fs from 'node:fs';
import path from 'node:path';
import { realSrc } from './mutant-module.js';
import { SLEEP, happens } from '../agent-run-harness.js';
import { paths, readJsonSafe } from '../../src/state.js';

/** The ceiling for a wait on a precondition (the lane is running, a ticket is claimed): event-driven, so a generous figure costs nothing when it passes and keeps a slow runner from failing a test before its subject starts. */
export const PRECONDITION_MS = 60_000;

/** Budget for the outcome a scenario is about: generous for the real tree, short for a canary, whose guard is broken and which is expected to fail. */
export const wait = (load) => (load === realSrc ? PRECONDITION_MS : 3_500);

/**
 * The agent's clock: real time plus whatever a test has skipped. A wall-clock limit is armed by advancing it once the lane is
 * observed running, so the limit's size never races the time a slow runner takes to start the command.
 */
export function skewClock() {
  let skew = 0;
  return { now: () => Date.now() + skew, advance: (ms) => { skew += ms; } };
}

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
  await happens('the lane is running', () => agent.core.table.get(execId)?.subphase === 'RUNNING' && leaseOf(rw)?.childPgid, PRECONDITION_MS);
  return { ticket, agent, row, execId, lease: leaseOf(rw) };
}
