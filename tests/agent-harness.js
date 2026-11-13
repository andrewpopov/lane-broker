import { LdClient as RealLdClient } from '../src/agent/ld-client.js';
import { randomUUID } from 'node:crypto';
import { logins, call, enqueue, tid } from './lane-dispatch-fixture.js';

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const opened = new Set();

export const closeAgents = async () => {
  await Promise.all([...opened].map((c) => c.close()));
  opened.clear();
};

// statement_timeout=0 lets a blocked call outlive the role's 2 s default, so only the guard ends it.
export const agentPg = (w, login = logins.a1) => ({ ...w.cluster.conn(w.db, login), options: '-c statement_timeout=0' });

export async function openAgent(w, { login = logins.a1, snapshot = () => [], open = true, LdClient = RealLdClient, ...rest } = {}) {
  const calls = [];
  const events = [];
  const client = new LdClient({
    pg: agentPg(w, login), version: 'test', snapshot, onCall: (name, args) => calls.push([name, args]), onWake: (e) => events.push(e),
    backoff: { minMs: 20, maxMs: 100 }, ...rest,
  });
  opened.add(client);
  if (open) await client.open();
  return { client, calls, events, names: () => calls.map(([name]) => name) };
}

export async function backendPids(w, like, login = logins.a1) {
  const { rows } = await (await w.su()).query('SELECT pid FROM pg_stat_activity WHERE usename = $1 AND query LIKE $2', [login, like]);
  return rows.map((r) => r.pid);
}

export async function terminateBackend(w, pid) {
  const su = await w.su();
  await su.query('SELECT pg_terminate_backend($1)', [pid]);
  for (let i = 0; i < 100; i++) {
    if ((await su.query('SELECT 1 FROM pg_stat_activity WHERE pid = $1', [pid])).rows.length === 0) return;
    await sleep(20);
  }
  throw new Error(`backend ${pid} did not die`);
}

export async function guardUntilKill(w, maxMs = 15000) {
  const admin = await w.as(logins.admin);
  for (const t0 = Date.now(); Date.now() - t0 < maxMs; await sleep(250)) {
    if (await call(admin, 'ld_guard', '1 second') > 0) return;
  }
  throw new Error(`the guard killed nothing within ${maxMs} ms`);
}

export async function takeTicket(w, client, held, { n = 1, started = true, login = logins.s1 } = {}) {
  await enqueue(await w.as(login), tid(n));
  const claimed = await client.claim(randomUUID(), 8, 1e9);
  if (claimed.status !== 'claimed') throw new Error(`expected a claim, got ${JSON.stringify(claimed)}`);
  held.push({ id: claimed.id, epoch: claimed.epoch, token: claimed.token, phase: started ? 'running' : 'pre-start' });
  if (started && await client.start(claimed.id, claimed.epoch, claimed.token) !== true) throw new Error('ld_start refused');
  return claimed;
}
