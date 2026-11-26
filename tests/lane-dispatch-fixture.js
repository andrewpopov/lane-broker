import fs from 'node:fs';
import { randomUUID } from 'node:crypto';
import { applyDatabaseAcls } from '../src/lane-db.js';
import { PgCluster, roleNames, gid } from './lane-db-harness.js';

export const logins = {
  a1: 'lane_agent_a1', a2: 'lane_agent_a2', x: 'lane_agent_x', s1: 'lane_submit_s1', s2: 'lane_submit_s2',
  admin: roleNames.admin, reader: roleNames.reader, legacy: roleNames.agent1, stranger: 'lane_stranger',
};

const NEW_LOGINS = [[logins.a1, 'lane_agent'], [logins.a2, 'lane_agent'], [logins.x, 'lane_agent'], [logins.s1, 'lane_submit'], [logins.s2, 'lane_submit']];

/** Cluster-level setup the SQL files deliberately leave out (docs/lane-dispatch-db-runbook.md). */
async function setUpClusterRoles(cluster) {
  const su = await cluster.admin();
  for (const [login, group] of NEW_LOGINS) {
    await su.query(`CREATE ROLE ${login} LOGIN IN ROLE ${group} CONNECTION LIMIT 3`);
    await su.query(`ALTER ROLE ${login} SET statement_timeout = '2s'`);
    await su.query(`ALTER ROLE ${login} SET idle_in_transaction_session_timeout = '5s'`);
  }
  await su.query(`CREATE ROLE ${logins.stranger} LOGIN`);
  await su.query('GRANT pg_signal_backend, pg_read_all_stats TO lane_admin');
}

export async function startDispatchCluster() {
  const cluster = PgCluster.start();
  await cluster.freshDb();
  await setUpClusterRoles(cluster);
  return cluster;
}

export class World {
  constructor(cluster, db) {
    this.cluster = cluster;
    this.db = db;
    this.connections = new Map();
  }

  static async create(cluster, { mutate } = {}) {
    const world = new World(cluster, await cluster.freshDb({ mutate }));
    const su = await world.su();
    await applyDatabaseAcls(su);
    const admin = await world.as(logins.admin);
    await call(admin, 'ld_set_principal', logins.a1, 'agent', 'a1', ['linux', 'fast'], ['repoA'], 2);
    await call(admin, 'ld_set_principal', logins.a2, 'agent', 'a2', ['linux'], ['*'], 1);
    await call(admin, 'ld_set_principal', logins.x, 'agent', 'x', [], ['*'], 1);
    await call(admin, 'ld_set_principal', logins.s1, 'submit', null, [], [], 1);
    await call(admin, 'ld_set_principal', logins.s2, 'submit', null, [], [], 1);
    for (const login of [logins.a1, logins.a2, logins.x]) await call(await world.as(login), 'ld_register', 'test', 1, null);
    return world;
  }

  su() {
    return this.as('postgres');
  }

  async as(login) {
    if (!this.connections.has(login)) this.connections.set(login, await this.cluster.client(this.db, login));
    return this.connections.get(login);
  }

  async ticket(id) {
    const su = await this.su();
    return (await su.query('SELECT * FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0];
  }
}

export async function call(client, name, ...args) {
  const { rows: [row] } = await client.query(`SELECT lane_dispatch.${name}(${args.map((_, i) => `$${i + 1}`).join(', ')}) AS v`, args);
  return row.v;
}

export const tid = gid;

export const enqueue = (client, id, { repo = 'repoA', laneKey = `lk-${id}`, tags = [], cpu = 1, mem = 1000, priority = 0, minProtocol = 1, ages = true, source = null, estRunS = null, avoidTags = [], requires = [], minCpu, weight, dispatchKey, capabilityClass, avoidHosts, targetHost } = {}) =>
  call(client, 'ld_enqueue', id, repo, 'test', laneKey, { protocol: 1 }, { cmd: ['true'], ...(weight === undefined ? {} : { weight }) }, { key: 'snap', sha256: 'x' }, tags, cpu, mem, priority, minProtocol, ages, source, estRunS, avoidTags, requires,
    ...(capabilityClass !== undefined || avoidHosts !== undefined || targetHost !== undefined ? [minCpu ?? null, dispatchKey ?? null, capabilityClass ?? null, avoidHosts ?? [], targetHost ?? null]
      : dispatchKey !== undefined ? [minCpu ?? null, dispatchKey] : minCpu === undefined ? [] : [minCpu]));

/** `capacity` (the runner's whole CPU budget, BRAIN-1099) and `freeWeight` with `weightLimit` (BRAIN-1106) are sent only when given, so the default call carries neither, as an older agent's does. */
export const claim = (client, { req = randomUUID(), cpu = 8, mem = 1e9, held = [], has, capacity, capacityMem = 1e10, freeWeight, weightLimit = null } = {}) =>
  call(client, 'ld_claim', req, cpu, mem, held, ...(capacity !== undefined || freeWeight !== undefined ? [has ?? [], capacity ?? null, capacityMem, ...(freeWeight === undefined ? [] : [freeWeight, weightLimit])] : has !== undefined ? [has] : []));

export const finish = (client, claimed, result = { kind: 'ok', exitCode: 0 }) => call(client, 'ld_finish', claimed.id, claimed.epoch, claimed.token, result);
export const start = (client, claimed) => call(client, 'ld_start', claimed.id, claimed.epoch, claimed.token);
export const heartbeat = (client, claimed) => call(client, 'ld_heartbeat', claimed.id, claimed.epoch, claimed.token);
export const release = (client, claimed, proof = 'never-started') => call(client, 'ld_release', claimed.id, claimed.epoch, claimed.token, proof);

/** A requeue backs the ticket off (5 s and up); a test that claims it again straight away lifts the backoff first. */
export async function liftBackoff(world) {
  await (await world.su()).query("UPDATE lane_dispatch.tickets SET not_before = '-infinity'");
}

/** Moves lane_dispatch.now() for sessions opened from here on (the GUC only counts when set on the database), so every connection the world held is reopened. */
export async function setNow(world, iso) {
  const su = await world.su();
  await su.query(`ALTER DATABASE ${world.db} SET lane.test_now = '${iso}'`);
  for (const [login, client] of [...world.connections].filter(([login]) => login !== 'postgres')) {
    await client.end().catch(() => {});
    world.cluster.clients.delete(client);
    world.connections.delete(login);
  }
}

/** Puts the world's queue back to schema version 6 (the 2fcebb0 functions.sql, a copy of the last release's): no 007, 008 or 009 columns, none of their function signatures. */
export async function downgradeToV6(world) {
  await downgradeToV8(world);
  const su = await world.su();
  await su.query('ALTER TABLE lane_dispatch.tickets DROP COLUMN avoid_tags, DROP COLUMN requires');
  await su.query('ALTER TABLE lane_dispatch.cluster DROP COLUMN avoid_grace_s');
  await su.query('DELETE FROM lane.schema_version WHERE version IN (7, 8)');
  await su.query(fs.readFileSync(new URL('./fixtures/lane-v6-functions.sql', import.meta.url), 'utf8'));
  await su.query('DROP FUNCTION lane_dispatch.ld_enqueue(uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real, text[], text[])');
  await su.query('DROP FUNCTION lane_dispatch.ld_enqueue_fenced(text, uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real, text[], text[])');
  await su.query('DROP FUNCTION lane_dispatch.ld_claim(uuid, real, bigint, text[], text[])');
  await su.query('DROP FUNCTION lane_dispatch.ld_needed_tools()');
}

/** Puts the world's queue back to schema version 8 (the 7a8fe38 functions.sql): no 009 columns, none of the 009 function signatures. */
export async function downgradeToV8(world) {
  await downgradeToV9(world);
  const su = await world.su();
  await su.query('ALTER TABLE lane_dispatch.tickets DROP COLUMN min_cpu_cores, DROP COLUMN claimed_cpu_cores');
  await su.query('ALTER TABLE lane_dispatch.cluster DROP COLUMN elastic_floor_after_s');
  await su.query('DELETE FROM lane.schema_version WHERE version = 9');
  await su.query(fs.readFileSync(new URL('./fixtures/lane-v8-functions.sql', import.meta.url), 'utf8'));
  await su.query('DROP FUNCTION lane_dispatch.ld_enqueue(uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real, text[], text[], real)');
  await su.query('DROP FUNCTION lane_dispatch.ld_enqueue_fenced(text, uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real, text[], text[], real)');
  await su.query('DROP FUNCTION lane_dispatch.ld_claim(uuid, real, bigint, text[], text[], real, bigint)');
  await su.query('DROP FUNCTION lane_dispatch.claim_need(real, real, interval, interval)');
}

/** Puts the world's queue back to schema version 9 (the 9e7487f functions.sql, BRAIN-1099): no 010 function signatures. */
export async function downgradeToV9(world) {
  const su = await world.su();
  // PKG-439 (migration 013): the placement functions and arities go; the columns stay, which a v9 function never reads. The v9 file defines its own two-argument scarcity, so the five-argument one must not stay beside it.
  await su.query('DROP FUNCTION lane_dispatch.ld_placeable(text, text[], text, text[], text)');
  await su.query('DROP FUNCTION lane_dispatch.ld_ticket_by_key(text, jsonb)');
  await su.query('DROP FUNCTION lane_dispatch.ld_known_hosts()');
  await su.query('DROP FUNCTION lane_dispatch.ld_set_platform(text, text)');
  await su.query('DROP FUNCTION lane_dispatch.scarcity(text[], text, text, text[], text)');
  await su.query('DROP FUNCTION lane_dispatch.ticket_view(uuid, text)');
  // PKG-438 (migration 012): the dispatch-key enqueues and lookup go; the column and its index stay, which a v9 function never reads
  await su.query('DROP FUNCTION lane_dispatch.ld_enqueue(uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real, text[], text[], real, text, text, text[], text)');
  await su.query('DROP FUNCTION lane_dispatch.ld_enqueue_fenced(text, uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real, text[], text[], real, text, text, text[], text)');
  await su.query('DROP FUNCTION lane_dispatch.ld_ticket_by_key(text)');
  await su.query('DELETE FROM lane.schema_version WHERE version = 10');
  await su.query('DROP FUNCTION lane_dispatch.ld_claim(uuid, real, bigint, text[], text[], real, bigint, real, real)');
  await su.query('DROP FUNCTION lane_dispatch.ticket_weight(jsonb, real)');
  await su.query(fs.readFileSync(new URL('./fixtures/lane-v9-functions.sql', import.meta.url), 'utf8'));
}

/** A restore, as far as the queue can tell: the recorded timeline no longer matches the cluster's. */
export async function simulateRestore(world) {
  const su = await world.su();
  await su.query('UPDATE lane_dispatch.cluster SET timeline = timeline + 1000');
}

/** Resolves once `pid` is waiting on a lock, so a test controls the order in which blocked calls queue. */
export async function waitBlocked(world, pid) {
  const su = await world.cluster.client(world.db);
  try {
    for (let i = 0; i < 400; i++) {
      const { rows } = await su.query("SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock'", [pid]);
      if (rows.length) return;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  } finally {
    await su.end();
    world.cluster.clients.delete(su);
  }
  throw new Error(`backend ${pid} never blocked on a lock`);
}

/** A second superuser connection that holds a lock until released, with `sql` as the lock. */
export async function holdLock(w, sql) {
  const holder = await w.cluster.client(w.db);
  await holder.query('BEGIN');
  await holder.query(sql);
  return { release: () => holder.query('COMMIT') };
}
