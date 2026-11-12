import { randomUUID } from 'node:crypto';
import { applyDatabaseAcls } from '../src/lane-db.js';
import { PgCluster, roleNames, gid } from './lane-db-harness.js';

export const logins = {
  a1: 'lane_agent_a1', a2: 'lane_agent_a2', s1: 'lane_submit_s1', s2: 'lane_submit_s2',
  admin: roleNames.admin, reader: roleNames.reader, legacy: roleNames.agent1, stranger: 'lane_stranger',
};

const NEW_LOGINS = [[logins.a1, 'lane_agent'], [logins.a2, 'lane_agent'], [logins.s1, 'lane_submit'], [logins.s2, 'lane_submit']];

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
    await call(admin, 'ld_set_principal', logins.s1, 'submit', null, [], [], 1);
    await call(admin, 'ld_set_principal', logins.s2, 'submit', null, [], [], 1);
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

export const enqueue = (client, id, { repo = 'repoA', laneKey = `lk-${id}`, tags = [], cpu = 1, mem = 1000, priority = 0 } = {}) =>
  call(client, 'ld_enqueue', id, repo, 'test', laneKey, { protocol: 1 }, { cmd: ['true'] }, { key: 'snap', sha256: 'x' }, tags, cpu, mem, priority);

export const claim = (client, { req = randomUUID(), cpu = 8, mem = 1e9, held = [] } = {}) => call(client, 'ld_claim', req, cpu, mem, held);

export const finish = (client, claimed, result = { kind: 'ok', exitCode: 0 }) => call(client, 'ld_finish', claimed.id, claimed.epoch, claimed.token, result);
export const start = (client, claimed) => call(client, 'ld_start', claimed.id, claimed.epoch, claimed.token);
export const heartbeat = (client, claimed) => call(client, 'ld_heartbeat', claimed.id, claimed.epoch, claimed.token);
export const release = (client, claimed, proof = 'never-started') => call(client, 'ld_release', claimed.id, claimed.epoch, claimed.token, proof);
