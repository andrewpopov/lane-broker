import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { LANE_SQL_DIR, connect } from '../src/lane-db.js';

/**
 * Disposable Postgres for the lane-db tests. initdb into a temp dir, listen on a Unix socket only, in a SHORT
 * /tmp path (sun_path is ~104 bytes on macOS; os.tmpdir() overflows it, see BRAIN-376), tear down on exit.
 */
const BIN_DIRS = [process.env.LANE_PG_BIN, '/opt/homebrew/bin', '/opt/homebrew/opt/postgresql@16/bin', '/usr/lib/postgresql/16/bin', '/usr/lib/postgresql/17/bin'].filter(Boolean);

function findBin() {
  for (const dir of BIN_DIRS) {
    if (['initdb', 'pg_ctl', 'postgres'].every((b) => fs.existsSync(path.join(dir, b)))) return dir;
  }
  return null;
}

/** Why the Postgres tests cannot run here, or null when they can. Tests skip with this message. */
export const PG_SKIP_REASON = findBin() ? null : 'postgres binaries (initdb, pg_ctl, postgres) not found; set LANE_PG_BIN or install PostgreSQL';

/** The environment for every child: git's exported repo-local vars must never reach a test process. */
function cleanEnv() {
  const env = { ...process.env, LC_ALL: 'C' };
  for (const name of Object.keys(env)) if (name.startsWith('GIT_')) delete env[name];
  return env;
}

function run(bin, args) {
  const r = spawnSync(path.join(findBin(), bin), args, { env: cleanEnv(), encoding: 'utf8' });
  if (r.status !== 0) throw new Error(`${bin} ${args.join(' ')} exited ${r.status}: ${r.stderr || r.stdout}`);
  return r;
}

export const roleNames = { agent1: 'lane_agent_h1', agent2: 'lane_agent_h2', submit: 'lane_test_submit', admin: 'lane_test_admin', reader: 'lane_test_reader' };

const LOGIN_ROLES = [['agent1', 'lane_agent'], ['agent2', 'lane_agent'], ['submit', 'lane_submit'], ['admin', 'lane_admin'], ['reader', 'lane_reader']];

// Roles are cluster-wide, so they are created once; the rows that reference them are per database.
const SEED_ROLES = `DO $do$ BEGIN
${LOGIN_ROLES.map(([k, g]) => `  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${roleNames[k]}') THEN CREATE ROLE ${roleNames[k]} LOGIN IN ROLE ${g}; END IF;`).join('\n')}
END $do$;`;

const SEED = `
INSERT INTO lane.hosts (host_id) VALUES ('h1'), ('h2');
INSERT INTO lane.principals (login_role, host_id, kind) VALUES ('${roleNames.agent1}', 'h1', 'agent'), ('${roleNames.agent2}', 'h2', 'agent');
INSERT INTO lane.principals (login_role, kind, allowed_accounts, allowed_dest_hosts) VALUES ('${roleNames.submit}', 'submit', '{acct}', '{h1,h2}');
INSERT INTO lane.principals (login_role, kind) VALUES ('${roleNames.admin}', 'admin');
INSERT INTO lane.host_class_policy (host_id, class, weight) VALUES ('h1', 'test', 1), ('h1', 'sim', 1);
INSERT INTO lane.command_templates (template_id, version) VALUES ('t', 1);
UPDATE lane.cluster SET claims_enabled = true;`;

export class PgCluster {
  constructor(dir, port) {
    this.dir = dir;
    this.port = port;
    this.clients = new Set();
    this.templateReady = false;
  }

  static start() {
    const dir = fs.mkdtempSync('/tmp/lbpg-');
    const port = 20000 + Math.floor(Math.random() * 40000);
    const cluster = new PgCluster(dir, port);
    const data = path.join(dir, 'data');
    try {
      run('initdb', ['-D', data, '-U', 'postgres', '-A', 'trust', '-E', 'UTF8', '--no-sync']);
      run('pg_ctl', ['-D', data, '-w', '-l', path.join(dir, 'pg.log'), '-o',
        `-c listen_addresses='' -c unix_socket_directories=${dir} -c port=${port} -c fsync=off -c max_connections=60`, 'start']);
    } catch (err) {
      cluster.stop();
      throw err;
    }
    cluster.exitHook = () => cluster.stop();
    process.on('exit', cluster.exitHook);
    return cluster;
  }

  conn(database, user = 'postgres') {
    return { host: this.dir, port: this.port, user, database };
  }

  async client(database, user = 'postgres') {
    const c = await connect(this.conn(database, user));
    this.clients.add(c);
    return c;
  }

  async admin() {
    return this.client('postgres');
  }

  /** A fresh database with the schema applied and the seed data loaded, cloned from a template. */
  async freshDb({ mutate } = {}) {
    const admin = await this.admin();
    const name = `t_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    if (mutate) {
      await admin.query(`CREATE DATABASE ${name}`);
      await this.applySql(name, mutate);
      await this.seed(name);
    } else {
      if (!this.templateReady) {
        await admin.query('CREATE DATABASE lane_template');
        await this.applySql('lane_template');
        await this.seed('lane_template');
        this.templateReady = true;
      }
      await admin.query(`CREATE DATABASE ${name} TEMPLATE lane_template`);
    }
    await admin.end();
    this.clients.delete(admin);
    return name;
  }

  /** Apply sql/lane in order. `mutate` is [from, to]: it must match, so a canary can never silently no-op. */
  async applySql(database, mutate) {
    const c = await this.client(database);
    let mutated = !mutate;
    for (const file of fs.readdirSync(LANE_SQL_DIR).filter((f) => f.endsWith('.sql')).sort()) {
      let text = fs.readFileSync(path.join(LANE_SQL_DIR, file), 'utf8');
      if (mutate && text.includes(mutate[0])) {
        text = text.replace(mutate[0], mutate[1]);
        mutated = true;
      }
      await c.query(text);
    }
    if (!mutated) throw new Error(`mutation target not found in sql/lane: ${mutate[0]}`);
    await c.end();
    this.clients.delete(c);
  }

  async seed(database) {
    const c = await this.client(database);
    await c.query(SEED_ROLES);
    await c.query(SEED);
    await c.end();
    this.clients.delete(c);
  }

  async closeClients() {
    for (const c of this.clients) await c.end().catch(() => {});
    this.clients.clear();
  }

  stop() {
    if (this.exitHook) process.removeListener('exit', this.exitHook);
    const data = path.join(this.dir, 'data');
    if (fs.existsSync(path.join(data, 'postmaster.pid'))) {
      spawnSync(path.join(findBin(), 'pg_ctl'), ['-D', data, '-m', 'immediate', '-w', 'stop'], { env: cleanEnv() });
    }
    fs.rmSync(this.dir, { recursive: true, force: true });
  }
}

export const ago = (seconds) => new Date(Date.now() - seconds * 1000);
export const gid = (n) => `00000000-0000-0000-0000-${String(n).padStart(12, '0')}`;

/** The jobs JSON `lane.submit_group` takes. */
export function jobsJson(n, { cls = 'test', est = 1000, cpu = 1, extra = {} } = {}) {
  return Array.from({ length: n }, (_, i) => ({
    idem_key: `k${i}`, class: cls, template_id: 't', template_version: 1, est_ref_ms: est, cpu_req: cpu, dup_safe: true, ...extra,
  }));
}

/**
 * Insert an ACTIVE group with its jobs directly (superuser), so a test controls activated_at / last_claim_at.
 * `jobs` is [{cls, est, cpu, n}]; est may differ per job to exercise the job-order keys.
 */
export async function addGroup(c, { n, tier = 1, activatedAt = ago(60), lastClaimAt = null, jobs = [{}], state = 'active' }) {
  await c.query(
    `INSERT INTO lane.groups (id, submit_uuid, kind, account, owner, tier, snapshot, aggregator, state, activated_at, last_claim_at)
     VALUES ($1, $1, 'single', 'acct', 'lane_test_submit', $2, '{}', 'x', $3, $4, $5)`,
    [gid(n), tier, state, activatedAt, lastClaimAt]);
  let seq = 0;
  for (const { cls = 'test', est = 1000, cpu = 1, count = 1 } of jobs) {
    await c.query(
      `INSERT INTO lane.jobs (group_id, seq, idem_key, class, template_id, template_version, params, est_ref_ms, cpu_req, cpu_min,
                              mem_bytes, dup_safe, max_infra, max_work)
       SELECT $1, $2::bigint + g, 'k' || ($2::bigint + g), $3, 't', 1, '{}', $4, $5, $5, 0, true, 3, 2 FROM generate_series(1, $6::int) g`,
      [gid(n), seq, cls, est, cpu, count]);
    seq += count;
  }
}

export const ROOM = { test: 1, sim: 1 };
