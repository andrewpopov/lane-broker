import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { LANE_SQL_DIR, connect, claimNext } from '../src/lane-db.js';

/**
 * Disposable Postgres for the lane-db tests. initdb into a temp dir, listen on a Unix socket only, in a SHORT
 * /tmp path (sun_path is ~104 bytes on macOS; os.tmpdir() overflows it, see BRAIN-376), tear down on exit.
 * autovacuum is OFF so no table ever gets background statistics: the claim must hold its plan without them (BRAIN-400).
 */
// LANE_TEST_PG_BIN pins the Postgres under test (e.g. /usr/lib/postgresql/17/bin); when set, nothing else is tried,
// so a run meant for PG17 can never silently test whatever else is installed.
const BIN_DIRS = process.env.LANE_TEST_PG_BIN
  ? [process.env.LANE_TEST_PG_BIN]
  : ['/opt/homebrew/bin', '/opt/homebrew/opt/postgresql@16/bin', '/usr/lib/postgresql/16/bin', '/usr/lib/postgresql/17/bin'];

function findBin() {
  for (const dir of BIN_DIRS) {
    if (['initdb', 'pg_ctl', 'postgres'].every((b) => fs.existsSync(path.join(dir, b)))) return dir;
  }
  return null;
}

/** Why the Postgres tests cannot run here, or null when they can. Tests skip with this message. */
export const PG_SKIP_REASON = findBin() ? null : 'postgres binaries (initdb, pg_ctl, postgres) not found; set LANE_TEST_PG_BIN or install PostgreSQL';

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
UPDATE lane.cluster SET claims_enabled = true;
UPDATE lane.config SET wsjf_enabled = true, deadlines_enabled = true;`;

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
        `-c listen_addresses='' -c unix_socket_directories=${dir} -c port=${port} -c fsync=off -c autovacuum=off -c max_connections=60`, 'start']);
    } catch (err) {
      cluster.stop();
      throw err;
    }
    cluster.exitHook = () => cluster.stop();
    process.on('exit', cluster.exitHook);
    return cluster;
  }

  /** The server's version, e.g. "17.11", so a run can prove which Postgres it exercised. */
  async serverVersion() {
    const c = await this.admin();
    const { rows: [{ server_version: version }] } = await c.query('SHOW server_version');
    await c.end();
    this.clients.delete(c);
    return version;
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

  /** Apply sql/lane in order, as `user`. `mutate` is [from, to]: it must match, so a canary can never silently no-op. */
  async applySql(database, mutate, user = 'postgres') {
    const c = await this.client(database, user);
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
export function jobsJson(n, { cls = 'test', est = 100, cpu = 1, extra = {} } = {}) {
  return Array.from({ length: n }, (_, i) => ({
    idem_key: `k${i}`, class: cls, template_id: 't', template_version: 1, est_p50_s: est, cpu_req: cpu, dup_safe: true, ...extra,
  }));
}

/**
 * Insert an ACTIVE group with its jobs directly (superuser), so a test controls aging_anchor / last_claim_at, creating its
 * scheduling parent (account, prio) on first use. `parentAnchor` / `parentLastClaim` only apply when that call creates the parent.
 * `jobs` is [{cls, est (ref-s), cpu, cpuMin, count, stage, mem, exclusive}]; est may differ per job to exercise the job-order keys.
 * No ANALYZE: a live queue is not guaranteed statistics and the claim must not depend on them.
 */
export async function addGroup(c, {
  n, prio = 'normal', account = 'acct', activatedAt = ago(60), lastClaimAt = null, parentAnchor = activatedAt, parentLastClaim = lastClaimAt,
  deadline = null, deadlineValid = deadline !== null, jobs = [{}], state = 'active',
}) {
  await c.query(
    'INSERT INTO lane.sched_parents (account, prio_class, aging_anchor, last_claim_at) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING',
    [account, prio, parentAnchor, parentLastClaim]);
  const { rows: [{ id: parentId }] } = await c.query('SELECT id FROM lane.sched_parents WHERE account = $1 AND prio_class = $2', [account, prio]);
  await c.query(
    `INSERT INTO lane.groups (id, submit_uuid, kind, account, owner, parent_id, prio_class, snapshot, aggregator, state, aging_anchor, last_claim_at,
                              deadline_at, deadline_valid)
     VALUES ($1, $1, 'single', $2, 'lane_test_submit', $3, $4, '{}', 'x', $5, $6, $7, $8, $9)`,
    [gid(n), account, parentId, prio, state, activatedAt, lastClaimAt, deadline, deadlineValid]);
  let seq = 0;
  for (const { cls = 'test', est = 100, cpu = 1, cpuMin = cpu, count = 1, stage = 0, mem = 0, exclusive = false } of jobs) {
    await c.query(
      `INSERT INTO lane.jobs (group_id, seq, idem_key, stage, class, template_id, template_version, params, est_p50_s, est_p90_s, cpu_req, cpu_min,
                              mem_bytes, dup_safe, max_infra, max_work, exclusive)
       SELECT $1, $2::bigint + g, 'k' || ($2::bigint + g), $7, $3, 't', 1, '{}', $4::real, 2 * $4::real, $5, $6, $8, true, 3, 2, $10 FROM generate_series(1, $9::int) g`,
      [gid(n), seq, cls, est, cpu, cpuMin, stage, mem, count, exclusive]);
    seq += count;
  }
  return parentId;
}

/** Overwrite fields of a scheduling parent (superuser). */
export async function setParent(c, account, prio, fields) {
  const keys = Object.keys(fields);
  await c.query(`UPDATE lane.sched_parents SET ${keys.map((k, i) => `${k} = $${i + 3}`).join(', ')} WHERE account = $1 AND prio_class = $2`,
    [account, prio, ...keys.map((k) => fields[k])]);
}

/** One claim as the given agent, with `extra` overriding free / used / closed / memBytes. */
export const claimAs = (agent, extra = {}) => claimNext(agent, { generation: 1, free: FREE, memBytes: 1e12, token: randomUUID(), ...extra });

/** CPU the agent reports free in the ordering tests: room for every unit job, so size never decides the order. */
export const FREE = 8;

export const SCALE_CLASSES = ['test', 'sim', 'build'];
const SCALE_PRIOS = ['batch', 'normal', 'gate'];

/**
 * A realistic fleet on top of the standard seed: `hosts` agents (lane_agent_h1..hN) each with a policy row per class,
 * and `jobs` queued jobs spread over `groups` active groups and the three classes, with varied estimates.
 * Returns the agent login for each host.
 */
export async function seedScale(c, { jobs, groups, hosts }) {
  const agents = Array.from({ length: hosts }, (_, i) => ({ host: `h${i + 1}`, login: `lane_agent_h${i + 1}` }));
  for (const { host, login } of agents) {
    await c.query(`DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${login}') THEN CREATE ROLE ${login} LOGIN IN ROLE lane_agent; END IF; END $do$`);
    await c.query('INSERT INTO lane.hosts (host_id) VALUES ($1) ON CONFLICT DO NOTHING', [host]);
    await c.query("INSERT INTO lane.principals (login_role, host_id, kind) VALUES ($1, $2, 'agent') ON CONFLICT DO NOTHING", [login, host]);
    for (const [i, cls] of SCALE_CLASSES.entries()) {
      await c.query('INSERT INTO lane.host_class_policy (host_id, class, weight) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [host, cls, i + 1]);
    }
  }
  await c.query('UPDATE lane.principals SET allowed_dest_hosts = $1 WHERE kind = \'submit\'', [agents.map((a) => a.host)]);
  await c.query(
    `INSERT INTO lane.sched_parents (account, prio_class, aging_anchor) SELECT 'acct', p, now() - interval '5 minutes' FROM unnest($1::text[]) p`, [SCALE_PRIOS]);
  await c.query(
    `INSERT INTO lane.groups (id, submit_uuid, kind, account, owner, parent_id, prio_class, snapshot, aggregator, state, aging_anchor)
     SELECT ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid, ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid,
            'single', 'acct', '${roleNames.submit}', p.id, p.prio_class, '{}', 'x', 'active', now() - (g || ' minutes')::interval
       FROM generate_series(1, $1::int) g JOIN lane.sched_parents p ON p.account = 'acct' AND p.prio_class = ($2::text[])[1 + g % ${SCALE_PRIOS.length}]`, [groups, SCALE_PRIOS]);
  await c.query(
    `INSERT INTO lane.jobs (group_id, seq, idem_key, class, template_id, template_version, params, est_p50_s, est_p90_s, cpu_req, cpu_min, mem_bytes, dup_safe, max_infra, max_work)
     SELECT ('00000000-0000-0000-0000-' || lpad((1 + n % $1::int)::text, 12, '0'))::uuid, n, 'k' || n, ($3::text[])[1 + n % ${SCALE_CLASSES.length}], 't', 1, '{}',
            100 + (n * 7919) % 60000, 2 * (100 + (n * 7919) % 60000), 1, 1, 0, true, 3, 2
       FROM generate_series(1, $2::int) n`, [groups, jobs, SCALE_CLASSES]);
  // Deliberately NO ANALYZE: the claim has to hold its plan on a table nobody has analysed.
  return agents;
}
