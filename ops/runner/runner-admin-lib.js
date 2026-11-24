import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { isDeepStrictEqual, parseArgs } from 'node:util';

export const DB_HOST = 'pitelite';
export const DB_NAME = 'lane_queue_db';
export const TAILNET_DB = 'pitelite.tail615381.ts.net';
export const HBA_PATH = '/etc/postgresql/17/main/pg_hba.conf';
export const CA_PATH = '/etc/postgresql/17/main/homelab-postgres-ca.pem';
export const RUSTFS_ENV_PATH = '/home/admin/rustfs/rustfs.env';
export const TRAIN_CONFIG_PATH = '/data/train/config/lane-broker/config.json';
export const OP_VAULT = 'BotAccess';
export const REDACTED = '<redacted>';
const MC_ALIAS = 'pl';
const HOST_RE = /^[a-z][a-z0-9-]{0,30}$/;
const LIST_ITEM_RE = /^[\w.*:/@+-]+$/;

export const pgRole = (host) => `lane_agent_${host.replaceAll('-', '_')}`;
export const rustfsName = (host) => `lane-runner-${host}`;
export const opItemTitle = (host) => `lane-agent ${host}`;

export function validateHost(host) {
  if (!HOST_RE.test(host ?? '')) throw new Error(`invalid host ${JSON.stringify(host)}: must match ${HOST_RE}`);
  return host;
}

export function validateTailnetIp(ip) {
  const octets = String(ip ?? '').split('.');
  const wellFormed = octets.length === 4 && octets.every((o) => /^\d{1,3}$/.test(o) && String(Number(o)) === o && Number(o) <= 255);
  if (!wellFormed || Number(octets[0]) !== 100 || Number(octets[1]) < 64 || Number(octets[1]) > 127) {
    throw new Error(`invalid ip ${JSON.stringify(ip)}: must be an IPv4 address inside 100.64.0.0/10`);
  }
  return ip;
}

export const sqlIdent = (name) => `"${String(name).replaceAll('"', '""')}"`;

export function sqlLit(value) {
  const text = String(value);
  if (text.includes('\0')) throw new Error('a NUL byte cannot be quoted into SQL');
  return `'${text.replaceAll("'", "''")}'`;
}

export const shellQuote = (arg) => (/^[A-Za-z0-9_@%+=:,./-]+$/.test(arg) ? arg : `'${String(arg).replaceAll("'", "'\\''")}'`);

export function shellSplit(command) {
  const words = [];
  for (const match of command.matchAll(/'((?:[^']|'\\'')*)'|(\S+)/g)) words.push(match[1] !== undefined ? match[1].replaceAll("'\\''", "'") : match[2]);
  return words;
}

const sqlTextArray = (items) => (items.length === 0 ? 'ARRAY[]::text[]' : `ARRAY[${items.map(sqlLit).join(',')}]::text[]`);

export function roleSql({ role, host, password, roleExists, tags, repos, maxClaims }) {
  return [
    roleExists
      ? `ALTER ROLE ${sqlIdent(role)} PASSWORD ${sqlLit(password)};`
      : `CREATE ROLE ${sqlIdent(role)} LOGIN PASSWORD ${sqlLit(password)} IN ROLE lane_agent CONNECTION LIMIT 3;`,
    `ALTER ROLE ${sqlIdent(role)} SET statement_timeout = '2s';`,
    `ALTER ROLE ${sqlIdent(role)} SET idle_in_transaction_session_timeout = '5s';`,
    `SELECT lane_dispatch.ld_set_principal(${sqlLit(role)}, 'agent', ${sqlLit(host)}, ${sqlTextArray(tags)}, ${sqlTextArray(repos)}, ${Number(maxClaims)});`,
    '',
  ].join('\n');
}

export const roleExistsSql = (role) => `SELECT count(*) FROM pg_roles WHERE rolname = ${sqlLit(role)};\n`;
export const heldTicketsSql = (host) => `SELECT id, lane FROM lane_dispatch.tickets WHERE host = ${sqlLit(host)} AND state IN ('claimed','running') ORDER BY created_at;\n`;
export const hbaErrorCountSql = () => 'SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL;\n';
export const reloadSql = () => 'SELECT pg_reload_conf();\n';

export function removeSql({ role, host }) {
  return [
    `DELETE FROM lane_dispatch.principals WHERE login_role = ${sqlLit(role)};`,
    `DELETE FROM lane_dispatch.runners WHERE host = ${sqlLit(host)};`,
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = ${sqlLit(role)};`,
    `DROP ROLE IF EXISTS ${sqlIdent(role)};`,
    '',
  ].join('\n');
}

const BEGIN_MARK = (host) => `# BEGIN lane-runner-admin ${host}`;
const END_MARK = (host) => `# END lane-runner-admin ${host}`;
const BLOCK_ANCHOR = /^local\s+lane_queue_db\s+all\s+reject/;
const LOCAL_REJECT = /^(local\s+all\s+)(\S+)(\s+reject.*)$/;
const HOST_REJECT = /^(host\s+all\s+)(\S+)(\s+all\s+reject.*)$/;

function hbaBlock({ role, host, ip, note }) {
  const why = `# lane-agent-${host}: from its own source address only, never through the svc:pg-primary forward${note ? `; ${note}` : ''}`;
  return [BEGIN_MARK(host), why, `hostssl ${DB_NAME}  ${role}  ${ip}/32  scram-sha-256`, END_MARK(host)];
}

const laneLoginsList = (roles, role) => roles.some((r) => r.startsWith('lane_agent_') && r !== role);

function editRejectList(lines, pattern, label, role, mode) {
  const targets = lines.flatMap((line, i) => {
    const roles = pattern.exec(line)?.[2].split(',');
    if (!roles) return [];
    return (mode === 'add' ? laneLoginsList(roles, role) : roles.includes(role)) ? [i] : [];
  });
  if (mode === 'add' && targets.length !== 1) {
    throw new Error(`pg_hba.conf needs exactly one "${label}" reject line listing the lane logins, found ${targets.length}`);
  }
  const edited = [...lines];
  for (const i of targets) {
    const match = pattern.exec(lines[i]);
    const roles = match[2].split(',');
    const next = mode === 'add' ? (roles.includes(role) ? roles : [...roles, role]) : roles.filter((r) => r !== role);
    if (next.length === 0) throw new Error(`removing ${role} would empty the ${label} reject list`);
    edited[i] = `${match[1]}${next.join(',')}${match[3]}`;
  }
  return edited;
}

export function editHba(text, { role, host, ip, note }, mode) {
  if (mode !== 'add' && mode !== 'remove') throw new Error(`editHba mode must be add or remove, not ${mode}`);
  let lines = text.split('\n');
  const begin = lines.indexOf(BEGIN_MARK(host));
  let at = -1;
  if (begin >= 0) {
    const end = lines.indexOf(END_MARK(host), begin);
    if (end < 0) throw new Error(`pg_hba.conf has "${BEGIN_MARK(host)}" with no matching END line`);
    lines.splice(begin, end - begin + 1);
    at = begin;
  }
  if (mode === 'add') {
    const handWritten = new RegExp(`^hostssl?\\s+${DB_NAME}\\s+${role}\\s`);
    if (lines.some((line) => handWritten.test(line))) throw new Error(`pg_hba.conf already has a hand-written ${DB_NAME} line for ${role}; remove it first`);
    if (at < 0) at = lines.findIndex((line) => BLOCK_ANCHOR.test(line));
    if (at < 0) throw new Error(`pg_hba.conf has no "local ${DB_NAME} all reject" line to anchor the block before`);
    lines.splice(at, 0, ...hbaBlock({ role, host, ip, note }));
  }
  lines = editRejectList(lines, LOCAL_REJECT, 'local all', role, mode);
  lines = editRejectList(lines, HOST_REJECT, 'host all', role, mode);
  return lines.join('\n');
}

export function hbaState(text, { role, host }) {
  const lines = text.split('\n');
  const listed = (pattern) => lines.some((line) => pattern.exec(line)?.[2].split(',').includes(role));
  return {
    block: lines.includes(BEGIN_MARK(host)),
    line: lines.find((line) => new RegExp(`^hostssl?\\s+${DB_NAME}\\s+${role}\\s`).test(line)) ?? null,
    localRejected: listed(LOCAL_REJECT),
    hostRejected: listed(HOST_REJECT),
  };
}

const jsonIndent = (text) => /^[ \t]+(?=")/m.exec(text)?.[0] ?? '  ';

const INLINE_ENTRY = /^(\s*)(\{.*"name"\s*:\s*"([^"]*)".*\}),?\s*$/;

// Keeps the file's own layout when every runner sits on one line; null sends the caller to a full re-serialise.
function editInlineRunners(text, host, mode, expected) {
  const lines = text.split('\n');
  const entries = lines.flatMap((line, at) => (INLINE_ENTRY.test(line) ? [at] : []));
  if (entries.length === 0 || entries.length !== JSON.parse(text).runners.length) return null;
  const last = entries.at(-1);
  if (mode === 'add') {
    const [, indent, body] = INLINE_ENTRY.exec(lines[last]);
    const spaced = body.startsWith('{ ');
    const entry = spaced ? `{ "name": ${JSON.stringify(host)}, "transport": "pull" }` : JSON.stringify({ name: host, transport: 'pull' });
    lines[last] = lines[last].replace(/,?\s*$/, ',');
    lines.splice(last + 1, 0, `${indent}${entry}`);
  } else {
    const at = entries.find((entryAt) => INLINE_ENTRY.exec(lines[entryAt])[3] === host);
    const wasLast = at === last;
    lines.splice(at, 1);
    if (wasLast && entries.length > 1) lines[entries.at(-2)] = lines[entries.at(-2)].replace(/,\s*$/, '');
  }
  const edited = lines.join('\n');
  try { return isDeepStrictEqual(JSON.parse(edited), expected) ? edited : null; } catch { return null; }
}

export function editTrainRunners(text, host, mode) {
  const config = JSON.parse(text);
  if (!Array.isArray(config.runners)) throw new Error('train config has no .runners array');
  const index = config.runners.findIndex((runner) => runner?.name === host);
  if (mode === 'add') {
    if (index >= 0 && config.runners[index].transport === 'pull') return text;
    if (index >= 0) config.runners[index] = { ...config.runners[index], transport: 'pull' };
    else config.runners.push({ name: host, transport: 'pull' });
  } else if (index < 0) return text;
  else config.runners.splice(index, 1);
  return editInlineRunners(text, host, mode, config) ?? `${JSON.stringify(config, null, jsonIndent(text))}${text.endsWith('\n') ? '\n' : ''}`;
}

export const trainHasRunner = (text, host) => (JSON.parse(text).runners ?? []).some((runner) => runner?.name === host);

export const rustfsPolicy = (host) => `${JSON.stringify({
  Version: '2012-10-17',
  Statement: [
    { Effect: 'Allow', Action: ['s3:GetObject'], Resource: ['arn:aws:s3:::lane-snap/*'] },
    { Effect: 'Allow', Action: ['s3:PutObject'], Resource: [`arn:aws:s3:::lane-out/${host}/*`] },
    { Effect: 'Allow', Action: ['s3:GetObject'], Resource: [`arn:aws:s3:::lane-out/${host}/.probe`] },
  ],
}, null, 2)}\n`;

export function workerConfig(host, role) {
  return `${JSON.stringify({
    runnerName: host,
    laneQueueUrl: `postgres://${role}@${TAILNET_DB}:5432/${DB_NAME}`,
    speedFactor: 1,
    rustfs: { endpoint: 'http://pitelite:9000' },
    rustfsEnvFile: 'rustfs.env',
  }, null, 2)}\n`;
}

export function parseEnvFile(text) {
  const entries = {};
  for (const line of text.split('\n')) {
    const match = /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
    if (match) entries[match[1]] = match[2].trim().replace(/^(['"])(.*)\1$/, '$2');
  }
  return entries;
}

export function unifiedDiff(before, after, label) {
  const a = before.split('\n');
  const b = after.split('\n');
  const lcs = Array.from({ length: a.length + 1 }, () => new Uint32Array(b.length + 1));
  for (let i = a.length - 1; i >= 0; i -= 1) {
    for (let j = b.length - 1; j >= 0; j -= 1) lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
  }
  const ops = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) { ops.push({ tag: ' ', text: a[i], a: i, b: j }); i += 1; j += 1; }
    else if (lcs[i + 1][j] >= lcs[i][j + 1]) { ops.push({ tag: '-', text: a[i], a: i, b: j }); i += 1; }
    else { ops.push({ tag: '+', text: b[j], a: i, b: j }); j += 1; }
  }
  for (; i < a.length; i += 1) ops.push({ tag: '-', text: a[i], a: i, b: j });
  for (; j < b.length; j += 1) ops.push({ tag: '+', text: b[j], a: i, b: j });

  const CONTEXT = 3;
  const changed = ops.flatMap((op, k) => (op.tag === ' ' ? [] : [k]));
  if (changed.length === 0) return '';
  const hunks = [];
  for (const k of changed) {
    const last = hunks.at(-1);
    if (last && k - CONTEXT - 1 <= last.to) last.to = k + CONTEXT;
    else hunks.push({ from: Math.max(0, k - CONTEXT), to: k + CONTEXT });
  }
  const out = [`--- ${label} (current)`, `+++ ${label} (proposed)`];
  for (const { from, to } of hunks) {
    const slice = ops.slice(from, Math.min(to, ops.length - 1) + 1);
    const aCount = slice.filter((op) => op.tag !== '+').length;
    const bCount = slice.filter((op) => op.tag !== '-').length;
    out.push(`@@ -${slice[0].a + 1},${aCount} +${slice[0].b + 1},${bCount} @@`);
    for (const op of slice) out.push(`${op.tag}${op.text}`);
  }
  return out.join('\n');
}

export function spawnExec(cmd, args, { input, env } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'pipe'] });
    const stdout = [];
    const stderr = [];
    child.stdout.on('data', (chunk) => stdout.push(chunk));
    child.stderr.on('data', (chunk) => stderr.push(chunk));
    child.on('error', reject);
    child.on('close', (code) => resolve({ code: code ?? 1, stdout: Buffer.concat(stdout).toString('utf8'), stderr: Buffer.concat(stderr).toString('utf8') }));
    child.stdin.on('error', () => {});
    child.stdin.end(input ?? '');
  });
}

const ALNUM = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
export const defaultRandom = {
  password: () => crypto.randomBytes(32).toString('base64url'),
  alnum: (length) => Array.from({ length }, () => ALNUM[crypto.randomInt(ALNUM.length)]).join(''),
};

const utcStamp = (date) => date.toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');

class Context {
  constructor({ exec, random = defaultRandom, now = () => new Date(), fsImpl = fs, opts }) {
    Object.assign(this, { exec, random, now, fs: fsImpl, opts, secrets: null, rootCreds: null });
    this.known = new Set();
  }

  learn(...values) { for (const value of values) if (value) this.known.add(value); }

  scrub(text) {
    let clean = String(text);
    for (const secret of this.known) clean = clean.replaceAll(secret, REDACTED);
    return clean;
  }

  async run(cmd, args, { input, env, label, allow } = {}) {
    const result = await this.exec(cmd, args, { input, env });
    if (result.code !== 0 && !allow?.test(`${result.stderr}${result.stdout}`)) {
      throw new Error(`${label ?? cmd} failed (exit ${result.code}): ${this.scrub(result.stderr || result.stdout).trim()}`);
    }
    return result;
  }

  remote(args, opts) { return this.run('ssh', [DB_HOST, args.map(shellQuote).join(' ')], opts); }

  psql(sql, { query = false, label = 'psql' } = {}) {
    const flags = query ? ['-X', '-tAq', '-F', '|'] : [];
    return this.remote(['sudo', '-n', '-u', 'postgres', 'psql', '-d', DB_NAME, '-v', 'ON_ERROR_STOP=1', ...flags], { input: sql, label });
  }

  async psqlRows(sql, label) {
    const { stdout } = await this.psql(sql, { query: true, label });
    return stdout.split('\n').filter(Boolean).map((row) => row.split('|'));
  }

  async rootCredentials() {
    if (!this.rootCreds) {
      const env = parseEnvFile((await this.remote(['sudo', '-n', 'cat', RUSTFS_ENV_PATH], { label: 'read rustfs.env' })).stdout);
      if (!env.RUSTFS_ACCESS_KEY || !env.RUSTFS_SECRET_KEY) throw new Error(`${RUSTFS_ENV_PATH} lacks RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY`);
      this.rootCreds = { accessKey: env.RUSTFS_ACCESS_KEY, secretKey: env.RUSTFS_SECRET_KEY };
      this.learn(env.RUSTFS_SECRET_KEY);
    }
    return this.rootCreds;
  }

  async mc(args, opts = {}) {
    const { accessKey, secretKey } = await this.rootCredentials();
    const target = `http://${encodeURIComponent(accessKey)}:${encodeURIComponent(secretKey)}@${TAILNET_DB}:9000`;
    return this.run('mc', args, { ...opts, env: { MC_HOST_pl: target }, label: `mc ${args.slice(0, 3).join(' ')}` });
  }

  async withPrivateDir(use) {
    const dir = this.fs.mkdtempSync(path.join(os.tmpdir(), 'lane-runner-admin-'));
    try { return await use(dir); } finally { this.fs.rmSync(dir, { recursive: true, force: true }); }
  }

  async getItem() {
    const result = await this.run('op-bot', ['item', 'get', opItemTitle(this.opts.host), '--vault', OP_VAULT, '--format', 'json'], { label: 'op-bot item get', allow: /isn't an item|not found|no item/i });
    if (result.code !== 0) return null;
    const item = JSON.parse(result.stdout);
    const field = (match) => item.fields?.find(match)?.value;
    const found = {
      password: field((f) => f.id === 'password' || f.purpose === 'PASSWORD'),
      accessKey: field((f) => f.label === 'rustfs_access_key'),
      secretKey: field((f) => f.label === 'rustfs_secret_key'),
    };
    if (!found.password || !found.accessKey || !found.secretKey) throw new Error(`1Password item "${opItemTitle(this.opts.host)}" exists but lacks password, rustfs_access_key or rustfs_secret_key`);
    this.learn(found.password, found.secretKey);
    return found;
  }

  async ensureSecrets() {
    const existing = await this.getItem();
    this.secrets = existing ?? { password: this.random.password(), accessKey: this.random.alnum(20), secretKey: this.random.alnum(40) };
    this.learn(this.secrets.password, this.secrets.secretKey);
    if (existing) return 'reused the values already in the item';
    const template = {
      title: opItemTitle(this.opts.host),
      category: 'PASSWORD',
      fields: [
        { id: 'password', label: 'password', type: 'CONCEALED', purpose: 'PASSWORD', value: this.secrets.password },
        { id: 'rustfs_access_key', label: 'rustfs_access_key', type: 'STRING', value: this.secrets.accessKey },
        { id: 'rustfs_secret_key', label: 'rustfs_secret_key', type: 'CONCEALED', value: this.secrets.secretKey },
      ],
    };
    await this.withPrivateDir(async (dir) => {
      const file = path.join(dir, 'item.json');
      this.fs.writeFileSync(file, JSON.stringify(template), { mode: 0o600 });
      await this.run('op-bot', ['item', 'create', '--vault', OP_VAULT, '--template', file], { label: 'op-bot item create' });
    });
    return 'created the item with generated values';
  }
}

async function editRemoteFile(ctx, { readArgs, writeArgs, copyArgs, path: file, edit, validate, label }) {
  const before = (await ctx.remote(readArgs, { label: `read ${label}` })).stdout;
  const after = edit(before);
  return { before, after, file, changed: after !== before, write: async () => {
    const backup = `${file}.bak-lane-runner-admin-${utcStamp(ctx.now())}`;
    await ctx.remote([...copyArgs, '-p', file, backup], { label: `back up ${label}` });
    await ctx.remote(writeArgs, { input: after, label: `write ${label}` });
    const problem = await validate(after);
    if (problem) {
      await ctx.remote([...copyArgs, '-p', backup, file], { label: `RESTORE ${label} from ${backup}` });
      throw new Error(`${label} was rejected (${problem}); restored ${backup}`);
    }
    return backup;
  } };
}

const hbaEdit = (ctx, mode) => editRemoteFile(ctx, {
  readArgs: ['sudo', '-n', 'cat', HBA_PATH],
  writeArgs: ['sudo', '-n', 'tee', HBA_PATH],
  copyArgs: ['sudo', '-n', 'cp'],
  path: HBA_PATH,
  label: 'pg_hba.conf',
  edit: (text) => editHba(text, ctx.hbaEntry, mode),
  validate: async () => {
    const errorCount = Number((await ctx.psql(hbaErrorCountSql(), { query: true, label: 'validate pg_hba.conf' })).stdout.trim());
    if (errorCount > 0) return `${errorCount} rule errors in pg_hba_file_rules`;
    if (!Number.isInteger(errorCount)) return 'pg_hba_file_rules count unreadable';
    return null;
  },
});

const trainEdit = (ctx, mode) => editRemoteFile(ctx, {
  readArgs: ['sudo', '-n', '-u', 'train', 'cat', TRAIN_CONFIG_PATH],
  writeArgs: ['sudo', '-n', '-u', 'train', 'tee', TRAIN_CONFIG_PATH],
  copyArgs: ['sudo', '-n', '-u', 'train', 'cp'],
  path: TRAIN_CONFIG_PATH,
  label: 'train config',
  edit: (text) => editTrainRunners(text, ctx.opts.host, mode),
  validate: async (after) => {
    try { JSON.parse(after); return null; } catch (err) { return err.message; }
  },
});

const PROVISION_REMINDER = 'agent_brain/ops/pitelite-controller/provision.sh writes this file: a re-provision drops the runner unless it is added there too.';

function editStep(ctx, { title, kind, load, extra }) {
  return {
    title,
    kind,
    async preview() {
      const edit = await load();
      return `${edit.changed ? unifiedDiff(edit.before, edit.after, edit.file) : `${edit.file}: no change`}${extra ? `\n${extra}` : ''}`;
    },
    async apply() {
      const edit = await load();
      if (!edit.changed) return `${edit.file}: already converged${extra ? `\n${extra}` : ''}`;
      return `backed up to ${await edit.write()}${extra ? `\n${extra}` : ''}`;
    },
  };
}

function hbaStep(ctx, mode) {
  const step = editStep(ctx, { title: `pg_hba.conf: ${mode} the ${ctx.hbaEntry.role} block and reject-list entries`, kind: 'hba', load: () => hbaEdit(ctx, mode) });
  const apply = step.apply;
  step.apply = async () => {
    const message = await apply();
    await ctx.psql(reloadSql(), { label: 'pg_reload_conf' });
    return `${message}\nreloaded postgres configuration`;
  };
  const preview = step.preview;
  step.preview = async () => `${await preview()}\nthen: ${reloadSql().trim()}`;
  return step;
}

const trainStep = (ctx, mode) => editStep(ctx, { title: `train config: ${mode} runner ${ctx.opts.host}`, kind: 'train', load: () => trainEdit(ctx, mode), extra: `note: ${PROVISION_REMINDER}` });

function buildAddPlan(ctx) {
  const { host, ip, out } = ctx.opts;
  const role = pgRole(host);
  const principal = { role, host, tags: ctx.opts.tags, repos: ctx.opts.repos, maxClaims: ctx.opts.maxClaims };
  ctx.hbaEntry = { role, host, ip, note: ctx.opts.note };
  const roleExists = async () => Number((await ctx.psql(roleExistsSql(role), { query: true, label: 'check role' })).stdout.trim()) > 0;
  const steps = [
    {
      title: `1Password: item "${opItemTitle(host)}" in vault ${OP_VAULT} (password, rustfs_access_key, rustfs_secret_key)`,
      kind: 'secrets',
      async preview() { return (await ctx.getItem()) ? 'item exists: its values will be reused' : 'item absent: would generate a 32-byte password, a 20-char access key and a 40-char secret and create it'; },
      apply: () => ctx.ensureSecrets(),
    },
    {
      title: `postgres: role ${role} and principal row (via ssh ${DB_HOST}, sudo -u postgres psql, SQL on stdin)`,
      kind: 'postgres',
      async preview() { return roleSql({ ...principal, password: REDACTED, roleExists: await roleExists() }); },
      async apply() {
        await ctx.psql(roleSql({ ...principal, password: ctx.secrets.password, roleExists: await roleExists() }), { label: 'create role and principal' });
        return `role ${role} and its principal row are in place`;
      },
    },
    hbaStep(ctx, 'add'),
    {
      title: `rustfs: user and policy ${rustfsName(host)} (mc on this machine, root credentials only in the child's environment)`,
      kind: 'rustfs',
      preview: async () => [`mc admin user add ${MC_ALIAS} <access key> ${REDACTED}`, `mc admin policy create ${MC_ALIAS} ${rustfsName(host)} <policy file>`, `mc admin policy attach ${MC_ALIAS} ${rustfsName(host)} --user <access key>`, rustfsPolicy(host).trimEnd()].join('\n'),
      async apply() {
        const { accessKey, secretKey } = ctx.secrets;
        // mc has no stdin form for `user add`: the secret is briefly visible in this machine's process list.
        await ctx.mc(['admin', 'user', 'add', MC_ALIAS, accessKey, secretKey]);
        await ctx.withPrivateDir(async (dir) => {
          const file = path.join(dir, 'policy.json');
          ctx.fs.writeFileSync(file, rustfsPolicy(host), { mode: 0o600 });
          await ctx.mc(['admin', 'policy', 'create', MC_ALIAS, rustfsName(host), file]);
        });
        await ctx.mc(['admin', 'policy', 'attach', MC_ALIAS, rustfsName(host), '--user', accessKey], { allow: /already/i });
        return `user and policy ${rustfsName(host)} are in place`;
      },
    },
    trainStep(ctx, 'add'),
  ];
  if (out) steps.push(workerFilesStep(ctx, out));
  return steps;
}

function workerFilesStep(ctx, out) {
  const role = pgRole(ctx.opts.host);
  return {
    title: `worker files in ${out} (dir 0700, files 0600): config.json, password, rustfs.env, ca.pem`,
    kind: 'worker-files',
    preview: async () => `config.json:\n${workerConfig(ctx.opts.host, role).trimEnd()}\npassword and rustfs.env: from the 1Password item (not shown)\nca.pem: ssh ${DB_HOST} sudo -n cat ${CA_PATH}`,
    async apply() {
      const ca = (await ctx.remote(['sudo', '-n', 'cat', CA_PATH], { label: 'read ca.pem' })).stdout;
      const { password, accessKey, secretKey } = ctx.secrets;
      ctx.fs.mkdirSync(out, { recursive: true, mode: 0o700 });
      ctx.fs.chmodSync(out, 0o700);
      const files = {
        'config.json': workerConfig(ctx.opts.host, role),
        password,
        'rustfs.env': `AWS_ACCESS_KEY_ID=${accessKey}\nAWS_SECRET_ACCESS_KEY=${secretKey}\n`,
        'ca.pem': ca,
      };
      for (const [name, content] of Object.entries(files)) {
        ctx.fs.writeFileSync(path.join(out, name), content, { mode: 0o600 });
        ctx.fs.chmodSync(path.join(out, name), 0o600);
      }
      return `wrote ${Object.keys(files).join(', ')} to ${out}; copy them to ~/.config/lane-agent/ on ${ctx.opts.host}`;
    },
  };
}

const parseTickets = (rows) => rows.map(([id, lane]) => `${id} (${lane})`);

function buildRemovePlan(ctx) {
  const { host, force } = ctx.opts;
  const role = pgRole(host);
  ctx.hbaEntry = { role, host };
  const held = () => ctx.psqlRows(heldTicketsSql(host), 'list held tickets').then(parseTickets);
  const tolerate = /not found|does not exist|doesn't exist|no such/i;
  const accessKeyOf = async () => (await ctx.getItem())?.accessKey ?? null;
  return [
    {
      title: `check ${host} holds no claimed or running tickets${force ? ' (--force: proceed regardless)' : ''}`,
      kind: 'check',
      async preview() {
        const tickets = await held();
        return tickets.length ? `${tickets.length} held: ${tickets.join(', ')}${force ? '' : '\nremove would refuse without --force'}` : 'none held';
      },
      async apply() {
        const tickets = await held();
        if (tickets.length && !force) throw new Error(`${host} still holds ${tickets.length} claimed or running ticket(s): ${tickets.join(', ')}; re-run with --force to remove it anyway`);
        return tickets.length ? `forced past ${tickets.length} held ticket(s): ${tickets.join(', ')}` : 'none held';
      },
    },
    {
      title: `postgres: delete principal and runners rows, terminate sessions, drop role ${role}`,
      kind: 'postgres',
      preview: async () => removeSql({ role, host }),
      async apply() { await ctx.psql(removeSql({ role, host }), { label: 'drop role' }); return `role ${role} dropped`; },
    },
    hbaStep(ctx, 'remove'),
    {
      title: `rustfs: detach policy, remove user and policy ${rustfsName(host)}`,
      kind: 'rustfs',
      preview: async () => [`mc admin policy detach ${MC_ALIAS} ${rustfsName(host)} --user <access key from 1Password>`, `mc admin user remove ${MC_ALIAS} <access key>`, `mc admin policy remove ${MC_ALIAS} ${rustfsName(host)}`].join('\n'),
      async apply() {
        const accessKey = await accessKeyOf();
        if (accessKey) {
          await ctx.mc(['admin', 'policy', 'detach', MC_ALIAS, rustfsName(host), '--user', accessKey], { allow: tolerate });
          await ctx.mc(['admin', 'user', 'remove', MC_ALIAS, accessKey], { allow: tolerate });
        }
        await ctx.mc(['admin', 'policy', 'remove', MC_ALIAS, rustfsName(host)], { allow: tolerate });
        return accessKey ? 'detached, removed' : `no 1Password item, so the user's access key is unknown: user not removed, policy removed`;
      },
    },
    trainStep(ctx, 'remove'),
    {
      title: `1Password: leave "${opItemTitle(host)}" in place`,
      kind: 'note',
      preview: async () => `archive it once the host is retired: op-bot item delete "${opItemTitle(host)}" --vault ${OP_VAULT} --archive`,
      apply: async () => `left in place; archive it with: op-bot item delete "${opItemTitle(host)}" --vault ${OP_VAULT} --archive`,
    },
  ];
}

export async function runPlan(steps, { apply, out, resume }) {
  const done = [];
  for (const step of steps) {
    out(`[${step.kind}] ${step.title}`);
    try {
      const detail = apply ? await step.apply() : await step.preview();
      if (detail) out(String(detail).split('\n').map((line) => `    ${line}`).join('\n'));
      done.push(step.title);
    } catch (err) {
      out(`    FAILED: ${err.message}`);
      out(`\n${apply ? 'completed' : 'previewed'} ${done.length} of ${steps.length} steps before the failure:`);
      for (const title of done) out(`  ok  ${title}`);
      if (apply) out(`resume (every step converges): ${resume}`);
      return { ok: false, done };
    }
  }
  out(apply ? `\napplied ${done.length} steps.` : `\ndry run: ${done.length} steps shown, nothing changed. Re-run with --apply to perform them.`);
  return { ok: true, done };
}

async function statusReport(ctx, out) {
  const { host } = ctx.opts;
  const role = pgRole(host);
  const line = (name, value) => out(`${name.padEnd(22)} ${value}`);
  line('postgres role', Number((await ctx.psql(roleExistsSql(role), { query: true, label: 'check role' })).stdout.trim()) > 0 ? `present (${role})` : `absent (${role})`);
  const principal = (await ctx.psqlRows(`SELECT login_role, host, kind, tags, allowed_repos, max_claims FROM lane_dispatch.principals WHERE login_role = ${sqlLit(role)};\n`, 'read principal'))[0];
  line('principal row', principal ? `host=${principal[1]} kind=${principal[2]} tags=${principal[3]} repos=${principal[4]} max_claims=${principal[5]}` : 'absent');
  const runner = (await ctx.psqlRows(`SELECT host, version, last_seen FROM lane_dispatch.runners WHERE host = ${sqlLit(host)};\n`, 'read runner'))[0];
  line('runners row', runner ? `version=${runner[1]} last_seen=${runner[2]}` : 'absent');
  const hba = hbaState((await ctx.remote(['sudo', '-n', 'cat', HBA_PATH], { label: 'read pg_hba.conf' })).stdout, { role, host });
  line('pg_hba line', hba.line ? `present: ${hba.line}${hba.block ? '' : ' (not in a lane-runner-admin block)'}` : 'absent');
  line('pg_hba reject lists', `local all: ${hba.localRejected ? 'listed' : 'not listed'}, host all: ${hba.hostRejected ? 'listed' : 'not listed'}`);
  const item = await ctx.getItem();
  line('1Password item', item ? `present (${opItemTitle(host)})` : 'absent');
  const user = item ? await ctx.mc(['admin', 'user', 'info', MC_ALIAS, item.accessKey], { allow: /^/ }) : null;
  line('rustfs user', user ? (user.code === 0 ? 'present' : 'absent') : 'unknown (no 1Password item holds its access key)');
  line('rustfs policy', (await ctx.mc(['admin', 'policy', 'info', MC_ALIAS, rustfsName(host)], { allow: /^/ })).code === 0 ? `present (${rustfsName(host)})` : 'absent');
  const train = (await ctx.remote(['sudo', '-n', '-u', 'train', 'cat', TRAIN_CONFIG_PATH], { label: 'read train config' })).stdout;
  line('train runners entry', trainHasRunner(train, host) ? 'present' : 'absent');
  const tickets = parseTickets(await ctx.psqlRows(heldTicketsSql(host), 'list held tickets'));
  line('claimed/running', tickets.length ? tickets.join(', ') : 'none');
}

const USAGE = `usage:
  lane-runner-admin add <host> --ip <tailnet IPv4> [--tags a,b] [--repos '*'|a,b] [--max-claims N] [--out <dir>] [--note <text>] [--apply]
  lane-runner-admin remove <host> [--force] [--apply]
  lane-runner-admin status <host>
Dry run unless --apply is given.`;

const csv = (value, fallback) => (value === undefined ? fallback : value.split(',').map((s) => s.trim()).filter(Boolean));

export function parseCli(argv) {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      ip: { type: 'string' }, tags: { type: 'string' }, repos: { type: 'string' }, 'max-claims': { type: 'string' },
      out: { type: 'string' }, note: { type: 'string' }, apply: { type: 'boolean' }, force: { type: 'boolean' },
    },
  });
  const [command, host, ...rest] = positionals;
  if (!['add', 'remove', 'status'].includes(command) || rest.length) throw new Error(USAGE);
  validateHost(host);
  const opts = { command, host, apply: values.apply === true, force: values.force === true };
  if (command !== 'add') {
    const stray = ['ip', 'tags', 'repos', 'max-claims', 'out', 'note'].filter((name) => values[name] !== undefined);
    if (stray.length) throw new Error(`--${stray[0]} only applies to add`);
    return opts;
  }
  if (values.force) throw new Error('--force only applies to remove');
  validateTailnetIp(values.ip);
  const tags = csv(values.tags, []);
  const repos = csv(values.repos, ['*']);
  for (const item of [...tags, ...repos]) if (!LIST_ITEM_RE.test(item)) throw new Error(`invalid tag or repo ${JSON.stringify(item)}`);
  const maxClaims = values['max-claims'] === undefined ? 1 : Number(values['max-claims']);
  if (!Number.isInteger(maxClaims) || maxClaims < 0) throw new Error(`--max-claims must be a non-negative integer, not ${values['max-claims']}`);
  if (/[\r\n]/.test(values.note ?? '')) throw new Error('--note must be a single line');
  return { ...opts, ip: values.ip, tags, repos, maxClaims, out: values.out ? path.resolve(values.out) : null, note: values.note ?? '' };
}

export async function runCli(argv, { exec, out = (text) => process.stdout.write(`${text}\n`), err = (text) => process.stderr.write(`${text}\n`), ...injected }) {
  let opts;
  try { opts = parseCli(argv); } catch (e) { err(e.message); return 2; }
  const ctx = new Context({ exec, opts, ...injected });
  try {
    if (opts.command === 'status') { await statusReport(ctx, out); return 0; }
    const steps = opts.command === 'add' ? buildAddPlan(ctx) : buildRemovePlan(ctx);
    const resume = `lane-runner-admin ${[...argv.filter((arg) => arg !== '--apply'), '--apply'].join(' ')}`;
    out(`${opts.command} ${opts.host}: ${opts.apply ? 'APPLY' : 'dry run'}`);
    const { ok } = await runPlan(steps, { apply: opts.apply, out: (text) => out(ctx.scrub(text)), resume });
    return ok ? 0 : 1;
  } catch (e) {
    err(ctx.scrub(e.message));
    return 1;
  }
}
