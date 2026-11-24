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
const NOTE_RE = /^[A-Za-z0-9 .,:;()~#+_/-]{0,120}$/;
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

export function validateNote(note) {
  if (!NOTE_RE.test(note)) throw new Error(`invalid --note ${JSON.stringify(note)}: at most 120 characters from [A-Za-z0-9 .,:;()~#+_/-] (a backslash would continue the pg_hba comment over the rule)`);
  return note;
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
    'BEGIN;',
    roleExists
      ? `ALTER ROLE ${sqlIdent(role)} LOGIN NOSUPERUSER NOREPLICATION NOBYPASSRLS NOCREATEDB NOCREATEROLE PASSWORD ${sqlLit(password)} CONNECTION LIMIT 3;`
      : `CREATE ROLE ${sqlIdent(role)} LOGIN NOSUPERUSER NOREPLICATION NOBYPASSRLS NOCREATEDB NOCREATEROLE PASSWORD ${sqlLit(password)} IN ROLE lane_agent CONNECTION LIMIT 3;`,
    `ALTER ROLE ${sqlIdent(role)} SET statement_timeout = '2s';`,
    `ALTER ROLE ${sqlIdent(role)} SET idle_in_transaction_session_timeout = '5s';`,
    `DO $lra$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = ${sqlLit(role)} AND (rolsuper OR rolreplication OR rolbypassrls OR rolcreatedb OR rolcreaterole)) THEN RAISE EXCEPTION 'role ${role} still holds an elevated attribute'; END IF; END $lra$;`,
    `SELECT lane_dispatch.ld_set_principal(${sqlLit(role)}, 'agent', ${sqlLit(host)}, ${sqlTextArray(tags)}, ${sqlTextArray(repos)}, ${Number(maxClaims)});`,
    'COMMIT;',
    '',
  ].join('\n');
}

export const roleAndPrincipalSql = (role) => `SELECT (SELECT count(*) FROM pg_roles WHERE rolname = ${sqlLit(role)}), (SELECT count(*) FROM lane_dispatch.principals WHERE login_role = ${sqlLit(role)});\n`;
export const roleExistsSql = (role) => `SELECT count(*) FROM pg_roles WHERE rolname = ${sqlLit(role)};\n`;
export const heldTicketsSql = (host) => `SELECT id, lane FROM lane_dispatch.tickets WHERE host = ${sqlLit(host)} AND state IN ('claimed','running') ORDER BY created_at;\n`;
export const reloadSql = () => 'SELECT pg_reload_conf();\n';

export function removeTxSql({ role, host, force }) {
  const held = `FROM lane_dispatch.tickets WHERE host = ${sqlLit(host)} AND state IN ('claimed','running')`;
  return [
    'BEGIN;',
    `SELECT 1 FROM lane_dispatch.principals WHERE login_role = ${sqlLit(role)} FOR UPDATE;`,
    ...(force ? [] : [`DO $lra$ BEGIN IF EXISTS (SELECT 1 ${held}) THEN RAISE EXCEPTION 'held tickets: %', (SELECT string_agg(id::text || ' (' || lane || ')', ', ' ORDER BY created_at) ${held}); END IF; END $lra$;`]),
    `DO $lra$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = ${sqlLit(role)}) THEN ALTER ROLE ${sqlIdent(role)} NOLOGIN; END IF; END $lra$;`,
    `DELETE FROM lane_dispatch.principals WHERE login_role = ${sqlLit(role)};`,
    `DELETE FROM lane_dispatch.runners WHERE host = ${sqlLit(host)};`,
    'COMMIT;',
    '',
  ].join('\n');
}

// The login is already NOLOGIN; once its sessions are confirmed gone (the timeout form waits), a registration that committed in that window is deleted again.
// The pids are selected first and terminated in a loop: pg_terminate_backend in a WHERE clause can run before the usename filter and kill other sessions.
export const dropRoleSql = (role, host) => [
  `DO $lra$ DECLARE target int; BEGIN FOR target IN SELECT pid FROM pg_stat_activity WHERE usename = ${sqlLit(role)} AND pid <> pg_backend_pid() LOOP IF NOT pg_terminate_backend(target, 10000) THEN RAISE EXCEPTION 'sessions of ${role} did not end within 10 s; re-run remove'; END IF; END LOOP; END $lra$;`,
  `DELETE FROM lane_dispatch.runners WHERE host = ${sqlLit(host)};`,
  `DROP ROLE IF EXISTS ${sqlIdent(role)};`,
  '',
].join('\n');

const MC_USER_MISSING = /The specified user does not exist|NoSuchUser|user .* not found/i;
const MC_POLICY_MISSING = /The canned policy does not exist|NoSuchPolicy|policy .* (?:does not exist|not found)/i;
const MC_EITHER_MISSING = new RegExp(`${MC_USER_MISSING.source}|${MC_POLICY_MISSING.source}`, 'i');

const BEGIN_MARK = (host) => `# BEGIN lane-runner-admin ${host}`;
const END_MARK = (host) => `# END lane-runner-admin ${host}`;
const BLOCK_ANCHOR = /^local\s+lane_queue_db\s+all\s+reject/;
const LOCAL_REJECT = /^(local\s+all\s+)(\S+)(\s+reject.*)$/;
const HOST_REJECT = /^(host\s+all\s+)(\S+)(\s+all\s+reject.*)$/;

const HBA_TYPES = new Set(['local', 'host', 'hostssl', 'hostnossl', 'hostgssenc', 'hostnogssenc']);

export function parseHbaRule(line) {
  const tokens = line.replace(/\s#.*$/, '').trim().split(/\s+/);
  if (tokens[0].startsWith('#') || !HBA_TYPES.has(tokens[0]) || tokens.length < 4) return null;
  const list = (field) => field.split(',').map((entry) => entry.replace(/^"(.*)"$/, '$1'));
  return { type: tokens[0], databases: list(tokens[1]), users: list(tokens[2]), method: tokens.at(-1) };
}

const rulesAllowing = (lines, role) => lines.filter((line) => {
  const rule = parseHbaRule(line);
  return rule && rule.method !== 'reject' && rule.users.includes(role);
});

const opaque = (entry) => entry.startsWith('/') || entry.startsWith('@');

// Only a reject line in the main file that editRejectList will extend (the lane-logins line) can protect a role that is added to it.
const laneRejectAt = (entries, pattern) => entries.findIndex(({ text, main }) => main && pattern.exec(text)?.[2].split(',').some((r) => r.startsWith('lane_agent_')));

const INCLUDE_RE = /^\s*(include|include_if_exists|include_dir)\s+(\S+)/;
const includeTarget = (raw) => raw.replace(/^'(.*)'$/, '$1').replace(/^"(.*)"$/, '$1');

export function hbaIncludes(text) {
  return text.split('\n').flatMap((line) => {
    const match = INCLUDE_RE.exec(line);
    return match ? [{ kind: match[1], path: includeTarget(match[2]) }] : [];
  });
}

// The rules pg_hba evaluates, in order, each include spliced in where it appears. This is only the dry-run early warning: the
// edit script asks Postgres's own parse of the files on disk at commit time. Constructs this cannot read are refused.
function evaluatedRules(lines, included) {
  const out = [];
  const check = (line, where) => {
    if (/\\\s*$/.test(line)) throw new Error(`${where} has a line ending in a backslash (a continuation), which this tool cannot check: "${line.trim()}"; join it first`);
    if (!/^\s*#/.test(line) && line.includes('"')) throw new Error(`${where} has a quoted field, which this tool cannot check: "${line.trim()}"`);
    if (!/^\s*#/.test(line) && /(^|[\s,])@/.test(line)) throw new Error(`${where} has an @file list, which this tool cannot check: "${line.trim()}"`);
  };
  for (const line of lines) {
    const match = INCLUDE_RE.exec(line);
    if (!match) { check(line, 'pg_hba.conf'); out.push({ text: line, main: true }); continue; }
    if (match[1] === 'include_dir') throw new Error(`pg_hba.conf uses include_dir (${match[2]}), which this tool cannot check`);
    const body = included?.[includeTarget(match[2])];
    if (body === undefined) throw new Error(`pg_hba.conf includes ${match[2]}, which was not read`);
    for (const inner of body.split('\n')) {
      if (INCLUDE_RE.test(inner)) throw new Error(`${match[2]} has a nested include, which this tool cannot check`);
      check(inner, match[2]);
      out.push({ text: inner, main: false });
    }
  }
  return out;
}

// A rule that might admit `role` before the main file's lane-logins reject line of its kind: first match wins, so a broad rule
// below that line is unreachable for a listed lane login. A rule naming the role itself is refused wherever it is.
const rulesThatMayAdmit = (rawLines, role, included) => {
  const entries = evaluatedRules(rawLines, included);
  const cutoff = { local: laneRejectAt(entries, LOCAL_REJECT), host: laneRejectAt(entries, HOST_REJECT) };
  return entries.filter(({ text }, i) => {
    const rule = parseHbaRule(text);
    if (!rule || rule.method === 'reject') return false;
    const database = rule.databases.some((name) => [DB_NAME, 'all', 'sameuser', 'samerole', 'samegroup'].includes(name) || opaque(name));
    if (!database) return false;
    if (rule.users.includes(role)) return true;
    const broad = rule.users.some((name) => name === 'all' || name.startsWith('+') || opaque(name));
    const reject = cutoff[rule.type === 'local' ? 'local' : 'host'];
    return broad && (reject < 0 || i < reject);
  }).map(({ text }) => text);
};

function hbaBlock({ role, host, ip, note }) {
  if (note) validateNote(note);
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

export function editHba(text, { role, host, ip, note }, mode, { included = {} } = {}) {
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
    const [clash] = rulesThatMayAdmit(lines, role, included);
    if (clash) throw new Error(`pg_hba.conf already has a rule outside the ${BEGIN_MARK(host)} block that may let ${role} in: "${clash.trim()}"; narrow or remove it first`);
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
    line: rulesAllowing(lines, role)[0] ?? null,
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

const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

const secretForms = (value) => [
  value, value.replaceAll("'", "''"), encodeURIComponent(value), JSON.stringify(value).slice(1, -1),
  Buffer.from(value).toString('base64'), Buffer.from(value).toString('base64url'),
];

class Context {
  constructor({ exec, random = defaultRandom, fsImpl = fs, opts }) {
    Object.assign(this, { exec, random, fs: fsImpl, opts, secrets: null, rootCreds: null });
    this.known = new Set();
  }

  learn(...values) { for (const value of values) if (value) for (const form of secretForms(value)) this.known.add(form); }

  scrub(text) {
    let clean = String(text);
    for (const secret of [...this.known].sort((a, b) => b.length - a.length)) clean = clean.replaceAll(secret, REDACTED);
    return clean;
  }

  assertNoSecrets(text, what) {
    if ([...this.known].some((secret) => text.includes(secret))) throw new Error(`${what} would carry a secret; refusing to write it`);
  }

  // `quiet` is for calls whose input carries a secret: the child's output is never forwarded, only its exit code and SQLSTATE.
  async run(cmd, args, { input, env, label, allow, quiet = false } = {}) {
    const result = await this.exec(cmd, args, { input, env });
    if (result.code !== 0 && !allow?.test(`${result.stderr}${result.stdout}`)) {
      if (quiet) {
        const sqlstate = /ERROR:\s+([0-9A-Z]{5})\s*$/m.exec(result.stderr)?.[1];
        throw new Error(`${label ?? cmd} failed (exit ${result.code}${sqlstate ? `, SQLSTATE ${sqlstate}` : ''}; output withheld because the call carries a secret)`);
      }
      throw new Error(`${label ?? cmd} failed (exit ${result.code}): ${this.scrub(result.stderr || result.stdout).trim()}`);
    }
    return result;
  }

  remote(args, opts) { return this.run('ssh', [DB_HOST, args.map(shellQuote).join(' ')], opts); }

  psql(sql, { query = false, quiet = false, label = 'psql' } = {}) {
    const flags = query ? ['-X', '-tAq', '-F', '|'] : quiet ? ['-v', 'VERBOSITY=sqlstate'] : [];
    return this.remote(['sudo', '-n', '-u', 'postgres', 'psql', '-d', DB_NAME, '-v', 'ON_ERROR_STOP=1', ...flags], { input: sql, label, quiet });
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
      await this.run('op-bot', ['item', 'create', '--vault', OP_VAULT, '--template', file], { label: 'op-bot item create', quiet: true });
    });
    const title = opItemTitle(this.opts.host);
    const same = JSON.parse((await this.run('op-bot', ['item', 'list', '--vault', OP_VAULT, '--format', 'json'], { label: 'op-bot item list' })).stdout).filter((item) => item.title === title);
    if (same.length !== 1) throw new Error(`${same.length} 1Password items titled "${title}" exist (ids ${same.map((item) => item.id).join(', ')}): another admin run raced this one. Delete the extra by hand, then re-run`);
    this.secrets = await this.getItem();
    return 'created the item with generated values';
  }
}

const LOCK_FILE = '/run/lock/lane-runner-admin.lock';

// Postgres's own parse of the files on disk (includes, quoting and continuations resolved) decides whether the edit is safe.
// psql variables: :'role' :'ip' :'db'. Returns 'ok' or one reason naming file:line.
export const HBA_ADD_INVARIANT_SQL = `WITH r AS (
  SELECT rule_number, file_name, line_number, type, database, user_name, address, netmask, auth_method,
    type = 'local' AS is_local,
    (user_name && ARRAY[:'role', 'all'] OR EXISTS (SELECT 1 FROM unnest(user_name) u WHERE u LIKE '+%' OR u LIKE '/%')) AS may,
    (auth_method = 'reject' AND database && ARRAY['all'] AND user_name && ARRAY[:'role', 'all']
      AND (type = 'local' OR (type = 'host' AND (address IS NULL OR address = 'all')))) AS barrier,
    (type = 'hostssl' AND user_name = ARRAY[:'role'] AND database = ARRAY[:'db'] AND address = :'ip' AND netmask = '255.255.255.255' AND auth_method = 'scram-sha-256') AS ours,
    (database && ARRAY[:'db', 'all', 'sameuser', 'samerole', 'samegroup'] OR EXISTS (SELECT 1 FROM unnest(database) d WHERE d LIKE '/%')) AS db_may,
    format('%s %s %s %s', type, array_to_string(database, ','), array_to_string(user_name, ','), auth_method) AS what
  FROM pg_hba_file_rules WHERE error IS NULL
), m AS (
  SELECT * FROM r WHERE may
), c AS (
  SELECT is_local, min(rule_number) AS rn FROM m WHERE barrier GROUP BY is_local
), pre AS (
  SELECT m.* FROM m JOIN c USING (is_local) WHERE m.rule_number < c.rn
)
SELECT coalesce(
  (SELECT format('%s:%s: rule error: %s', file_name, line_number, error) FROM pg_hba_file_rules WHERE error IS NOT NULL ORDER BY rule_number LIMIT 1),
  (SELECT format('no catch-all reject covers %s over TCP', :'role') WHERE NOT EXISTS (SELECT 1 FROM c WHERE NOT is_local)),
  (SELECT format('%s:%s: a rule before the catch-all reject may admit %s: %s', file_name, line_number, :'role', what) FROM pre WHERE NOT is_local AND auth_method <> 'reject' AND NOT ours ORDER BY rule_number LIMIT 1),
  (SELECT format('no hostssl line for %s before the catch-all reject', :'role') WHERE (SELECT count(*) FROM pre WHERE NOT is_local AND ours) <> 1),
  (SELECT format('%s:%s: a reject before the hostssl line of %s would block it: %s', file_name, line_number, :'role', what) FROM pre
   WHERE NOT is_local AND auth_method = 'reject' AND db_may AND rule_number < (SELECT min(rule_number) FROM pre WHERE NOT is_local AND ours) ORDER BY rule_number LIMIT 1),
  (SELECT format('no catch-all reject covers %s on local connections', :'role') WHERE NOT EXISTS (SELECT 1 FROM c WHERE is_local)),
  (SELECT format('%s:%s: a local rule before the catch-all reject may admit %s: %s', file_name, line_number, :'role', what) FROM pre WHERE is_local AND auth_method <> 'reject' ORDER BY rule_number LIMIT 1),
  'ok');
`;

export const HBA_REMOVE_INVARIANT_SQL = `SELECT coalesce(
  (SELECT format('%s:%s: rule error: %s', file_name, line_number, error) FROM pg_hba_file_rules WHERE error IS NOT NULL ORDER BY rule_number LIMIT 1),
  (SELECT format('%s:%s: a rule other than reject still names %s', file_name, line_number, :'role') FROM pg_hba_file_rules WHERE error IS NULL AND auth_method <> 'reject' AND user_name && ARRAY[:'role'] ORDER BY rule_number LIMIT 1),
  'ok');
`;

export const listFilesCommand = (psql) => `${psql} -X -At -c "SELECT DISTINCT file_name FROM pg_hba_file_rules ORDER BY 1"`;

export const invariantFunctionBody = (psql) => `  if [ "$action" = add ]; then
    ${psql} -X -At -v ON_ERROR_STOP=1 -v "role=$role" -v "ip=$ip" -v "db=$db" -f - <<'LRA_SQL_ADD'
${HBA_ADD_INVARIANT_SQL}LRA_SQL_ADD
  else
    ${psql} -X -At -v ON_ERROR_STOP=1 -v "role=$role" -v "ip=$ip" -v "db=$db" -f - <<'LRA_SQL_REMOVE'
${HBA_REMOVE_INVARIANT_SQL}LRA_SQL_REMOVE
  fi`;

const SCRIPT_HOOKS = {
  sha: 'sha256sum',
  chown: 'chown',
  invariant: invariantFunctionBody('sudo -n -u postgres psql'),
  files: listFilesCommand('sudo -n -u postgres psql'),
  reload: `sudo -n -u postgres psql -At -c 'SELECT pg_reload_conf()'`,
  jsonCheck: `python3 -c 'import json,sys; json.load(open(sys.argv[1]))'`,
};

// Runs as root under `flock ... bash -s -- <path> <expected-sha256> <owner:group> <mode> <kind> [<add|remove> <role> <ip> <db>]`; `hooks` lets a test run it on a laptop.
export function buildEditScript({ content, hooks = {} }) {
  const h = { ...SCRIPT_HOOKS, ...hooks };
  const payload = Buffer.from(content).toString('base64').replace(/(.{76})/g, '$1\n');
  return `set -euo pipefail
path=$1; expect=$2; owner=$3; mode=$4; kind=$5
action=\${6:-}; role=\${7:-}; ip=\${8:-}; db=\${9:-}
want=${sha256(content)}
sha() { ${h.sha} "$1" | cut -d' ' -f1; }
listfiles() { ${h.files}; }
invariant() {
${h.invariant}
}
payload() { base64 -d <<'LRA_PAYLOAD_EOF'
${payload}
LRA_PAYLOAD_EOF
}
stage=; backup=; moved=0; ok=0
cleanup() {
  rc=$?
  trap - EXIT
  if [ "$ok" != 1 ]; then
    [ -z "$stage" ] || rm -f "$stage"
    if [ "$moved" = 1 ]; then
      restore="$path.restore.$$"
      cp -p "$backup" "$restore" && mv -f "$restore" "$path" && echo "restored $backup" >&2 || echo "RESTORE FAILED: copy $backup over $path by hand" >&2
    fi
  fi
  exit $rc
}
trap cleanup EXIT
[ "$(sha "$path")" = "$expect" ] || { echo "changed since read: $path" >&2; exit 3; }
stage=$(mktemp "$path.stage.XXXXXX")
payload > "$stage"
[ "$(sha "$stage")" = "$want" ] || { echo "payload damaged in transit" >&2; exit 4; }
if [ "$want" != "$expect" ]; then
  ${h.chown} "$owner" "$stage"
  chmod "$mode" "$stage"
  backup="$path.bak-lane-runner-admin-$(date -u +%Y%m%dT%H%M%S%NZ)"
  [ ! -e "$backup" ] || { echo "backup name exists: $backup" >&2; exit 5; }
  cp -p "$path" "$backup"
  mv -f "$stage" "$path"
  stage=; moved=1
else
  rm -f "$stage"; stage=
fi
case "$kind" in
  hba)
    [ "$action" = add ] || [ "$action" = remove ] || { echo "hba edits need an action of add or remove" >&2; exit 2; }
    files=$(listfiles) || { echo "could not list the files pg_hba_file_rules read" >&2; exit 6; }
    [ -n "$files" ] || { echo "pg_hba_file_rules lists no files" >&2; exit 6; }
    while IFS= read -r f; do
      [ -r "$f" ] || { echo "pg_hba invariant violated: cannot read $f" >&2; exit 6; }
      if grep -v '^[[:space:]]*#' "$f" | grep '"' > /dev/null; then echo "pg_hba invariant violated: $f has a quoted field, which pg_hba_file_rules cannot tell from a keyword" >&2; exit 6; fi
      if grep -v '^[[:space:]]*#' "$f" | grep -E '(^|[[:space:],])@' > /dev/null; then echo "pg_hba invariant violated: $f has an @file list, whose quoting pg_hba_file_rules cannot show" >&2; exit 6; fi
    done <<LRA_FILES_EOF
$files
LRA_FILES_EOF
    verdict=$(invariant | tr -d '\r') || verdict="the invariant query failed"
    [ "$verdict" = ok ] || { echo "pg_hba invariant violated: $verdict" >&2; exit 6; } ;;
  json) ${h.jsonCheck} "$path" || { echo "not valid JSON: $path" >&2; exit 6; } ;;
  *) echo "unknown kind $kind" >&2; exit 2 ;;
esac
ok=1
if [ "$kind" = hba ]; then
  reloaded=$({ ${h.reload}; } 2>/dev/null | tr -d '[:space:]') || reloaded=
  [ "$reloaded" = t ] || { echo "reload failed: the file is valid but not live; re-run to reload" >&2; exit 7; }
fi
if [ "$moved" = 1 ]; then echo "backup=$backup"; else echo "unchanged=$path"; fi
`;
}

async function editRemoteFile(ctx, { kind, path: file, readAs = [], edit, label, beforeEdit, scriptArgs = [] }) {
  const before = (await ctx.remote(['sudo', '-n', ...readAs, 'cat', file], { label: `read ${label}` })).stdout;
  if (beforeEdit) await beforeEdit(before);
  const after = edit(before);
  return {
    before, after, file, kind, changed: after !== before,
    async apply() {
      ctx.assertNoSecrets(after, label);
      const stat = (await ctx.remote(['sudo', '-n', 'stat', '-c', '%U:%G %a', file], { label: `stat ${label}` })).stdout.trim();
      const [owner, mode] = stat.split(' ');
      if (!/^[\w.-]+:[\w.-]+$/.test(owner ?? '') || !/^[0-7]{3,4}$/.test(mode ?? '')) throw new Error(`unreadable owner and mode for ${file}: ${JSON.stringify(stat)}`);
      const result = await ctx.remote(['sudo', '-n', 'flock', LOCK_FILE, 'bash', '-s', '--', file, sha256(before), owner, mode, kind, ...scriptArgs], { input: buildEditScript({ content: after }), label: `edit ${label}`, allow: /^/ });
      if (result.code === 3) throw new Error(`${label} changed on ${DB_HOST} since it was read; nothing was written. Re-run to plan against the new content`);
      if (result.code !== 0) throw new Error(`${label} edit failed (exit ${result.code}): ${ctx.scrub(result.stderr).trim()}`);
      return result.stdout.match(/^backup=(.*)$/m)?.[1] ?? null;
    },
  };
}

async function readIncludes(ctx, text) {
  const included = {};
  for (const { kind, path: target } of hbaIncludes(text)) {
    if (kind === 'include_dir') continue;
    const file = path.posix.isAbsolute(target) ? target : path.posix.join(path.posix.dirname(HBA_PATH), target);
    const result = await ctx.remote(['sudo', '-n', 'cat', file], { label: `read ${file}`, allow: kind === 'include_if_exists' ? /No such file/ : undefined });
    included[target] = result.code === 0 ? result.stdout : '';
  }
  return included;
}

async function hbaEdit(ctx, mode) {
  let included = {};
  const { role, ip } = ctx.hbaEntry;
  if (!/^lane_agent_[a-z0-9_]+$/.test(role)) throw new Error(`refusing to check pg_hba.conf for the unexpected role ${JSON.stringify(role)}`);
  if (mode === 'add') validateTailnetIp(ip);
  const edit = editRemoteFile(ctx, { kind: 'hba', path: HBA_PATH, label: 'pg_hba.conf', scriptArgs: [mode, role, mode === 'add' ? ip : '-', DB_NAME], beforeEdit: async (text) => { included = await readIncludes(ctx, text); }, edit: (text) => editHba(text, ctx.hbaEntry, mode, { included }) });
  return edit;
}

const trainEdit = (ctx, mode) => editRemoteFile(ctx, { kind: 'json', path: TRAIN_CONFIG_PATH, readAs: ['-u', 'train'], label: 'train config', edit: (text) => editTrainRunners(text, ctx.opts.host, mode) });

const PROVISION_REMINDER = 'agent_brain/ops/pitelite-controller/provision.sh writes this file: a re-provision drops the runner unless it is added there too.';

function editStep(ctx, { title, kind, load, extra }) {
  const tail = extra ? `\n${extra}` : '';
  return {
    title,
    kind,
    async preview() {
      const edit = await load();
      const note = edit.kind === 'hba' ? `\nthen validate with pg_hba_file_rules and ${reloadSql().trim()}` : '';
      return `${edit.changed ? unifiedDiff(edit.before, edit.after, edit.file) : `${edit.file}: no change`}${note}${tail}`;
    },
    async apply() {
      const edit = await load();
      if (!edit.changed && edit.kind !== 'hba') return `${edit.file}: already converged${tail}`;
      const backup = await edit.apply();
      return `${backup ? `backed up to ${backup}` : `${edit.file}: already converged, validated and reloaded`}${tail}`;
    },
  };
}

const hbaStep = (ctx, mode) => editStep(ctx, { title: `pg_hba.conf: ${mode} the ${ctx.hbaEntry.role} block and reject-list entries`, kind: 'hba', load: () => hbaEdit(ctx, mode) });

const trainStep = (ctx, mode) => editStep(ctx, { title: `train config: ${mode} runner ${ctx.opts.host}`, kind: 'train', load: () => trainEdit(ctx, mode), extra: `note: ${PROVISION_REMINDER}` });

export const LOCK_ENV = 'LANE_RUNNER_ADMIN_LOCKED';
export const LOCK_CONFLICT_EXIT = 75;

// --apply re-runs itself under flock(1): the kernel holds the lock for the process's lifetime, so a dead run never leaves a stale lock.
export function lockedReexec({ argv, host, env = process.env, execPath = process.execPath, scriptPath = process.argv[1], dir = path.join(os.homedir(), '.cache', 'lane-runner-admin') }) {
  if (env[LOCK_ENV] === host) return null;
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  return { cmd: 'flock', args: ['-n', '-E', String(LOCK_CONFLICT_EXIT), path.join(dir, `${host}.lock`), execPath, scriptPath, ...argv], env: { ...env, [LOCK_ENV]: host } };
}

function recheckRole(ctx, role) {
  return async () => {
    const [row] = await ctx.psqlRows(roleAndPrincipalSql(role), 'recheck role and principal');
    if (row?.join('|') !== '1|1') throw new Error(`the role was removed while this add ran (${role} or its principal row is gone); re-run add`);
  };
}

const guarded = (step, precondition) => ({ ...step, async apply() { await precondition(); return step.apply(); } });

function buildAddPlan(ctx) {
  const { host, ip, out } = ctx.opts;
  const role = pgRole(host);
  const principal = { role, host, tags: ctx.opts.tags, repos: ctx.opts.repos, maxClaims: ctx.opts.maxClaims };
  ctx.hbaEntry = { role, host, ip, note: ctx.opts.note };
  const stillThere = recheckRole(ctx, role);
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
        await ctx.psql(roleSql({ ...principal, password: ctx.secrets.password, roleExists: await roleExists() }), { label: 'create role and principal', quiet: true });
        return `role ${role} and its principal row are in place`;
      },
    },
    guarded(hbaStep(ctx, 'add'), stillThere),
    guarded({
      title: `rustfs: user and policy ${rustfsName(host)} (mc on this machine, root credentials only in the child's environment)`,
      kind: 'rustfs',
      preview: async () => [`mc admin user add ${MC_ALIAS} <access key> ${REDACTED}`, `mc admin policy create ${MC_ALIAS} ${rustfsName(host)} <policy file>`, `mc admin policy attach ${MC_ALIAS} ${rustfsName(host)} --user <access key>`, rustfsPolicy(host).trimEnd()].join('\n'),
      async apply() {
        const { accessKey, secretKey } = ctx.secrets;
        // mc has no stdin form for `user add`: the secret is briefly visible in this machine's process list.
        await ctx.mc(['admin', 'user', 'add', MC_ALIAS, accessKey, secretKey], { quiet: true });
        await ctx.withPrivateDir(async (dir) => {
          const file = path.join(dir, 'policy.json');
          ctx.fs.writeFileSync(file, rustfsPolicy(host), { mode: 0o600 });
          await ctx.mc(['admin', 'policy', 'create', MC_ALIAS, rustfsName(host), file]);
        });
        await ctx.mc(['admin', 'policy', 'attach', MC_ALIAS, rustfsName(host), '--user', accessKey], { allow: /already/i });
        return `user and policy ${rustfsName(host)} are in place`;
      },
    }, stillThere),
    guarded(trainStep(ctx, 'add'), stillThere),
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
      title: `postgres: in one transaction lock the principal, re-check held tickets, set NOLOGIN, delete the principal and runners rows; then terminate sessions and drop role ${role}`,
      kind: 'postgres',
      preview: async () => `${removeTxSql({ role, host, force })}\n${dropRoleSql(role, host)}`,
      async apply() {
        try {
          await ctx.psql(removeTxSql({ role, host, force }), { label: 'disable role and delete principal' });
        } catch (err) {
          const raced = /held tickets: (.*)/.exec(err.message)?.[1];
          if (raced) throw new Error(`${host} claimed ticket(s) while it was being removed: ${raced.trim()}; nothing was changed. Re-run with --force to remove it anyway`);
          throw err;
        }
        await ctx.psql(dropRoleSql(role, host), { label: 'drop role' });
        return `role ${role} disabled, its rows deleted and the role dropped`;
      },
    },
    hbaStep(ctx, 'remove'),
    {
      title: `rustfs: detach policy, remove user and policy ${rustfsName(host)}`,
      kind: 'rustfs',
      preview: async () => [`mc admin policy detach ${MC_ALIAS} ${rustfsName(host)} --user <access key from 1Password>`, `mc admin user remove ${MC_ALIAS} <access key>`, `mc admin policy remove ${MC_ALIAS} ${rustfsName(host)}`].join('\n'),
      async apply() {
        const accessKey = await accessKeyOf();
        if (accessKey) {
          await ctx.mc(['admin', 'policy', 'detach', MC_ALIAS, rustfsName(host), '--user', accessKey], { allow: MC_EITHER_MISSING });
          await ctx.mc(['admin', 'user', 'remove', MC_ALIAS, accessKey], { allow: MC_USER_MISSING });
        }
        await ctx.mc(['admin', 'policy', 'remove', MC_ALIAS, rustfsName(host)], { allow: MC_POLICY_MISSING });
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
  const failures = [];
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
  const presence = (result, name, missing) => {
    if (result.code === 0) return `present${name ? ` (${name})` : ''}`;
    if (missing.test(`${result.stderr}${result.stdout}`)) return 'absent';
    failures.push(name ?? 'rustfs user');
    return `unknown (mc failed, exit ${result.code}: ${ctx.scrub(result.stderr || result.stdout).trim().split('\n')[0]})`;
  };
  const user = item ? await ctx.mc(['admin', 'user', 'info', MC_ALIAS, item.accessKey], { allow: /^/ }) : null;
  line('rustfs user', user ? presence(user, undefined, MC_USER_MISSING) : 'unknown (no 1Password item holds its access key)');
  line('rustfs policy', presence(await ctx.mc(['admin', 'policy', 'info', MC_ALIAS, rustfsName(host)], { allow: /^/ }), rustfsName(host), MC_POLICY_MISSING));
  const train = (await ctx.remote(['sudo', '-n', '-u', 'train', 'cat', TRAIN_CONFIG_PATH], { label: 'read train config' })).stdout;
  line('train runners entry', trainHasRunner(train, host) ? 'present' : 'absent');
  const tickets = parseTickets(await ctx.psqlRows(heldTicketsSql(host), 'list held tickets'));
  line('claimed/running', tickets.length ? tickets.join(', ') : 'none');
  return failures;
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
  validateNote(values.note ?? '');
  return { ...opts, ip: values.ip, tags, repos, maxClaims, out: values.out ? path.resolve(values.out) : null, note: values.note ?? '' };
}

export async function runCli(argv, { exec, out = (text) => process.stdout.write(`${text}\n`), err = (text) => process.stderr.write(`${text}\n`), scriptPath = process.argv[1], cwd = process.cwd(), env = process.env, ...injected }) {
  let opts;
  try { opts = parseCli(argv); } catch (e) { err(e.message); return 2; }
  const ctx = new Context({ exec, opts, ...injected });
  try {
    if (opts.command === 'status') return (await statusReport(ctx, out)).length ? 1 : 0;
    if (opts.apply && env[LOCK_ENV] !== opts.host) throw new Error(`--apply must run under the per-host lock; run it as node ${scriptPath} …`);
    const steps = opts.command === 'add' ? buildAddPlan(ctx) : buildRemovePlan(ctx);
    const relative = path.relative(cwd, path.resolve(cwd, scriptPath));
    const script = relative.startsWith('..') || path.isAbsolute(relative) ? path.resolve(cwd, scriptPath) : relative;
    const resume = ['node', script, ...argv.filter((arg) => arg !== '--apply'), '--apply'].map(shellQuote).join(' ');
    out(`${opts.command} ${opts.host}: ${opts.apply ? 'APPLY' : 'dry run'}`);
    const { ok } = await runPlan(steps, { apply: opts.apply, out: (text) => out(ctx.scrub(text)), resume });
    return ok ? 0 : 1;
  } catch (e) {
    err(ctx.scrub(e.message));
    return 1;
  }
}
