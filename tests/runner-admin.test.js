import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import * as lib from '../ops/runner/runner-admin-lib.js';
import { importMutant } from './helpers/mutant-module.js';
import { makeTmpDir } from './helpers/tmp.js';

const LIB_FILE = fileURLToPath(new URL('../ops/runner/runner-admin-lib.js', import.meta.url));
const HBA_PATH = '/etc/postgresql/17/main/pg_hba.conf';
const HBA = `# header
local   all  postgres  peer
host    all  postgres  127.0.0.1/32  scram-sha-256
# lane-agent-mac-grandy: from its own source address only, never through the svc:pg-primary forward
hostssl lane_queue_db  lane_agent_mac_grandy  100.80.159.90/32  scram-sha-256
# lane-submit-workbook-pro: from its own source address only, never through the svc:pg-primary forward
hostssl lane_queue_db  lane_submit_workbook_pro  100.76.86.6/32  scram-sha-256
local   lane_queue_db  all  reject
host    lane_queue_db  all  all  reject
local   all  lane_submit_train,lane_admin_pitelite,lane_migrate,lane_agent_skybox,lane_agent_wintop,lane_agent_zirkbot,lane_agent_mac,lane_agent_mac_grandy,lane_submit_workbook_pro  reject
host    all  lane_submit_train,lane_admin_pitelite,lane_migrate,lane_agent_skybox,lane_agent_wintop,lane_agent_zirkbot,lane_agent_mac,lane_agent_mac_grandy,lane_submit_workbook_pro  all  reject
local   all  tp_sim_backup  reject
host    all  tp_sim_backup  all  reject
host    all  replicator  all  reject
local   all  all  reject
host    all  all  all  reject
`;
const OTHER_REJECTS = ['local   all  tp_sim_backup  reject', 'host    all  tp_sim_backup  all  reject', 'host    all  replicator  all  reject', 'local   all  all  reject', 'host    all  all  all  reject'];
const TRAIN = `{
  "runners": [
    { "name": "wintop", "transport": "pull" },
    { "name": "mac", "transport": "local" }
  ],
  "other": 1
}
`;
const ENTRY = { role: 'lane_agent_evo_x2', host: 'evo-x2', ip: '100.82.26.77', note: '' };
const ROOT = { accessKey: 'ROOTACCESSKEY', secretKey: 'ROOTSECRETKEYVALUE' };
const NOW = () => new Date('2026-11-24T10:11:12.345Z');

test('names derive from the host, and invalid hosts and addresses are refused', () => {
  assert.equal(lib.pgRole('evo-x2'), 'lane_agent_evo_x2');
  assert.equal(lib.rustfsName('evo-x2'), 'lane-runner-evo-x2');
  for (const host of ['Evo', '1evo', 'evo_x2', '', 'a'.repeat(32), undefined, 'evo x']) assert.throws(() => lib.validateHost(host), /invalid host/);
  assert.equal(lib.validateHost('a'.repeat(31)), 'a'.repeat(31));
  for (const ip of ['100.82.26.77', '100.64.0.0', '100.127.255.255']) assert.equal(lib.validateTailnetIp(ip), ip);
  for (const ip of ['100.63.0.1', '100.128.0.1', '10.0.0.1', '100.82.26', '100.82.26.256', '100.082.26.1', 'x', undefined]) assert.throws(() => lib.validateTailnetIp(ip), /invalid ip/);
});

test('editHba adds a block before the lane_queue_db reject and lists the role in both catch-alls', () => {
  const out = lib.editHba(HBA, { ...ENTRY, note: 'evo-x2 fleet' }, 'add');
  const lines = out.split('\n');
  const at = lines.indexOf('# BEGIN lane-runner-admin evo-x2');
  assert.deepEqual(lines.slice(at, at + 5), [
    '# BEGIN lane-runner-admin evo-x2',
    '# lane-agent-evo-x2: from its own source address only, never through the svc:pg-primary forward; evo-x2 fleet',
    'hostssl lane_queue_db  lane_agent_evo_x2  100.82.26.77/32  scram-sha-256',
    '# END lane-runner-admin evo-x2',
    'local   lane_queue_db  all  reject',
  ]);
  assert.match(out, /lane_submit_workbook_pro,lane_agent_evo_x2  reject\n/);
  assert.match(out, /lane_submit_workbook_pro,lane_agent_evo_x2  all  reject\n/);
});

test('editHba add is idempotent, takes a changed address in place, and remove reverses it exactly', () => {
  const added = lib.editHba(HBA, ENTRY, 'add');
  assert.equal(lib.editHba(added, ENTRY, 'add'), added);
  assert.equal(added.split('lane_agent_evo_x2').length - 1, 3);
  assert.match(lib.editHba(added, { ...ENTRY, ip: '100.82.26.78' }, 'add'), /lane_agent_evo_x2  100\.82\.26\.78\/32/);
  assert.equal(lib.editHba(added, ENTRY, 'remove'), HBA);
  assert.equal(lib.editHba(HBA, ENTRY, 'remove'), HBA);
});

test('editHba names a missing anchor, an unmatched BEGIN, a hand-written line and an emptied list', () => {
  assert.throws(() => lib.editHba(HBA.replace('local   lane_queue_db  all  reject\n', ''), ENTRY, 'add'), /no "local lane_queue_db all reject" line/);
  assert.throws(() => lib.editHba(HBA.replace(/^local {3}all {2}lane_submit_train.*\n/m, ''), ENTRY, 'add'), /exactly one "local all" reject line listing the lane logins, found 0/);
  const twoLists = HBA.replace(/^(host {4}all {2}lane_submit_train.*\n)/m, '$1$1');
  assert.throws(() => lib.editHba(twoLists, ENTRY, 'add'), /exactly one "host all" reject line listing the lane logins, found 2/);
  assert.equal(lib.editHba(HBA, ENTRY, 'remove'), HBA, 'removing a host that was never added changes nothing');
  assert.throws(() => lib.editHba(`${HBA}# BEGIN lane-runner-admin evo-x2\n`, ENTRY, 'add'), /no matching END/);
  assert.throws(() => lib.editHba(HBA, { ...ENTRY, role: 'lane_agent_mac_grandy', host: 'mac-grandy' }, 'add'), /already has a rule/);
  const single = 'local   all  only_one  reject\nhost    all  only_one  all  reject\nlocal   lane_queue_db  all  reject\n';
  assert.throws(() => lib.editHba(single, { ...ENTRY, role: 'only_one' }, 'remove'), /empty/);
});

test('hbaState reports the block, the line and both reject-list memberships', () => {
  assert.deepEqual(lib.hbaState(lib.editHba(HBA, ENTRY, 'add'), ENTRY), {
    block: true, line: 'hostssl lane_queue_db  lane_agent_evo_x2  100.82.26.77/32  scram-sha-256', localRejected: true, hostRejected: true,
  });
  assert.deepEqual(lib.hbaState(HBA, ENTRY), { block: false, line: null, localRejected: false, hostRejected: false });
});

test('editTrainRunners adds and removes one entry, keeps the rest and the indent, and is idempotent', () => {
  const added = lib.editTrainRunners(TRAIN, 'evo-x2', 'add');
  const config = JSON.parse(added);
  assert.deepEqual(config.runners.map((r) => r.name), ['wintop', 'mac', 'evo-x2']);
  assert.deepEqual(config.runners[2], { name: 'evo-x2', transport: 'pull' });
  assert.equal(config.other, 1);
  assert.ok(added.endsWith('}\n'));
  assert.equal(lib.editTrainRunners(added, 'evo-x2', 'add'), added);
  assert.ok(added.includes('    { "name": "mac", "transport": "local" },\n    { "name": "evo-x2", "transport": "pull" }\n  ],'));
  assert.equal(lib.editTrainRunners(added, 'evo-x2', 'remove'), TRAIN);
  const spread = JSON.stringify(JSON.parse(TRAIN), null, 4);
  const spreadAdded = lib.editTrainRunners(spread, 'evo-x2', 'add');
  assert.deepEqual(JSON.parse(spreadAdded).runners.at(-1), { name: 'evo-x2', transport: 'pull' });
  assert.equal(lib.editTrainRunners(spreadAdded, 'evo-x2', 'remove'), spread);
  assert.equal(lib.editTrainRunners(TRAIN, 'evo-x2', 'remove'), TRAIN);
  assert.equal(JSON.parse(lib.editTrainRunners(TRAIN, 'mac', 'add')).runners[1].transport, 'pull');
  assert.throws(() => lib.editTrainRunners('{}', 'evo-x2', 'add'), /no \.runners/);
});

test('SQL builders quote identifiers and literals, and a dry run shows the password as <redacted>', () => {
  assert.equal(lib.sqlIdent('a"b'), '"a""b"');
  assert.equal(lib.sqlLit("o'brien"), "'o''brien'");
  assert.throws(() => lib.sqlLit('a\0b'), /NUL/);
  const base = { role: 'lane_agent_evo_x2', host: 'evo-x2', tags: ['linux', "it's"], repos: ['*'], maxClaims: 14 };
  const create = lib.roleSql({ ...base, password: "pw'x", roleExists: false });
  assert.match(create, /^BEGIN;\nCREATE ROLE "lane_agent_evo_x2" LOGIN PASSWORD 'pw''x' IN ROLE lane_agent CONNECTION LIMIT 3;/);
  assert.match(create, /ALTER ROLE "lane_agent_evo_x2" SET statement_timeout = '2s';/);
  assert.match(create, /SET idle_in_transaction_session_timeout = '5s';/);
  assert.match(create, /COMMIT;\n$/);
  assert.match(create, /ld_set_principal\('lane_agent_evo_x2', 'agent', 'evo-x2', ARRAY\['linux','it''s'\]::text\[\], ARRAY\['\*'\]::text\[\], 14\);/);
  const redacted = lib.roleSql({ ...base, tags: [], password: lib.REDACTED, roleExists: true });
  assert.match(redacted, /^BEGIN;\nALTER ROLE "lane_agent_evo_x2" LOGIN PASSWORD '<redacted>' CONNECTION LIMIT 3;/);
  assert.match(redacted, /ARRAY\[\]::text\[\]/);
  const tx = lib.removeTxSql({ role: 'lane_agent_evo_x2', host: 'evo-x2', force: false });
  const order = ['BEGIN;', "FROM lane_dispatch.principals WHERE login_role = 'lane_agent_evo_x2' FOR UPDATE;", "RAISE EXCEPTION 'held tickets: %'", 'NOLOGIN', "DELETE FROM lane_dispatch.principals WHERE login_role = 'lane_agent_evo_x2';", "DELETE FROM lane_dispatch.runners WHERE host = 'evo-x2';", 'COMMIT;'];
  assert.deepEqual(order.map((part) => tx.indexOf(part)).every((at, k, all) => at >= 0 && (k === 0 || at > all[k - 1])), true, tx);
  assert.ok(!lib.removeTxSql({ role: 'lane_agent_evo_x2', host: 'evo-x2', force: true }).includes('held tickets'));
  assert.equal(lib.dropRoleSql('lane_agent_evo_x2'), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'lane_agent_evo_x2';\nDROP ROLE IF EXISTS \"lane_agent_evo_x2\";\n");
});

test("the policy is wintop's with the host swapped, and the worker config carries the agent's keys", () => {
  const policy = JSON.parse(lib.rustfsPolicy('evo-x2'));
  assert.deepEqual(policy.Statement.map((s) => s.Resource[0]), ['arn:aws:s3:::lane-snap/*', 'arn:aws:s3:::lane-out/evo-x2/*', 'arn:aws:s3:::lane-out/evo-x2/.probe']);
  assert.deepEqual(JSON.parse(lib.workerConfig('evo-x2', 'lane_agent_evo_x2')), {
    runnerName: 'evo-x2',
    laneQueueUrl: 'postgres://lane_agent_evo_x2@pitelite.tail615381.ts.net:5432/lane_queue_db',
    speedFactor: 1,
    rustfs: { endpoint: 'http://pitelite:9000' },
    rustfsEnvFile: 'rustfs.env',
  });
});

test('unifiedDiff shows the added block and nothing for equal text', () => {
  const diff = lib.unifiedDiff(HBA, lib.editHba(HBA, ENTRY, 'add'), 'pg_hba.conf');
  assert.match(diff, /^--- pg_hba\.conf \(current\)\n\+\+\+ pg_hba\.conf \(proposed\)\n@@ /);
  assert.equal(diff.split('\n').filter((l) => l.startsWith('+') && !l.startsWith('+++')).length, 4 + 2);
  assert.equal(lib.unifiedDiff(HBA, HBA, 'x'), '');
});


const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');
const TRAIN_PATH = '/data/train/config/lane-broker/config.json';

test('the hand-written-rule guard reads tokens: host, hostnossl, indentation and comma lists all refuse, a reject rule does not', () => {
  const add = (rule) => lib.editHba(HBA.replace('local   lane_queue_db  all  reject', `${rule}\nlocal   lane_queue_db  all  reject`), ENTRY, 'add');
  const rules = [
    'host lane_queue_db lane_agent_evo_x2 100.64.0.0/10 scram-sha-256',
    '  hostnossl lane_queue_db lane_agent_evo_x2 10.0.0.0/8 md5',
    'hostgssenc all other,lane_agent_evo_x2 0.0.0.0/0 trust',
    'local all "lane_agent_evo_x2" peer',
    'hostssl lane_queue_db lane_agent_evo_x2 100.1.1.1/32 scram-sha-256 # why',
  ];
  for (const rule of rules) assert.throws(() => add(rule), /already has a rule outside .* may let lane_agent_evo_x2 in/, rule);
  assert.doesNotThrow(() => add('host all lane_agent_evo_x2 all reject'));
  assert.doesNotThrow(() => add('# host lane_queue_db lane_agent_evo_x2 100.64.0.0/10 scram-sha-256'));
  const hand = 'host lane_queue_db lane_agent_evo_x2 100.64.0.0/10 scram-sha-256';
  assert.equal(lib.hbaState(`${HBA}${hand}\n`, ENTRY).line, hand);
});

test('a note that could continue the pg_hba comment, or break the rule, is refused', async () => {
  for (const note of ['foo\\', 'a\nb', "it's", '$(x)', 'x'.repeat(121)]) {
    assert.throws(() => lib.validateNote(note), /invalid --note/, JSON.stringify(note));
    assert.throws(() => lib.editHba(HBA, { ...ENTRY, note }, 'add'), /invalid --note/);
  }
  assert.equal(lib.validateNote('evo-x2 fleet (new), see #12: a/b+c_d.'), 'evo-x2 fleet (new), see #12: a/b+c_d.');
  const world = fakeWorld();
  const { code, errs } = await cli(world, [...ADD, '--note', 'foo\\']);
  assert.equal(code, 2);
  assert.match(errs, /invalid --note/);
  assert.equal(world.calls.length, 0);
});

test("editHba adds the role only to the lane-logins reject lines, never to other apps' reject lines or the final catch-all", () => {
  const added = lib.editHba(HBA, ENTRY, 'add');
  for (const line of OTHER_REJECTS) assert.ok(added.split('\n').includes(line), `untouched: ${line}`);
  assert.equal(added.split('\n').filter((l) => l.includes('lane_agent_evo_x2') && /reject/.test(l)).length, 2);
  assert.equal(lib.editHba(added, ENTRY, 'remove'), HBA);
});

test('a broad rule below the lane-logins reject line is unreachable and allowed; the same rule above it is refused', () => {
  const entry = { role: 'lane_agent_evo_x2', host: 'evo-x2', ip: '100.82.26.77' };
  const below = `${HBA}hostssl all  all  100.64.0.0/10  scram-sha-256\nlocal   all  all  peer\n`;
  assert.doesNotThrow(() => lib.editHba(below, entry, 'add'));
  const above = HBA.replace('local   lane_queue_db  all  reject\n', 'hostssl all  all  100.64.0.0/10  scram-sha-256\nlocal   lane_queue_db  all  reject\n');
  assert.throws(() => lib.editHba(above, entry, 'add'), /may let lane_agent_evo_x2 in: "hostssl all  all  100.64.0.0\/10/);
  const literalBelow = `${HBA}host lane_queue_db lane_agent_evo_x2 100.64.0.0/10 scram-sha-256\n`;
  assert.throws(() => lib.editHba(literalBelow, entry, 'add'), /may let lane_agent_evo_x2 in/);
});

test('shellQuote survives the remote shell: a psql field separator stays one word, not a pipe', () => {
  const words = ['psql', '-F', '|', "it's", 'a b', '-tAq'];
  assert.deepEqual(lib.shellSplit(words.map(lib.shellQuote).join(' ')), words);
  assert.equal(lib.shellQuote('|'), "'|'");
});

const HBA_STAT = 'postgres:postgres 640';
const TRAIN_STAT = 'train:train 644';

function fakeWorld({ hba = HBA, train = TRAIN, held = [], hbaErrors = 0, item = null, roleExists = false } = {}) {
  const world = {
    calls: [], hba, train, held, heldInTx: [], hbaErrors, item, roleExists, templates: [], mcEnvs: [], policyFile: null,
    fail: {}, scripts: [], reloads: 0, principalExists: false, roleLogin: true, afterRole: null, extraItems: [], backup: null, principalDeleted: false, beforeScript: null,
  };
  const ok = (stdout = '') => ({ code: 0, stdout, stderr: '' });
  const bad = (stderr, code = 1) => ({ code, stdout: '', stderr });
  const fileOf = (file) => (file === HBA_PATH ? 'hba' : 'train');

  function editScript(args, input) {
    const [file, expect, owner, mode, kind] = args.slice(args.indexOf('--') + 1);
    world.beforeScript?.(file);
    const key = fileOf(file);
    const payload = Buffer.from(/<<'LRA_PAYLOAD_EOF'\n([\s\S]*?)\nLRA_PAYLOAD_EOF/.exec(input)[1].replaceAll('\n', ''), 'base64').toString('utf8');
    world.scripts.push({ file, expect, owner, mode, kind, input, payload });
    if (sha256(world[key]) !== expect) return bad(`changed since read: ${file}`, 3);
    if (world.fail[key] === 'write') return bad('mv: cannot move: No space left on device', 1);
    const changed = payload !== world[key];
    const before = world[key];
    if (changed) world[key] = payload;
    if (key === 'hba' && (world.hbaErrors > 0 || world.fail.hba === 'validate' || world.fail.hba === 'throw')) {
      world[key] = before;
      return bad(world.fail.hba === 'throw' ? 'psql: connection to server failed' : `pg_hba_file_rules reports '${world.hbaErrors}' rule errors\nrestored ${file}.bak-lane-runner-admin-FAKE`, 6);
    }
    if (key === 'train' && world.fail.train === 'validate') { world[key] = before; return bad('not valid JSON\nrestored', 6); }
    if (key === 'hba' && world.fail.reload) return bad('reload failed: the file is valid but not live; re-run to reload', 7);
    if (key === 'hba') world.reloads += 1;
    return ok(changed ? `backup=${file}.bak-lane-runner-admin-FAKE\n` : `unchanged=${file}\n`);
  }

  world.exec = async (cmd, rawArgs, { input, env } = {}) => {
    if (cmd === 'ssh') assert.equal(rawArgs.length, 2, `ssh must get the host and one quoted command string, got ${JSON.stringify(rawArgs)}`);
    const args = cmd === 'ssh' ? [rawArgs[0], ...lib.shellSplit(rawArgs[1])] : rawArgs;
    world.calls.push({ cmd, args: [...args], input, env });
    const joined = args.join(' ');
    if (cmd === 'op-bot' && args[1] === 'get') {
      if (!world.item) return bad('"lane-agent evo-x2" isn\'t an item in vault BotAccess');
      return ok(JSON.stringify({ fields: world.item.fields }));
    }
    if (cmd === 'op-bot' && args[1] === 'list') return ok(JSON.stringify([...(world.item ? [{ id: 'item-1', title: 'lane-agent evo-x2' }] : []), ...world.extraItems]));
    if (cmd === 'op-bot' && args[1] === 'create') {
      const template = JSON.parse(fs.readFileSync(args[args.indexOf('--template') + 1], 'utf8'));
      world.templates.push(template);
      world.item = template;
      return ok();
    }
    if (cmd === 'mc') {
      world.mcEnvs.push(env);
      if (world.fail.mc && args.slice(0, 3).join(' ') === world.fail.mc) return bad(`mc: <ERROR> boom http://${ROOT.accessKey}:${ROOT.secretKey}@x`);
      if (args[1] === 'policy' && args[2] === 'create') world.policyFile = fs.readFileSync(args[5], 'utf8');
      return ok();
    }
    assert.equal(cmd, 'ssh', `unexpected command ${cmd}`);
    if (joined.includes(' flock ')) return editScript(args, input);
    if (joined.endsWith(`stat -c %U:%G %a ${HBA_PATH}`)) return ok(`${HBA_STAT}\n`);
    if (joined.endsWith(`stat -c %U:%G %a ${TRAIN_PATH}`)) return ok(`${TRAIN_STAT}\n`);
    if (joined.endsWith(`cat ${HBA_PATH}`)) return ok(world.hba);
    if (joined.includes('-u train cat')) return ok(world.train);
    if (joined.endsWith('cat /home/admin/rustfs/rustfs.env')) return ok(`RUSTFS_ACCESS_KEY=${ROOT.accessKey}\nRUSTFS_SECRET_KEY="${ROOT.secretKey}"\n`);
    if (joined.endsWith('homelab-postgres-ca.pem')) return ok('CA-PEM\n');
    if (joined.includes('psql')) {
      if (input.includes('NOLOGIN')) {
        if (input.includes("RAISE EXCEPTION 'held tickets") && world.heldInTx.length) return bad(`ERROR:  held tickets: ${world.heldInTx.map(([id, lane]) => `${id} (${lane})`).join(', ')}\nCONTEXT:  PL/pgSQL function inline_code_block`);
        world.principalDeleted = true;
        world.principalExists = false;
        return ok();
      }
      if (input.includes('DROP ROLE')) { world.roleExists = false; return ok(); }
      if (input.includes('CREATE ROLE') || input.includes('ALTER ROLE')) {
        if (world.fail.psql) return bad(world.fail.psql, 3);
        world.roleExists = true;
        world.principalExists = true;
        world.roleLogin = /\bLOGIN\b/.test(input);
        world.afterRole?.();
        return ok();
      }
      if (input.includes('login_role') && input.includes('count(*) FROM pg_roles')) return ok(`${world.roleExists ? 1 : 0}|${world.principalExists ? 1 : 0}\n`);
      if (input.includes('FROM pg_roles')) return ok(world.roleExists ? '1\n' : '0\n');
      if (input.includes('lane_dispatch.tickets')) return ok(world.held.map((t) => t.join('|')).join('\n'));
      return ok();
    }
    throw new Error(`unhandled call: ${cmd} ${joined}`);
  };
  return world;
}

const REPO = fileURLToPath(new URL('..', import.meta.url));
const SCRIPT = path.join(REPO, 'ops/runner/lane-runner-admin.js');

async function cli(world, argv, mod = lib, extra = {}) {
  const lines = [];
  const errs = [];
  const code = await mod.runCli(argv, { exec: world.exec, out: (t) => lines.push(t), err: (t) => errs.push(t), lockDir: makeTmpDir('runner-admin-lock-'), scriptPath: SCRIPT, cwd: REPO, ...extra });
  return { code, text: lines.join('\n'), errs: errs.join('\n') };
}

const ADD = ['add', 'evo-x2', '--ip', '100.82.26.77', '--max-claims', '14'];
const itemField = (world, match) => world.item.fields.find(match).value;
const passwordOf = (world) => itemField(world, (f) => f.id === 'password');
const secretKeyOf = (world) => itemField(world, (f) => f.label === 'rustfs_secret_key');
const accessKeyOf = (world) => itemField(world, (f) => f.label === 'rustfs_access_key');
const itemWith = (password) => ({ fields: [
  { id: 'password', purpose: 'PASSWORD', value: password },
  { label: 'rustfs_access_key', value: 'ACCESSKEY0123456789A' },
  { label: 'rustfs_secret_key', value: 'secret-value-2' },
] });
const heldItem = itemWith('pw-value-1');
const isEditCall = (call) => call.args.includes('flock');
const mcOps = (world) => world.calls.filter((c) => c.cmd === 'mc').map((c) => c.args.slice(0, 3).join(' '));

test('a dry run changes nothing, prints every step with the password redacted, and leaks no secret', async () => {
  const world = fakeWorld();
  const { code, text } = await cli(world, ADD);
  assert.equal(code, 0);
  for (const kind of ['secrets', 'postgres', 'hba', 'rustfs', 'train']) assert.match(text, new RegExp(`\\[${kind}\\]`));
  assert.match(text, /BEGIN;\n {4}CREATE ROLE "lane_agent_evo_x2" LOGIN PASSWORD '<redacted>' IN ROLE lane_agent CONNECTION LIMIT 3;/);
  assert.match(text, /\+hostssl lane_queue_db {2}lane_agent_evo_x2 {2}100\.82\.26\.77\/32 {2}scram-sha-256/);
  assert.match(text, /provision\.sh/);
  assert.match(text, /nothing changed/);
  assert.equal(world.hba, HBA);
  assert.equal(world.train, TRAIN);
  assert.equal(world.templates.length, 0);
  assert.equal(world.calls.filter((c) => c.cmd === 'mc').length, 0);
  assert.ok(!world.calls.some(isEditCall));
});

test('apply runs the steps in order, keeps every secret out of argv and output, and stores them in 1Password', async () => {
  const world = fakeWorld();
  const dir = path.join(makeTmpDir('runner-admin-out-'), 'worker');
  const { code, text } = await cli(world, [...ADD, '--out', dir, '--apply']);
  assert.equal(code, 0, text);
  const order = world.calls.map((c) => {
    if (c.cmd === 'op-bot') return `op-bot ${c.args[1]}`;
    if (c.cmd === 'mc') return `mc ${c.args.slice(0, 3).join(' ')}`;
    if (isEditCall(c)) return `edit ${c.args.at(-1)}`;
    return c.input?.includes('CREATE ROLE') ? 'create role' : null;
  }).filter(Boolean);
  assert.deepEqual(order, ['op-bot get', 'op-bot create', 'op-bot list', 'op-bot get', 'create role', 'edit hba', 'mc admin user add', 'mc admin policy create', 'mc admin policy attach', 'edit json']);

  const password = passwordOf(world);
  const secretKey = secretKeyOf(world);
  const accessKey = accessKeyOf(world);
  assert.match(password, /^[A-Za-z0-9_-]{43}$/);
  assert.match(accessKey, /^[A-Za-z0-9]{20}$/);
  assert.match(secretKey, /^[A-Za-z0-9]{40}$/);
  assert.equal(world.item.category, 'PASSWORD');
  const roleCall = world.calls.find((c) => c.input?.includes('CREATE ROLE'));
  assert.ok(roleCall.input.includes(`PASSWORD '${password}'`));
  assert.ok(roleCall.args.includes('ON_ERROR_STOP=1') && roleCall.args.includes('lane_queue_db'));
  assert.ok(world.mcEnvs.every((env) => env.MC_HOST_pl === `http://${ROOT.accessKey}:${ROOT.secretKey}@pitelite.tail615381.ts.net:9000`));
  assert.equal(JSON.parse(world.policyFile).Statement[1].Resource[0], 'arn:aws:s3:::lane-out/evo-x2/*');
  assert.match(world.hba, /lane_agent_evo_x2 {2}100\.82\.26\.77\/32/);
  assert.ok(lib.trainHasRunner(world.train, 'evo-x2'));
  assert.equal(world.reloads, 1);

  const [hbaEdit, trainEdit] = world.scripts;
  assert.deepEqual([hbaEdit.file, hbaEdit.expect, hbaEdit.owner, hbaEdit.mode, hbaEdit.kind], [HBA_PATH, sha256(HBA), 'postgres:postgres', '640', 'hba']);
  assert.deepEqual([trainEdit.file, trainEdit.expect, trainEdit.owner, trainEdit.mode, trainEdit.kind], [TRAIN_PATH, sha256(TRAIN), 'train:train', '644', 'json']);
  assert.deepEqual(world.calls.find(isEditCall).args.slice(0, 7), ['pitelite', 'sudo', '-n', 'flock', '/run/lock/lane-runner-admin.lock', 'bash', '-s']);

  const argvOf = (calls) => calls.flatMap((c) => c.args).join('\n');
  const userAdd = world.calls.find((c) => c.cmd === 'mc' && c.args[2] === 'add');
  assert.deepEqual(userAdd.args.slice(4), [accessKey, secretKey]);
  const others = world.calls.filter((c) => c !== userAdd);
  for (const secret of [password, secretKey, ROOT.secretKey]) {
    assert.ok(!argvOf(others).includes(secret), 'secret in argv');
    assert.ok(!text.includes(secret), 'secret in output');
    assert.ok(world.scripts.every((script) => !script.input.includes(secret) && !script.payload.includes(secret)), 'secret in an edit script');
  }
  assert.ok(!argvOf([userAdd]).includes(password) && !argvOf([userAdd]).includes(ROOT.secretKey));

  assert.equal(fs.statSync(dir).mode & 0o777, 0o700);
  for (const name of ['config.json', 'password', 'rustfs.env', 'ca.pem']) assert.equal(fs.statSync(path.join(dir, name)).mode & 0o777, 0o600, name);
  assert.equal(fs.readFileSync(path.join(dir, 'password'), 'utf8'), password);
  assert.equal(fs.readFileSync(path.join(dir, 'rustfs.env'), 'utf8'), `AWS_ACCESS_KEY_ID=${accessKey}\nAWS_SECRET_ACCESS_KEY=${secretKey}\n`);
  assert.equal(fs.readFileSync(path.join(dir, 'ca.pem'), 'utf8'), 'CA-PEM\n');
});

test('an edit that would carry a known secret is refused before any script runs', async () => {
  const world = fakeWorld({ item: itemWith('hunter2hunter2') });
  const { code, text } = await cli(world, [...ADD, '--note', 'hunter2hunter2', '--apply']);
  assert.equal(code, 1);
  assert.match(text, /pg_hba\.conf would carry a secret/);
  assert.ok(!world.calls.some(isEditCall));
});

test('re-running add converges: the item is reused, the role is altered, and the unchanged hba is validated and reloaded again', async () => {
  const world = fakeWorld();
  await cli(world, [...ADD, '--apply']);
  const password = passwordOf(world);
  world.calls.length = 0;
  world.scripts.length = 0;
  const { code } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 0);
  assert.equal(world.templates.length, 1);
  assert.match(world.calls.find((c) => c.input?.includes('ALTER ROLE')).input, new RegExp(`^BEGIN;\\nALTER ROLE "lane_agent_evo_x2" LOGIN PASSWORD '${password}' CONNECTION LIMIT 3;`));
  assert.deepEqual(world.scripts.map((s) => s.kind), ['hba']);
  assert.equal(world.reloads, 2);
  assert.equal(world.hba.split('BEGIN lane-runner-admin evo-x2').length, 2);
});

test('a psql error that echoes the statement never reaches the output: only the exit code and a fixed message', async () => {
  const password = "pw'x";
  const world = fakeWorld({ item: itemWith(password) });
  world.fail.psql = `ERROR:  syntax error at or near "x"\nLINE 1: ...LOGIN PASSWORD 'pw''x' IN ROLE lane_agent...`;
  const { code, text, errs } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 1);
  assert.match(text, /FAILED: create role and principal failed \(exit 3; output withheld/);
  for (const form of [password, "pw''x", 'pw%27x']) assert.ok(!text.includes(form) && !errs.includes(form), form);
  assert.ok(!world.calls.some((c) => c.cmd === 'mc'));
});

test('the SQLSTATE alone is surfaced from a withheld psql error', async () => {
  const world = fakeWorld();
  world.fail.psql = 'ERROR:  42710\n';
  const { text } = await cli(world, [...ADD, '--apply']);
  assert.match(text, /failed \(exit 3, SQLSTATE 42710; output withheld/);
});

test('other failures scrub every serialised form of a secret: quoted, URL-encoded, base64, JSON', async () => {
  const password = 'p"w\'x&y';
  const world = fakeWorld({ item: itemWith(password) });
  const inner = world.exec;
  const forms = [password, password.replaceAll("'", "''"), encodeURIComponent(password), Buffer.from(password).toString('base64'), JSON.stringify(password).slice(1, -1)];
  world.exec = async (cmd, args, opts) => (args.join(' ').endsWith(`cat ${HBA_PATH}`)
    ? { code: 1, stdout: '', stderr: `boom ${forms.join(' | ')}` }
    : inner(cmd, args, opts));
  const { code, text } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 1);
  assert.match(text, /FAILED: read pg_hba\.conf failed/);
  for (const form of forms) assert.ok(!text.includes(form), form);
});

test('a mc error echoing the root URL is scrubbed, and mc user add withholds its output', async () => {
  const world = fakeWorld();
  world.fail.mc = 'admin policy create';
  const { text } = await cli(world, [...ADD, '--apply']);
  assert.match(text, /FAILED: mc admin policy create failed/);
  assert.ok(!text.includes(ROOT.secretKey));
  const userAddWorld = fakeWorld();
  userAddWorld.fail.mc = 'admin user add';
  const failed = await cli(userAddWorld, [...ADD, '--apply']);
  assert.match(failed.text, /mc admin user add failed \(exit 1; output withheld/);
});

test('the resume command keeps shell quoting for every argument', async () => {
  const world = fakeWorld();
  world.fail.psql = 'x';
  const { text } = await cli(world, ['add', 'evo-x2', '--ip', '100.82.26.77', '--repos', '*', '--out', '/tmp/a b/$(x)', '--note', 'two words', '--apply']);
  assert.match(text, /resume \(every step converges\): node ops\/runner\/lane-runner-admin\.js add evo-x2 --ip 100\.82\.26\.77 --repos '\*' --out '\/tmp\/a b\/\$\(x\)' --note 'two words' --apply\n?$/);
});

const FAILURES = [
  { name: 'a write failure on pg_hba.conf', set: (w) => { w.fail.hba = 'write'; }, clear: (w) => { w.fail.hba = null; }, stoppedBefore: 'mc', hbaChanged: false, message: /pg_hba\.conf edit failed \(exit 1\): mv: cannot move/ },
  { name: 'a validation that throws on pg_hba.conf', set: (w) => { w.fail.hba = 'throw'; }, clear: (w) => { w.fail.hba = null; }, stoppedBefore: 'mc', hbaChanged: false, message: /pg_hba\.conf edit failed \(exit 6\): psql: connection/ },
  { name: 'a validation that rejects pg_hba.conf', set: (w) => { w.hbaErrors = 2; }, clear: (w) => { w.hbaErrors = 0; }, stoppedBefore: 'mc', hbaChanged: false, message: /rule errors/ },
  { name: 'a reload that postgres does not confirm', set: (w) => { w.fail.reload = true; }, clear: (w) => { w.fail.reload = false; }, stoppedBefore: 'mc', hbaChanged: true, message: /reload failed: the file is valid but not live/ },
  { name: 'a mc failure', set: (w) => { w.fail.mc = 'admin user add'; }, clear: (w) => { w.fail.mc = null; }, stoppedBefore: 'train', hbaChanged: true, message: /mc admin user add failed/ },
  { name: 'a write failure on the train config', set: (w) => { w.fail.train = 'write'; }, clear: (w) => { w.fail.train = null; }, stoppedBefore: null, hbaChanged: true, message: /train config edit failed/ },
  { name: 'a train config that does not parse after the write', set: (w) => { w.fail.train = 'validate'; }, clear: (w) => { w.fail.train = null; }, stoppedBefore: null, hbaChanged: true, message: /not valid JSON/ },
];

for (const failure of FAILURES) {
  test(`${failure.name} stops the run with a resume command, keeps the earlier steps, and a retry converges`, async () => {
    const world = fakeWorld();
    failure.set(world);
    const first = await cli(world, [...ADD, '--apply']);
    assert.equal(first.code, 1, first.text);
    assert.match(first.text, failure.message);
    assert.match(first.text, /resume \(every step converges\): node ops\/runner\/lane-runner-admin\.js add evo-x2 .* --apply/);
    assert.ok(world.item && world.roleExists, 'the earlier secret and role steps stand');
    assert.equal(world.hba === HBA, !failure.hbaChanged, 'hba is either untouched or fully changed, never half');
    assert.equal(world.train, TRAIN);
    if (failure.stoppedBefore === 'mc') assert.equal(mcOps(world).length, 0);
    if (failure.stoppedBefore === 'train') assert.ok(!world.scripts.some((s) => s.kind === 'json'));

    failure.clear(world);
    const retry = await cli(world, [...ADD, '--apply']);
    assert.equal(retry.code, 0, retry.text);
    assert.equal(world.hba.split('BEGIN lane-runner-admin evo-x2').length, 2);
    assert.deepEqual(JSON.parse(world.train).runners.map((r) => r.name), ['wintop', 'mac', 'evo-x2']);
    assert.ok(mcOps(world).includes('admin policy attach'));
    assert.equal(world.templates.length, 1, 'the 1Password item is created once');
  });
}

test('a file that changed between the read and the write is refused with nothing written', async () => {
  const world = fakeWorld();
  world.beforeScript = (file) => { if (file === HBA_PATH) world.hba = `${HBA}# edited by hand\n`; };
  const { code, text } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 1);
  assert.match(text, /pg_hba\.conf changed on pitelite since it was read; nothing was written/);
  assert.equal(world.hba, `${HBA}# edited by hand\n`);
  assert.equal(mcOps(world).length, 0);
});

async function runScript({ current, content, expect = sha256(current), kind = 'hba', hbaErrors = 0, jsonCheck, reloadOutput = 't' }) {
  const dir = makeTmpDir('runner-admin-script-');
  const target = path.join(dir, kind === 'hba' ? 'pg_hba.conf' : 'config.json');
  fs.writeFileSync(target, current, { mode: 0o600 });
  const log = path.join(dir, 'reload.log');
  const script = lib.buildEditScript({ content, hooks: {
    sha: 'shasum -a 256', chown: 'true', hbaErrors: `echo ${hbaErrors}`, reload: `echo reloaded >> '${log}'; echo ${reloadOutput}`,
    ...(jsonCheck ? { jsonCheck } : { jsonCheck: `'${process.execPath}' -e 'JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"))'` }),
  } });
  const result = await lib.spawnExec('bash', ['-s', '--', target, expect, 'whoever:whatever', '640', kind], { input: script });
  const files = fs.readdirSync(dir);
  return {
    ...result, target, files,
    text: fs.readFileSync(target, 'utf8'),
    mode: fs.statSync(target).mode & 0o777,
    backups: files.filter((f) => f.includes('.bak-lane-runner-admin-')).map((f) => fs.readFileSync(path.join(dir, f), 'utf8')),
    reloaded: fs.existsSync(log) ? fs.readFileSync(log, 'utf8') : '',
  };
}

const NEW_HBA = lib.editHba(HBA, ENTRY, 'add');

async function validationFailureScenario(mod = lib) {
  const script = mod.buildEditScript;
  const dir = makeTmpDir('runner-admin-script-');
  const target = path.join(dir, 'pg_hba.conf');
  fs.writeFileSync(target, HBA);
  const text = script({ content: NEW_HBA, hooks: { sha: 'shasum -a 256', chown: 'true', hbaErrors: 'echo 2', reload: `echo reloaded >> '${dir}/reload.log'; echo t` } });
  const result = await lib.spawnExec('bash', ['-s', '--', target, sha256(HBA), 'a:b', '640', 'hba'], { input: text });
  assert.notEqual(result.code, 0, 'a rejected pg_hba must fail the script');
  assert.equal(fs.readFileSync(target, 'utf8'), HBA, 'the live file is restored from the backup');
  assert.match(result.stderr, /restored .*\.bak-lane-runner-admin-/);
  assert.ok(!fs.existsSync(`${dir}/reload.log`), 'a rejected file is never reloaded');
  assert.deepEqual(fs.readdirSync(dir).filter((f) => f.includes('.stage.') || f.includes('.restore.')), []);
}

async function changedSinceReadScenario(mod = lib) {
  const dir = makeTmpDir('runner-admin-script-');
  const target = path.join(dir, 'pg_hba.conf');
  fs.writeFileSync(target, `${HBA}# edited meanwhile\n`);
  const text = mod.buildEditScript({ content: NEW_HBA, hooks: { sha: 'shasum -a 256', chown: 'true', hbaErrors: 'echo 0', reload: 'echo t' } });
  const result = await lib.spawnExec('bash', ['-s', '--', target, sha256(HBA), 'a:b', '640', 'hba'], { input: text });
  assert.equal(result.code, 3, 'a file that changed since it was read must stop the script');
  assert.equal(fs.readFileSync(target, 'utf8'), `${HBA}# edited meanwhile\n`, 'nothing is written over the concurrent edit');
  assert.deepEqual(fs.readdirSync(dir), ['pg_hba.conf']);
}

test('the edit script on a real shell: write is atomic, owner/mode applied, backup kept, validated, reloaded', async () => {
  const run = await runScript({ current: HBA, content: NEW_HBA });
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.text, NEW_HBA);
  assert.equal(run.mode, 0o640);
  assert.deepEqual(run.backups, [HBA]);
  assert.match(run.stdout, /^backup=.*\.bak-lane-runner-admin-\d{8}T\d{6}.*Z\n$/);
  assert.equal(run.reloaded, 'reloaded\n');
  assert.deepEqual(run.files.filter((f) => f.includes('.stage.') || f.includes('.restore.')), []);
});

test('the edit script carries awkward content intact', async () => {
  const awkward = `quote ' "dq" $(echo no) \`tick\` \\ back\nLRA_PAYLOAD_EOF\n${'x'.repeat(500)}\n`;
  const run = await runScript({ current: 'old\n', content: awkward });
  assert.equal(run.code, 0, run.stderr);
  assert.equal(run.text, awkward);
});

test('the edit script on a real shell: a validation failure restores the backup and never reloads', () => validationFailureScenario());

test('the edit script on a real shell: a file changed since the read exits 3 and writes nothing', () => changedSinceReadScenario());

test('the edit script on a real shell: an unchanged hba is still validated and reloaded, with no backup', async () => {
  const run = await runScript({ current: NEW_HBA, content: NEW_HBA });
  assert.equal(run.code, 0, run.stderr);
  assert.deepEqual(run.backups, []);
  assert.match(run.stdout, /^unchanged=/);
  assert.equal(run.reloaded, 'reloaded\n');
  const rejected = await runScript({ current: NEW_HBA, content: NEW_HBA, hbaErrors: 3 });
  assert.notEqual(rejected.code, 0);
  assert.equal(rejected.reloaded, '');
});

test('the edit script on a real shell: json is validated and a broken one is restored', async () => {
  const good = await runScript({ current: TRAIN, content: lib.editTrainRunners(TRAIN, 'evo-x2', 'add'), kind: 'json' });
  assert.equal(good.code, 0, good.stderr);
  assert.equal(good.reloaded, '', 'only hba reloads postgres');
  const broken = await runScript({ current: TRAIN, content: '{ "runners": [', kind: 'json' });
  assert.notEqual(broken.code, 0);
  assert.equal(broken.text, TRAIN);
  assert.match(broken.stderr, /not valid JSON/);
});

test('the edit script refuses a damaged payload and an existing backup name', async () => {
  const dir = makeTmpDir('runner-admin-script-');
  const target = path.join(dir, 'f');
  fs.writeFileSync(target, 'old\n');
  const script = lib.buildEditScript({ content: 'new\n', hooks: { sha: 'shasum -a 256', chown: 'true' } }).replace(/^want=.*$/m, 'want=0000');
  const result = await lib.spawnExec('bash', ['-s', '--', target, sha256('old\n'), 'a:b', '640', 'json'], { input: script });
  assert.equal(result.code, 4);
  assert.equal(fs.readFileSync(target, 'utf8'), 'old\n');
});

test('canary: with the restore of the backup removed, the validation-failure scenario fails', async () => {
  const mutant = await importMutant(LIB_FILE, ['cp -p "$backup" "$restore" &&', 'true &&']);
  await assert.rejects(validationFailureScenario(mutant), (err) => err.code === 'ERR_ASSERTION');
});

test('canary: with the sha check removed, the changed-since-read scenario fails', async () => {
  const mutant = await importMutant(LIB_FILE, ['[ "$(sha "$path")" = "$expect" ]', 'true']);
  await assert.rejects(changedSinceReadScenario(mutant), (err) => err.code === 'ERR_ASSERTION');
});

async function raceScenario(mod = lib) {
  const world = fakeWorld({ hba: lib.editHba(HBA, ENTRY, 'add'), train: lib.editTrainRunners(TRAIN, 'evo-x2', 'add'), item: heldItem, roleExists: true });
  world.heldInTx = [['33333333-cccc', 'e2e']];
  const { code, text } = await cli(world, ['remove', 'evo-x2', '--apply'], mod);
  assert.equal(code, 1, 'a ticket claimed after the pre-check must stop the removal');
  assert.match(text, /claimed ticket\(s\) while it was being removed: 33333333-cccc \(e2e\)/);
  assert.equal(world.principalDeleted, false);
  assert.ok(!world.calls.some((c) => c.input?.includes('DROP ROLE')));
  assert.ok(!world.scripts.length && mcOps(world).length === 0, 'nothing after the transaction ran');
}

test('remove: a ticket that appears only inside the locked transaction stops the removal', () => raceScenario());

test('canary: with the in-transaction held-ticket check removed, the race scenario fails', async () => {
  const mutant = await importMutant(LIB_FILE, ["RAISE EXCEPTION 'held tickets", "RAISE NOTICE 'held tickets"]);
  await assert.rejects(raceScenario(mutant), (err) => err.code === 'ERR_ASSERTION');
});

test('ld_claim takes the principal row lock that remove relies on', () => {
  const functions = fs.readFileSync(new URL('../sql/lane/functions.sql', import.meta.url), 'utf8');
  const claim = functions.slice(functions.indexOf('FUNCTION lane_dispatch.ld_claim('));
  const body = claim.slice(0, claim.indexOf('$$;'));
  assert.match(body, /FROM lane_dispatch\.principals p WHERE p\.login_role = session_user AND p\.kind = 'agent' FOR UPDATE/);
});

test('remove refuses while the host holds claimed or running tickets, and --force proceeds', async () => {
  const held = [['11111111-aaaa', 'e2e'], ['22222222-bbbb', 'unit']];
  const registered = () => ({ hba: lib.editHba(HBA, ENTRY, 'add'), train: lib.editTrainRunners(TRAIN, 'evo-x2', 'add'), held, item: heldItem, roleExists: true });
  const refused = fakeWorld(registered());
  const preview = await cli(refused, ['remove', 'evo-x2']);
  assert.match(preview.text, /2 held: 11111111-aaaa \(e2e\), 22222222-bbbb \(unit\)\s+remove would refuse without --force/);
  const result = await cli(refused, ['remove', 'evo-x2', '--apply']);
  assert.equal(result.code, 1);
  assert.match(result.text, /11111111-aaaa \(e2e\), 22222222-bbbb \(unit\); re-run with --force/);
  assert.ok(!refused.calls.some((c) => c.input?.includes('NOLOGIN')));

  const forced = fakeWorld(registered());
  const done = await cli(forced, ['remove', 'evo-x2', '--force', '--apply']);
  assert.equal(done.code, 0, done.text);
  assert.equal(forced.hba, HBA);
  assert.deepEqual(JSON.parse(forced.train).runners.map((r) => r.name), ['wintop', 'mac']);
  assert.deepEqual(mcOps(forced), ['admin policy detach', 'admin user remove', 'admin policy remove']);
  const txAt = forced.calls.findIndex((c) => c.input?.includes('NOLOGIN'));
  const dropAt = forced.calls.findIndex((c) => c.input?.includes('DROP ROLE'));
  const hbaAt = forced.calls.findIndex((c) => isEditCall(c) && c.args.at(-1) === 'hba');
  assert.ok(txAt >= 0 && dropAt > txAt && hbaAt > dropAt);
  assert.ok(!forced.calls[txAt].input.includes('held tickets'), '--force drops the in-transaction check');
  assert.match(done.text, /op-bot item delete "lane-agent evo-x2" --vault BotAccess --archive/);
  assert.ok(!forced.calls.some((c) => c.cmd === 'op-bot' && c.args[1] === 'delete'));
});

test('status reports each piece and is read-only', async () => {
  const world = fakeWorld({ hba: lib.editHba(HBA, ENTRY, 'add'), train: lib.editTrainRunners(TRAIN, 'evo-x2', 'add'), held: [['t1', 'unit']], item: heldItem });
  const { code, text } = await cli(world, ['status', 'evo-x2']);
  assert.equal(code, 0);
  for (const label of ['postgres role', 'principal row', 'runners row', 'pg_hba line', 'pg_hba reject lists', '1Password item', 'rustfs user', 'rustfs policy', 'train runners entry', 'claimed/running']) assert.match(text, new RegExp(label));
  assert.match(text, /pg_hba line +present/);
  assert.match(text, /local all: listed, host all: listed/);
  assert.match(text, /train runners entry +present/);
  assert.match(text, /claimed\/running +t1 \(unit\)/);
  assert.ok(!text.includes('pw-value-1') && !text.includes('secret-value-2'));
  assert.ok(!world.calls.some((c) => isEditCall(c) || c.input?.includes('DELETE')));
});

test('bad arguments exit 2 with a message and touch nothing', async () => {
  const world = fakeWorld();
  const bad = [['add', 'evo-x2'], ['add', 'Evo', '--ip', '100.82.26.77'], ['add', 'evo-x2', '--ip', '10.0.0.1'], ['remove', 'evo-x2', '--ip', '100.82.26.77'], ['frob', 'x'], ['add', 'evo-x2', '--ip', '100.82.26.77', '--force']];
  for (const argv of bad) {
    const { code, errs } = await cli(world, argv);
    assert.equal(code, 2, argv.join(' '));
    assert.ok(errs.length > 0);
  }
  assert.equal(world.calls.length, 0);
});

test('the guard refuses broad earlier rules: +group, all, /regex and @file in the user or database field', () => {
  const add = (rule) => lib.editHba(HBA.replace('local   lane_queue_db  all  reject', `${rule}\nlocal   lane_queue_db  all  reject`), ENTRY, 'add');
  const refused = [
    'hostssl lane_queue_db +lane_agent 100.64.0.0/10 scram-sha-256',
    'host lane_queue_db +some_group 100.64.0.0/10 scram-sha-256',
    'host lane_queue_db all 100.64.0.0/10 scram-sha-256',
    'host all all 127.0.0.1/32 scram-sha-256',
    'host samerole /^lane_ 10.0.0.0/8 md5',
    'host lane_queue_db /^lane_ 10.0.0.0/8 md5',
    'host lane_queue_db @users.txt 10.0.0.0/8 md5',
    'host @dbs.txt lane_agent_evo_x2 10.0.0.0/8 md5',
    'local sameuser all peer',
    'host a,lane_queue_db,b x,"lane_agent_evo_x2" 10.0.0.0/8 md5',
  ];
  for (const rule of refused) assert.throws(() => add(rule), /already has a rule outside .* may let lane_agent_evo_x2 in: /, rule);
  assert.throws(() => add(refused[0]), new RegExp(refused[0].replaceAll('+', '\\+').replaceAll('/', '\\/')));
  const passes = [
    'host all postgres 127.0.0.1/32 scram-sha-256',
    'local all postgres peer',
    'host otherdb all 10.0.0.0/8 md5',
    'host replication all 10.0.0.0/8 md5',
    'host lane_queue_db lane_submit_other 10.0.0.1/32 scram-sha-256',
    'host all all 10.0.0.0/8 reject',
  ];
  for (const rule of passes) assert.doesNotThrow(() => add(rule), rule);
  assert.doesNotThrow(() => lib.editHba(HBA, ENTRY, 'add'), 'the live-like layout is accepted');
  assert.match(HBA, /^host {4}all {2}postgres {2}127\.0\.0\.1\/32 {2}scram-sha-256$/m);
});

test('the per-host lock: a live holder refuses the run, a dead one is reclaimed, and the lock is released afterwards', async () => {
  const dir = makeTmpDir('runner-admin-lock-');
  const file = path.join(dir, 'evo-x2.lock');
  const release = lib.acquireHostLock('evo-x2', { dir, now: NOW });
  assert.deepEqual(JSON.parse(fs.readFileSync(file, 'utf8')), { pid: process.pid, start: '2026-11-24T10:11:12.345Z' });
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  assert.throws(() => lib.acquireHostLock('evo-x2', { dir }), /another lane-runner-admin run for evo-x2 holds .*pid \d+/);
  assert.doesNotThrow(() => lib.acquireHostLock('other-host', { dir })());
  release();
  assert.ok(!fs.existsSync(file));

  const dead = spawnSync(process.execPath, ['-e', '']).pid;
  fs.writeFileSync(file, JSON.stringify({ pid: dead, start: 'long ago' }));
  lib.acquireHostLock('evo-x2', { dir })();
  fs.writeFileSync(file, 'half');
  assert.throws(() => lib.acquireHostLock('evo-x2', { dir }), /unreadable/);
  fs.rmSync(file);

  const world = fakeWorld();
  const held = lib.acquireHostLock('evo-x2', { dir });
  const refused = await cli(world, [...ADD, '--apply'], lib, { lockDir: dir });
  assert.equal(refused.code, 1);
  assert.match(refused.errs, /only one admin may run it at a time/);
  assert.equal(world.calls.length, 0);
  assert.equal((await cli(world, ADD, lib, { lockDir: dir })).code, 0, 'a dry run takes no lock');
  held();
  assert.equal((await cli(world, [...ADD, '--apply'], lib, { lockDir: dir })).code, 0);
  assert.deepEqual(fs.readdirSync(dir), [], 'released after success');
  world.fail.psql = 'x';
  world.roleExists = true;
  assert.equal((await cli(world, [...ADD, '--apply'], lib, { lockDir: dir })).code, 1);
  assert.deepEqual(fs.readdirSync(dir), [], 'released after failure');
});

async function roleRemovedScenario(mod = lib) {
  const world = fakeWorld();
  world.afterRole = () => { world.roleExists = false; world.principalExists = false; };
  const { code, text } = await cli(world, [...ADD, '--apply'], mod);
  assert.equal(code, 1, 'an add whose role vanished must stop');
  assert.match(text, /the role was removed while this add ran/);
  assert.equal(world.scripts.length, 0, 'no file was edited');
  assert.equal(mcOps(world).length, 0, 'no rustfs user was created');
  assert.equal(world.hba, HBA);
}

test('add re-checks the role and principal before the hba, rustfs and train steps', () => roleRemovedScenario());

test('canary: with the per-step role re-check removed, the role-removed scenario fails', async () => {
  const mutant = await importMutant(LIB_FILE, ["if (row?.join('|') !== '1|1') throw", 'if (false) throw']);
  await assert.rejects(roleRemovedScenario(mutant), (err) => err.code === 'ERR_ASSERTION');
});

test('add stops before the rustfs and train steps when the role disappears after the hba step', async () => {
  const world = fakeWorld();
  const inner = world.exec;
  world.exec = async (cmd, args, opts) => {
    const result = await inner(cmd, args, opts);
    if (args[1]?.includes(' flock ') && args[1].endsWith(' hba')) { world.roleExists = false; world.principalExists = false; }
    return result;
  };
  const { code, text } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 1);
  assert.match(text, /the role was removed while this add ran/);
  assert.equal(mcOps(world).length, 0);
  assert.deepEqual(world.scripts.map((script) => script.kind), ['hba']);
});

test('1Password create is re-read, and two items with the title fail loudly naming both', async () => {
  const world = fakeWorld();
  world.extraItems = [{ id: 'item-2', title: 'lane-agent evo-x2' }];
  const { code, text } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 1);
  assert.match(text, /2 1Password items titled "lane-agent evo-x2" exist \(ids item-1, item-2\): another admin run raced this one/);
  assert.ok(!world.calls.some((c) => c.input?.includes('CREATE ROLE')), 'nothing after the secrets step ran');
  const fine = fakeWorld();
  assert.equal((await cli(fine, [...ADD, '--apply'])).code, 0);
  assert.deepEqual(fine.calls.filter((c) => c.cmd === 'op-bot').map((c) => c.args[1]), ['get', 'create', 'list', 'get']);
});

async function reloadUnconfirmedScenario(mod = lib) {
  const dir = makeTmpDir('runner-admin-script-');
  const target = path.join(dir, 'pg_hba.conf');
  fs.writeFileSync(target, HBA);
  const script = (output) => mod.buildEditScript({ content: NEW_HBA, hooks: { sha: 'shasum -a 256', chown: 'true', hbaErrors: 'echo 0', reload: `echo ${output}` } });
  const run = (sha, output) => lib.spawnExec('bash', ['-s', '--', target, sha, 'a:b', '640', 'hba'], { input: script(output) });
  const failed = await run(sha256(HBA), 'f');
  assert.notEqual(failed.code, 0, 'a reload that answers f must not report success');
  assert.doesNotMatch(failed.stdout, /backup=|unchanged=/);
  assert.match(failed.stderr, /reload failed: the file is valid but not live; re-run to reload/);
  assert.equal(fs.readFileSync(target, 'utf8'), NEW_HBA, 'the validated file is kept');
  const empty = await run(sha256(NEW_HBA), '');
  assert.notEqual(empty.code, 0, 'an empty reload answer is not a confirmation');
  const converged = await run(sha256(NEW_HBA), 't');
  assert.equal(converged.code, 0, converged.stderr);
  assert.match(converged.stdout, /^unchanged=/);
}

test('the edit script on a real shell: an unconfirmed reload keeps the valid file, fails, and a re-run converges', () => reloadUnconfirmedScenario());

test('canary: with the reload confirmation removed, the unconfirmed-reload scenario fails', async () => {
  const mutant = await importMutant(LIB_FILE, ['[ "$reloaded" = t ]', 'true']);
  await assert.rejects(reloadUnconfirmedScenario(mutant), (err) => err.code === 'ERR_ASSERTION');
});

test('re-adding a half-removed runner undoes its NOLOGIN: an existing role is altered with LOGIN, the limit and both timeouts', async () => {
  const world = fakeWorld({ roleExists: true, item: itemWith('pw-value-1') });
  world.roleLogin = false;
  const { code } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 0);
  assert.equal(world.roleLogin, true);
  const sql = world.calls.find((c) => c.input?.includes('ALTER ROLE')).input;
  assert.match(sql, /ALTER ROLE "lane_agent_evo_x2" LOGIN PASSWORD 'pw-value-1' CONNECTION LIMIT 3;/);
  assert.match(sql, /SET statement_timeout = '2s';/);
  assert.match(sql, /SET idle_in_transaction_session_timeout = '5s';/);
});

test('the resume command names the script as invoked: relative from the repo, absolute from elsewhere', async () => {
  const resumeFrom = async (extra) => {
    const world = fakeWorld();
    world.fail.psql = 'x';
    return (await cli(world, [...ADD, '--apply'], lib, extra)).text;
  };
  assert.match(await resumeFrom({}), /resume \(every step converges\): node ops\/runner\/lane-runner-admin\.js add evo-x2 --ip 100\.82\.26\.77 --max-claims 14 --apply/);
  assert.match(await resumeFrom({ scriptPath: 'ops/runner/lane-runner-admin.js' }), /: node ops\/runner\/lane-runner-admin\.js add/);
  assert.match(await resumeFrom({ cwd: makeTmpDir('runner-admin-cwd-') }), new RegExp(`: node ${SCRIPT.replaceAll('/', '\\/')} add`));
  assert.match(await resumeFrom({ cwd: path.join(REPO, 'ops') }), /: node runner\/lane-runner-admin\.js add/);
});

test('spawnExec runs without a shell, feeds stdin and merges env', async () => {
  const script = 'process.stdout.write(process.env.X + require("fs").readFileSync(0, "utf8") + process.argv[1])';
  const result = await lib.spawnExec(process.execPath, ['-e', script, '$(echo no)'], { input: 'in', env: { X: 'x' } });
  assert.deepEqual(result, { code: 0, stdout: 'xin$(echo no)', stderr: '' });
});
