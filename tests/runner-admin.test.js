import assert from 'node:assert/strict';
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
  assert.throws(() => lib.editHba(HBA, { ...ENTRY, role: 'lane_agent_mac_grandy', host: 'mac-grandy' }, 'add'), /hand-written/);
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
  assert.match(create, /^CREATE ROLE "lane_agent_evo_x2" LOGIN PASSWORD 'pw''x' IN ROLE lane_agent CONNECTION LIMIT 3;/);
  assert.match(create, /ALTER ROLE "lane_agent_evo_x2" SET statement_timeout = '2s';/);
  assert.match(create, /SET idle_in_transaction_session_timeout = '5s';/);
  assert.match(create, /ld_set_principal\('lane_agent_evo_x2', 'agent', 'evo-x2', ARRAY\['linux','it''s'\]::text\[\], ARRAY\['\*'\]::text\[\], 14\);/);
  const redacted = lib.roleSql({ ...base, tags: [], password: lib.REDACTED, roleExists: true });
  assert.match(redacted, /^ALTER ROLE "lane_agent_evo_x2" PASSWORD '<redacted>';/);
  assert.match(redacted, /ARRAY\[\]::text\[\]/);
  assert.match(lib.removeSql({ role: 'lane_agent_evo_x2', host: 'evo-x2' }), /DELETE FROM lane_dispatch\.principals WHERE login_role = 'lane_agent_evo_x2';\nDELETE FROM lane_dispatch\.runners WHERE host = 'evo-x2';\nSELECT pg_terminate_backend\(pid\) FROM pg_stat_activity WHERE usename = 'lane_agent_evo_x2';\nDROP ROLE IF EXISTS "lane_agent_evo_x2";/);
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

function fakeWorld({ hba = HBA, train = TRAIN, held = [], hbaErrors = 0, item = null, roleExists = false } = {}) {
  const world = { calls: [], hba, train, held, hbaErrors, item, roleExists, templates: [], mcEnvs: [], backupText: null, backup: null, policyFile: null };
  const ok = (stdout = '') => ({ code: 0, stdout, stderr: '' });
  world.exec = async (cmd, rawArgs, { input, env } = {}) => {
    if (cmd === 'ssh') assert.equal(rawArgs.length, 2, `ssh must get the host and one quoted command string, got ${JSON.stringify(rawArgs)}`);
    const args = cmd === 'ssh' ? [rawArgs[0], ...lib.shellSplit(rawArgs[1])] : rawArgs;
    world.calls.push({ cmd, args: [...args], input, env });
    const joined = args.join(' ');
    if (cmd === 'op-bot' && args[1] === 'get') {
      if (!world.item) return { code: 1, stdout: '', stderr: '"lane-agent evo-x2" isn\'t an item in vault BotAccess' };
      return ok(JSON.stringify({ fields: world.item.fields }));
    }
    if (cmd === 'op-bot' && args[1] === 'create') {
      const template = JSON.parse(fs.readFileSync(args[args.indexOf('--template') + 1], 'utf8'));
      world.templates.push(template);
      world.item = template;
      return ok();
    }
    if (cmd === 'mc') {
      world.mcEnvs.push(env);
      if (args[1] === 'policy' && args[2] === 'create') world.policyFile = fs.readFileSync(args[5], 'utf8');
      return ok();
    }
    assert.equal(cmd, 'ssh', `unexpected command ${cmd}`);
    if (joined.endsWith(`cat ${HBA_PATH}`)) return ok(world.hba);
    if (joined.endsWith(`tee ${HBA_PATH}`)) { world.hba = input; return ok(input); }
    if (joined.includes('cp -p') && args.at(-1).startsWith(`${HBA_PATH}.bak-`)) { world.backup = args.at(-1); world.backupText = world.hba; return ok(); }
    if (joined.includes('cp -p') && args.at(-1) === HBA_PATH) { world.hba = world.backupText; return ok(); }
    if (joined.includes('-u train cat')) return ok(world.train);
    if (joined.includes('-u train tee')) { world.train = input; return ok(input); }
    if (joined.includes('-u train cp')) return ok();
    if (joined.endsWith('cat /home/admin/rustfs/rustfs.env')) return ok(`RUSTFS_ACCESS_KEY=${ROOT.accessKey}\nRUSTFS_SECRET_KEY="${ROOT.secretKey}"\n`);
    if (joined.endsWith('homelab-postgres-ca.pem')) return ok('CA-PEM\n');
    if (joined.includes('psql')) {
      if (input.includes('pg_hba_file_rules')) return ok(`${world.hbaErrors}\n`);
      if (input.includes('FROM pg_roles')) return ok(world.roleExists ? '1\n' : '0\n');
      if (input.includes('lane_dispatch.tickets')) return ok(world.held.map((t) => t.join('|')).join('\n'));
      return ok();
    }
    throw new Error(`unhandled call: ${cmd} ${joined}`);
  };
  return world;
}

async function cli(world, argv, mod = lib) {
  const lines = [];
  const errs = [];
  const code = await mod.runCli(argv, { exec: world.exec, out: (t) => lines.push(t), err: (t) => errs.push(t), now: NOW });
  return { code, text: lines.join('\n'), errs: errs.join('\n') };
}

const ADD = ['add', 'evo-x2', '--ip', '100.82.26.77', '--max-claims', '14'];
const itemField = (world, match) => world.item.fields.find(match).value;
const passwordOf = (world) => itemField(world, (f) => f.id === 'password');
const secretKeyOf = (world) => itemField(world, (f) => f.label === 'rustfs_secret_key');
const accessKeyOf = (world) => itemField(world, (f) => f.label === 'rustfs_access_key');
const heldItem = { fields: [
  { id: 'password', purpose: 'PASSWORD', value: 'pw-value-1' },
  { label: 'rustfs_access_key', value: 'ACCESSKEY0123456789A' },
  { label: 'rustfs_secret_key', value: 'secret-value-2' },
] };

test("editHba adds the role only to the lane-logins reject lines, never to other apps' reject lines or the final catch-all", () => {
  const entry = { role: 'lane_agent_evo_x2', host: 'evo-x2', ip: '100.82.26.77' };
  const added = lib.editHba(HBA, entry, 'add');
  for (const line of OTHER_REJECTS) assert.ok(added.split('\n').includes(line), `untouched: ${line}`);
  assert.equal(added.split('\n').filter((l) => l.includes('lane_agent_evo_x2') && /reject/.test(l)).length, 2);
  assert.equal(lib.editHba(added, entry, 'remove'), HBA);
});

test('shellQuote survives the remote shell: a psql field separator stays one word, not a pipe', () => {
  const words = ['psql', '-F', '|', "it's", 'a b', '-tAq'];
  assert.deepEqual(lib.shellSplit(words.map(lib.shellQuote).join(' ')), words);
  assert.equal(lib.shellQuote('|'), "'|'");
});

test('a dry run changes nothing, prints every step with the password redacted, and leaks no secret', async () => {
  const world = fakeWorld();
  const { code, text } = await cli(world, ADD);
  assert.equal(code, 0);
  for (const kind of ['secrets', 'postgres', 'hba', 'rustfs', 'train']) assert.match(text, new RegExp(`\\[${kind}\\]`));
  assert.match(text, /CREATE ROLE "lane_agent_evo_x2" LOGIN PASSWORD '<redacted>' IN ROLE lane_agent CONNECTION LIMIT 3;/);
  assert.match(text, /\+hostssl lane_queue_db {2}lane_agent_evo_x2 {2}100\.82\.26\.77\/32 {2}scram-sha-256/);
  assert.match(text, /provision\.sh/);
  assert.match(text, /nothing changed/);
  assert.equal(world.hba, HBA);
  assert.equal(world.train, TRAIN);
  assert.equal(world.templates.length, 0);
  assert.equal(world.calls.filter((c) => c.cmd === 'mc').length, 0);
  assert.ok(!world.calls.some((c) => /\b(tee|cp)\b/.test(c.args.join(' '))));
});

test('apply runs the steps in order, keeps every secret out of argv and output, and stores them in 1Password', async () => {
  const world = fakeWorld();
  const dir = path.join(makeTmpDir('runner-admin-out-'), 'worker');
  const { code, text } = await cli(world, [...ADD, '--out', dir, '--apply']);
  assert.equal(code, 0, text);
  const order = world.calls.map((c) => {
    if (c.cmd === 'op-bot') return `op-bot ${c.args[1]}`;
    if (c.cmd === 'mc') return `mc ${c.args.slice(0, 3).join(' ')}`;
    return c.input?.includes('pg_reload_conf') ? 'reload' : null;
  }).filter(Boolean);
  assert.deepEqual(order, ['op-bot get', 'op-bot create', 'reload', 'mc admin user add', 'mc admin policy create', 'mc admin policy attach']);

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
  assert.equal(world.backup, `${HBA_PATH}.bak-lane-runner-admin-20261124T101112Z`);
  assert.match(world.hba, /lane_agent_evo_x2 {2}100\.82\.26\.77\/32/);
  assert.ok(lib.trainHasRunner(world.train, 'evo-x2'));

  const argvOf = (calls) => calls.flatMap((c) => c.args).join('\n');
  const userAdd = world.calls.find((c) => c.cmd === 'mc' && c.args[2] === 'add');
  assert.deepEqual(userAdd.args.slice(4), [accessKey, secretKey]);
  const others = world.calls.filter((c) => c !== userAdd);
  for (const secret of [password, secretKey, ROOT.secretKey]) {
    assert.ok(!argvOf(others).includes(secret), 'secret in argv');
    assert.ok(!text.includes(secret), 'secret in output');
  }
  assert.ok(!argvOf([userAdd]).includes(password) && !argvOf([userAdd]).includes(ROOT.secretKey));

  assert.equal(fs.statSync(dir).mode & 0o777, 0o700);
  for (const name of ['config.json', 'password', 'rustfs.env', 'ca.pem']) assert.equal(fs.statSync(path.join(dir, name)).mode & 0o777, 0o600, name);
  assert.equal(fs.readFileSync(path.join(dir, 'password'), 'utf8'), password);
  assert.equal(fs.readFileSync(path.join(dir, 'rustfs.env'), 'utf8'), `AWS_ACCESS_KEY_ID=${accessKey}\nAWS_SECRET_ACCESS_KEY=${secretKey}\n`);
  assert.equal(fs.readFileSync(path.join(dir, 'ca.pem'), 'utf8'), 'CA-PEM\n');
});

test('re-running add converges: the item is reused, the role is altered, and nothing is rewritten', async () => {
  const world = fakeWorld();
  await cli(world, [...ADD, '--apply']);
  const password = passwordOf(world);
  world.roleExists = true;
  world.calls.length = 0;
  const { code } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 0);
  assert.equal(world.templates.length, 1);
  assert.match(world.calls.find((c) => c.input?.includes('ALTER ROLE')).input, new RegExp(`^ALTER ROLE "lane_agent_evo_x2" PASSWORD '${password}';`));
  assert.ok(!world.calls.some((c) => /\btee\b/.test(c.args.join(' '))), 'nothing to write the second time');
  assert.equal(world.hba.split('BEGIN lane-runner-admin evo-x2').length, 2);
});

test('a psql error that echoes the statement is scrubbed of the password, and the run stops with a resume command', async () => {
  const world = fakeWorld();
  const inner = world.exec;
  world.exec = async (cmd, args, opts) => (opts?.input?.includes('CREATE ROLE')
    ? { code: 3, stdout: '', stderr: `ERROR: boom\nLINE 1: ${opts.input}` }
    : inner(cmd, args, opts));
  const { code, text, errs } = await cli(world, [...ADD, '--apply']);
  assert.equal(code, 1);
  assert.match(text, /FAILED: create role and principal failed/);
  assert.match(text, /resume \(every step converges\): lane-runner-admin add evo-x2 --ip 100\.82\.26\.77 --max-claims 14 --apply/);
  assert.match(text, /ok {2}1Password/);
  assert.ok(!text.includes(passwordOf(world)) && !errs.includes(passwordOf(world)));
  assert.ok(!world.calls.some((c) => c.cmd === 'mc'), 'stops at the first failure');
});

async function hbaRejectedScenario(mod = lib) {
  const world = fakeWorld({ hbaErrors: 2 });
  const { code, text } = await cli(world, [...ADD, '--apply'], mod);
  assert.equal(code, 1, 'a pg_hba the server rejects must fail the run');
  assert.equal(world.hba, HBA, 'the backup is restored');
  assert.match(text, /pg_hba\.conf was rejected \(2 rule errors in pg_hba_file_rules\); restored \/etc\/postgresql\/17\/main\/pg_hba\.conf\.bak-lane-runner-admin-/);
  assert.ok(!world.calls.some((c) => c.input?.includes('pg_reload_conf')), 'a rejected file is never reloaded');
  assert.ok(!world.calls.some((c) => c.cmd === 'mc'), 'nothing after the hba step ran');
}

test('a pg_hba the server rejects is restored from the backup and never reloaded', () => hbaRejectedScenario());

test('canary: with the revert on pg_hba_file_rules errors removed, the restore scenario fails', async () => {
  const mutant = await importMutant(LIB_FILE, ['if (errorCount > 0) return', 'if (false) return']);
  await assert.rejects(hbaRejectedScenario(mutant), (err) => err.code === 'ERR_ASSERTION');
});

test('remove refuses while the host holds claimed or running tickets, and --force proceeds', async () => {
  const held = [['11111111-aaaa', 'e2e'], ['22222222-bbbb', 'unit']];
  const registered = () => ({ hba: lib.editHba(HBA, ENTRY, 'add'), train: lib.editTrainRunners(TRAIN, 'evo-x2', 'add'), held, item: heldItem });
  const refused = fakeWorld(registered());
  const preview = await cli(refused, ['remove', 'evo-x2']);
  assert.match(preview.text, /2 held: 11111111-aaaa \(e2e\), 22222222-bbbb \(unit\)\s+remove would refuse without --force/);
  const result = await cli(refused, ['remove', 'evo-x2', '--apply']);
  assert.equal(result.code, 1);
  assert.match(result.text, /11111111-aaaa \(e2e\), 22222222-bbbb \(unit\); re-run with --force/);
  assert.ok(!refused.calls.some((c) => c.input?.includes('DROP ROLE')));

  const forced = fakeWorld(registered());
  const done = await cli(forced, ['remove', 'evo-x2', '--force', '--apply']);
  assert.equal(done.code, 0, done.text);
  assert.equal(forced.hba, HBA);
  assert.deepEqual(JSON.parse(forced.train).runners.map((r) => r.name), ['wintop', 'mac']);
  assert.deepEqual(forced.calls.filter((c) => c.cmd === 'mc').map((c) => c.args.slice(0, 3).join(' ')), ['admin policy detach', 'admin user remove', 'admin policy remove']);
  const dropAt = forced.calls.findIndex((c) => c.input?.includes('DROP ROLE'));
  const hbaAt = forced.calls.findIndex((c) => c.args.join(' ').endsWith(`tee ${HBA_PATH}`));
  assert.ok(dropAt >= 0 && hbaAt > dropAt);
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
  assert.ok(!world.calls.some((c) => /\b(tee|cp)\b/.test(c.args.join(' ')) || c.input?.includes('DELETE')));
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

test('spawnExec runs without a shell, feeds stdin and merges env', async () => {
  const script = 'process.stdout.write(process.env.X + require("fs").readFileSync(0, "utf8") + process.argv[1])';
  const result = await lib.spawnExec(process.execPath, ['-e', script, '$(echo no)'], { input: 'in', env: { X: 'x' } });
  assert.deepEqual(result, { code: 0, stdout: 'xin$(echo no)', stderr: '' });
});
