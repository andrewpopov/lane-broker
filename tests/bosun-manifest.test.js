import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { CORE_SCHEMA, load } from 'js-yaml';
import { makeTmpDir } from './helpers/tmp.js';
import { expandSelf, validateBosunManifest } from './helpers/bosun-manifest-schema.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifestText = fs.readFileSync(path.join(root, 'bosun', 'lane.yaml'), 'utf8');
const loadManifest = () => load(manifestText, { schema: CORE_SCHEMA });
const ASSETS = ['runner-prereqs.sh', 'Brewfile.lane-runner', 'requirements.lane-runner.txt', 'playwright.lane-runner.txt'];

const mutated = (edit) => { const manifest = loadManifest(); edit(manifest); return validateBosunManifest(manifest, 'lane'); };

test('bosun/lane.yaml satisfies the documented manifest schema', () => {
  assert.deepEqual(validateBosunManifest(loadManifest(), 'lane'), []);
});

test('the manifest runs the agent, drains by exec hook, and declares what it needs of the daemon', () => {
  const manifest = loadManifest();
  assert.deepEqual(manifest.run.argv, ['${node}', '${self}/bin/lane.js', 'agent']);
  assert.deepEqual(manifest.drain, { kind: 'exec', argv: ['${node}', '${self}/bin/lane.js', 'drain-hook'], ack: 'exec', modes: ['wait', 'interrupt'], hookTimeoutSeconds: 60 });
  assert.deepEqual(manifest.compat.capabilities, ['exec-drain']);
  assert.match(manifest.compat.bosun, /^>=\d+\.\d+\.\d+$/);
});

test('survivesStop is an overridable instance parameter, not a hard-coded value', () => {
  const { params, drain } = loadManifest();
  assert.equal(params.survivesStop.overridable, true);
  assert.equal(params.survivesStop.type, 'string');
  assert.equal(params.survivesStop.default, 'false', 'interrupt stays off until the unit-stop host gate has run');
  assert.equal(Object.hasOwn(drain, 'survivesStop'), false);
  assert.match(mutated((manifest) => { manifest.drain.survivesStop = true; }).join('\n'), /unknown key drain\.survivesStop/);
});

test('every command resolves under ${self}, the store path, and names a file the package ships', () => {
  const manifest = loadManifest();
  const store = makeTmpDir('lane-store-path-');
  fs.cpSync(path.join(root, 'bin'), path.join(store, 'bin'), { recursive: true });
  fs.cpSync(path.join(root, 'bosun'), path.join(store, 'bosun'), { recursive: true });
  const params = Object.fromEntries(Object.entries(manifest.params).map(([name, param]) => [name, param.default]));
  const setup = manifest.setup.argv.map((arg) => expandSelf(arg, store, process.execPath, params));
  assert.equal(setup[0], path.join(store, 'bosun', 'runner-prereqs.sh'));
  assert.ok(fs.statSync(setup[0]).mode & 0o111, 'setup script is executable');
  for (const argv of [manifest.run.argv, manifest.drain.argv]) assert.ok(fs.existsSync(expandSelf(argv[1], store, process.execPath)), `${argv[1]} exists`);
  const usage = spawnSync('/bin/bash', [setup[0], '--bogus'], { encoding: 'utf8', env: { PATH: '/usr/bin:/bin' } });
  assert.equal(usage.status, 2);
  assert.match(usage.stderr, /usage: .*runner-prereqs\.sh \(--check \| --install \| --dry-run \| --apply\) \[--path/);
});

test('the manifest covers darwin and linux, and the old script name still reaches the same usage', () => {
  assert.deepEqual(loadManifest().platforms, ['darwin', 'linux']);
  const wrapper = spawnSync('/bin/bash', [path.join(root, 'bosun', 'mac-runner-prereqs.sh'), '--bogus'], { encoding: 'utf8', env: { PATH: '/usr/bin:/bin' } });
  assert.equal(wrapper.status, 2);
  assert.match(wrapper.stderr, /usage: .*runner-prereqs\.sh \(--check \| --install \| --dry-run \| --apply\)/);
});

test('the four lane-runner assets ship in the package and the script reads its siblings', () => {
  const { files } = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
  assert.ok(files.includes('bosun'));
  for (const asset of ASSETS) assert.ok(fs.existsSync(path.join(root, 'bosun', asset)), asset);
  const script = fs.readFileSync(path.join(root, 'bosun', 'runner-prereqs.sh'), 'utf8');
  for (const asset of ASSETS.slice(1)) assert.ok(script.includes(`"$here/${asset}"`), `script reads $here/${asset}`);
});

test('canary: a hook argv that does not name ${self} or ${node} fails the validator by name', () => {
  const cases = [
    ['run bare lane', (m) => { m.run.argv = ['lane', 'agent']; }, /run\.argv\[0\] is lane: a store package's command must be/],
    ['run bare node', (m) => { m.run.argv = ['node', '${self}/bin/lane.js', 'agent']; }, /run\.argv\[0\] is node/],
    ['run script outside self', (m) => { m.run.argv = ['${node}', 'bin/lane.js', 'agent']; }, /run\.argv runs bin\/lane\.js under \$\{node\}/],
    ['drain bare lane', (m) => { m.drain.argv = ['lane', 'drain-hook']; }, /drain\.argv\[0\] is lane/],
    ['drain bare node', (m) => { m.drain.argv = ['node', 'bin/lane.js', 'drain-hook']; }, /drain\.argv\[0\] is node/],
    ['setup from PATH', (m) => { m.setup.argv = ['runner-prereqs.sh']; }, /setup\.argv\[0\] is runner-prereqs\.sh/],
    ['setup escapes the package', (m) => { m.setup.argv[0] = '${self}/../runner-prereqs.sh'; }, /setup\.argv\[0\] is \$\{self\}\/\.\.\//],
  ];
  for (const [label, edit, expected] of cases) assert.match(mutated(edit).join('\n'), expected, label);
});
