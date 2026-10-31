import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gitFixture } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { selectAffected, renderSelection } from '../scripts/affected-tests.js';

function write(repo, file, content = '') {
  fs.mkdirSync(path.dirname(path.join(repo, file)), { recursive: true });
  fs.writeFileSync(path.join(repo, file), content);
}

function commitAll(repo, message) {
  gitFixture(['add', '-A'], repo);
  gitFixture(['commit', '-q', '-m', message], repo);
}

/** A repo whose origin/main is the base commit and whose HEAD is a feature branch off it. */
function fixtureRepo(t, baseFiles = {}) {
  const repo = makeTmpDir('affected-tests-', t);
  gitFixture(['init', '-q'], repo);
  write(repo, 'src/widget.js', 'export const widget = 1;\n');
  write(repo, 'src/gadget.js', 'export const gadget = 1;\n');
  write(repo, 'tests/widget.test.js', 'test();\n');
  write(repo, 'tests/uses-gadget.test.js', "import { gadget } from '../src/gadget.js';\n");
  write(repo, 'tests/untouched.test.js', "import { x } from './helpers.js';\n");
  for (const [file, content] of Object.entries(baseFiles)) write(repo, file, content);
  commitAll(repo, 'base');
  gitFixture(['update-ref', 'refs/remotes/origin/main', 'HEAD'], repo);
  gitFixture(['checkout', '-q', '-b', 'feature'], repo);
  return repo;
}

test('a changed src selects its importer and its name-matched test', (t) => {
  const repo = fixtureRepo(t);
  write(repo, 'src/gadget.js', 'export const gadget = 2;\n');
  write(repo, 'src/widget.js', 'export const widget = 2;\n');
  commitAll(repo, 'edit src');
  assert.deepEqual(selectAffected(repo), { files: ['tests/uses-gadget.test.js', 'tests/widget.test.js'] });
});

test('a changed test selects itself', (t) => {
  const repo = fixtureRepo(t);
  write(repo, 'tests/untouched.test.js', 'test(1);\n');
  commitAll(repo, 'edit test');
  assert.deepEqual(selectAffected(repo), { files: ['tests/untouched.test.js'] });
});

test('a deleted test is not selected', (t) => {
  const repo = fixtureRepo(t);
  fs.rmSync(path.join(repo, 'tests/untouched.test.js'));
  commitAll(repo, 'delete test');
  const selection = selectAffected(repo);
  assert.deepEqual(selection, { files: [] });
  assert.equal(renderSelection(selection), 'affected: no related tests');
});

test('more than 20 selected files prints the skip reason', (t) => {
  const repo = fixtureRepo(t);
  for (let i = 0; i < 21; i++) write(repo, `tests/bulk-${i}.test.js`, 'test();\n');
  commitAll(repo, 'many tests');
  assert.match(renderSelection(selectAffected(repo)), /^affected: 21 tests selected \(limit 20\), skipping unit tests \(the train runs them\)$/);
});

test('no merge-base prints the skip reason', (t) => {
  const repo = fixtureRepo(t);
  gitFixture(['update-ref', '-d', 'refs/remotes/origin/main'], repo);
  assert.equal(renderSelection(selectAffected(repo)), 'affected: no merge-base with origin/main, skipping unit tests (the train runs them)');
});

test('a dependency change skips but a scripts-only package.json change does not', (t) => {
  const repo = fixtureRepo(t, { 'package.json': '{"scripts":{},"dependencies":{}}\n' });
  write(repo, 'package.json', '{"scripts":{"a":"b"},"dependencies":{}}\n');
  commitAll(repo, 'scripts only');
  assert.deepEqual(selectAffected(repo), { files: [] });
  write(repo, 'package.json', '{"scripts":{"a":"b"},"dependencies":{"pg":"1"}}\n');
  commitAll(repo, 'add dep');
  assert.match(renderSelection(selectAffected(repo)), /^affected: package dependencies changed, skipping/);
});
