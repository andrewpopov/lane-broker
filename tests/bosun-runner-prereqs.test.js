import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeTmpDir } from './helpers/tmp.js';

const script = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'bosun', 'runner-prereqs.sh');
const TOOLS = ['bash', 'timeout', 'flock', 'python3', 'npx'];

const stubBody = (tool, browserDir) => ({
  timeout: 'echo "timeout (GNU coreutils) 9.4"',
  npx: `echo "  Install location: ${browserDir}"`,
}[tool] ?? 'exit 0');

const makeStubDir = (omit = [], overrides = {}) => {
  const dir = makeTmpDir('runner-prereqs-stubs-');
  const browserDir = makeTmpDir('runner-prereqs-browsers-');
  for (const tool of TOOLS.filter((name) => !omit.includes(name))) {
    const file = path.join(dir, tool);
    fs.writeFileSync(file, `#!/bin/sh\n${overrides[tool] ?? stubBody(tool, browserDir)}\n`, { mode: 0o755 });
  }
  return dir;
};

const dryRun = (stubDir, uname = 'Linux', tmpdir = makeTmpDir('runner-prereqs-tmp-')) => spawnSync('/bin/bash', [script, '--dry-run', '--path', stubDir], {
  encoding: 'utf8',
  env: { PATH: '/usr/bin:/bin', RUNNER_PREREQS_UNAME: uname, TMPDIR: tmpdir },
});

test('linux: every prerequisite present passes the dry run and creates nothing', () => {
  const stubDir = makeStubDir();
  const before = fs.readdirSync(stubDir).sort();
  const tmpdir = makeTmpDir('runner-prereqs-tmp-');
  const result = dryRun(stubDir, 'Linux', tmpdir);
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(fs.readdirSync(tmpdir), []);
  assert.match(result.stdout, /^ok: every lane-runner prerequisite/);
  assert.deepEqual(fs.readdirSync(stubDir).sort(), before);
});

test('linux: a missing flock exits 1 naming it and its apt fix', () => {
  const result = dryRun(makeStubDir(['flock']));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /missing: flock \(apt install util-linux\)/);
});

test('linux: a uutils timeout (Ubuntu 26.04) that takes -k passes; the banner does not matter (PKG-431)', () => {
  const result = dryRun(makeStubDir([], { timeout: '[ "$1" = --version ] && echo "timeout (uutils coreutils) 0.10.0"; exit 0' }));
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /^ok: every lane-runner prerequisite/);
});

test('linux: a timeout that refuses -k exits 1 naming it', () => {
  const result = dryRun(makeStubDir([], { timeout: '[ "$1" = -k ] && exit 125; echo "timeout (GNU coreutils) 9.4"' }));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /missing: timeout with -k \(apt install coreutils\)/);
});

test('linux: a missing npx exits 1 naming node', () => {
  const result = dryRun(makeStubDir(['npx']));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /missing: npx \(node\).*apt install nodejs npm/);
});

test('an unsupported platform exits 2 with a clear message', () => {
  const result = dryRun(makeStubDir(), 'FreeBSD');
  assert.equal(result.status, 2);
  assert.match(result.stderr, /unsupported platform FreeBSD/);
});
