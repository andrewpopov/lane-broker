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

// PKG-464: --wheels-only checks python3 and the PyPI wheels and nothing else.
const wheelsStubDir = ({ python = 'exit 0', calls } = {}) => {
  const dir = makeTmpDir('runner-prereqs-wheels-');
  fs.writeFileSync(path.join(dir, 'python3'), `#!/bin/sh\n${calls ? `echo "python3 $*" >> ${calls}\n` : ''}${python}\n`, { mode: 0o755 });
  for (const tool of ['npx', 'brew']) fs.writeFileSync(path.join(dir, tool), `#!/bin/sh\n${calls ? `echo "${tool} $*" >> ${calls}\n` : ''}exit 1\n`, { mode: 0o755 });
  return dir;
};

const runScript = (args, stubDir, uname = 'Linux') => spawnSync('/bin/bash', [script, ...args, '--path', stubDir], {
  encoding: 'utf8',
  env: { PATH: '/usr/bin:/bin', RUNNER_PREREQS_UNAME: uname, TMPDIR: makeTmpDir('runner-prereqs-tmp-') },
});

for (const uname of ['Linux', 'Darwin']) {
  test(`${uname}: --wheels-only passes with python3 and its wheels while bash, timeout, flock, npx and the browsers are missing`, () => {
    const result = runScript(['--dry-run', '--wheels-only'], wheelsStubDir(), uname);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /^ok: every lane Python wheel prerequisite/);
    assert.equal(result.stderr, '');
  });
}

test('--wheels-only names the missing numpy and Pillow', () => {
  const result = runScript(['--dry-run', '--wheels-only'], wheelsStubDir({ python: 'case "$2" in *numpy*|*PIL*) exit 1 ;; esac\nexit 0' }));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /missing: python3 numpy \(pip numpy\)/);
  assert.match(result.stderr, /missing: python3 Pillow \(pip pillow\)/);
  assert.match(result.stderr, /2 lane Python wheel prerequisite\(s\) missing/);
  assert.doesNotMatch(result.stderr, /playwright|flock|timeout|bash/);
});

test('--wheels-only without python3 >= 3.11 fails naming it', () => {
  const result = runScript(['--dry-run', '--wheels-only'], wheelsStubDir({ python: 'case "$2" in *tomllib*) exit 1 ;; esac\nexit 0' }));
  assert.equal(result.status, 1);
  assert.match(result.stderr, /missing: python3 >= 3.11 with tomllib/);
});

for (const uname of ['Linux', 'Darwin']) {
  test(`${uname}: --wheels-only --apply pip-installs the wheel list and never runs brew or npx`, () => {
    const calls = path.join(makeTmpDir('runner-prereqs-calls-'), 'calls.log');
    const result = runScript(['--apply', '--wheels-only'], wheelsStubDir({ calls }), uname);
    assert.equal(result.status, 0, result.stderr);
    const log = fs.readFileSync(calls, 'utf8');
    assert.match(log, /^python3 -m pip install .*--user .*--only-binary=:all: -r .*requirements\.lane-runner\.txt$/m);
    assert.doesNotMatch(log, /^(npx|brew) /m);
  });
}

test('the full mode still requires the runner toolchain', () => {
  const result = runScript(['--dry-run'], wheelsStubDir());
  assert.equal(result.status, 1);
  assert.match(result.stderr, /lane-runner prerequisite\(s\) missing/);
});
