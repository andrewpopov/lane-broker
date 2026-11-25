import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeTmpDir } from './helpers/tmp.js';

const script = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'bosun', 'runner-prereqs.sh');
const TOOLS = ['bash', 'timeout', 'flock', 'python3', 'npx'];

const stubBody = (tool, browserDir, timeoutVersion = 'timeout (GNU coreutils) 9.4') => ({
  timeout: `echo "${timeoutVersion}"`,
  npx: `echo "  Install location: ${browserDir}"`,
}[tool] ?? 'exit 0');

const makeStubDir = (omit = [], timeoutVersion = undefined) => {
  const dir = makeTmpDir('runner-prereqs-stubs-');
  const browserDir = makeTmpDir('runner-prereqs-browsers-');
  for (const tool of TOOLS.filter((name) => !omit.includes(name))) {
    const file = path.join(dir, tool);
    fs.writeFileSync(file, `#!/bin/sh\n${stubBody(tool, browserDir, timeoutVersion)}\n`, { mode: 0o755 });
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

test('linux: the uutils timeout (Ubuntu 25.10+) passes; a BusyBox one is refused by name', () => {
  assert.equal(dryRun(makeStubDir([], 'timeout (uutils coreutils) 0.10.0')).status, 0);
  const busybox = dryRun(makeStubDir([], 'BusyBox v1.36.1 multi-call binary.'));
  assert.equal(busybox.status, 1);
  assert.match(busybox.stderr, /missing: coreutils timeout \(apt install coreutils\)/);
});
