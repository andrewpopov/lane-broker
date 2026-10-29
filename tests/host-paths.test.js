import test from 'node:test';
import assert from 'node:assert/strict';
import { hostOnlyPaths, hostPathsRefusal } from '../src/host-paths.js';

// BRAIN-721: fake filesystem -- every listed path "exists" and is its own realpath.
const ROOT = '/Volumes/Lexar/worktrees/r/w';
const detect = (argv, { cwd = ROOT, present = [] } = {}) =>
  hostOnlyPaths(argv, { worktreeRoot: ROOT, cwd, home: '/Users/a', exists: (p) => present.includes(p), realpath: (p) => p });

test('an existing host-only script path is flagged as outside the worktree', () => {
  const found = detect(['sh', '/private/tmp/x/foo.sh'], { present: ['/private/tmp/x/foo.sh'] });
  assert.deepEqual(found, [{ path: '/private/tmp/x/foo.sh', insideWorktree: false, suggestion: null }]);
});

test('a path inside sh -c is inside the worktree with a cwd-relative suggestion', () => {
  const found = detect(['sh', '-c', `cd ${ROOT}/sub && npm test`], { present: [`${ROOT}/sub`] });
  assert.deepEqual(found, [{ path: `${ROOT}/sub`, insideWorktree: true, suggestion: './sub' }]);
});

test('the suggestion is relative to the command cwd, not the worktree root', () => {
  const present = [`${ROOT}/a/b/f.js`, ROOT];
  assert.equal(detect(['node', `${ROOT}/a/b/f.js`], { cwd: `${ROOT}/a/c`, present })[0].suggestion, '../b/f.js');
  assert.equal(detect(['sh', `${ROOT}/check.sh`], { present: [`${ROOT}/check.sh`] })[0].suggestion, './check.sh');
  assert.equal(detect(['ls', ROOT], { cwd: ROOT, present })[0].suggestion, '.');
});

test('a --flag=/abs value is checked', () => {
  assert.equal(detect(['tool', '--out=/Users/a/x'], { present: ['/Users/a/x'] }).length, 1);
});

test('a host path that does not exist here is not flagged', () => {
  assert.deepEqual(detect(['cat', '/tmp/lane-result']), []);
});

test('unexpanded $HOME and ~ are left to the runner shell', () => {
  assert.deepEqual(detect(['sh', '-c', 'ls $HOME/.cache ~/x'], { present: ['/Users/a/.cache', '/Users/a/x'] }), []);
});

test('system binaries, bare commands, relative paths and URLs are not flagged', () => {
  const present = ['/usr/bin/env', '/bin/sh', '/Users/x', 'node', 'rel/path'];
  assert.deepEqual(detect(['/usr/bin/env', '/bin/sh', 'node', 'rel/path', 'https://h/Users/x'], { present }), []);
});

test('the refusal text names the path and omits --local on a dispatch-only host', () => {
  const found = [{ path: '/tmp/x', insideWorktree: false, suggestion: null }];
  assert.match(hostPathsRefusal('default', found, { dispatchOnly: false })[0], /names \/tmp\/x, which exists only on this machine.*--local --local-reason/);
  assert.doesNotMatch(hostPathsRefusal('default', found, { dispatchOnly: true })[0], /--local/);
});

test('an output redirect target is not flagged, an input redirect still is', () => {
  const present = ['/tmp/test.out', '/tmp/in.txt'];
  assert.deepEqual(detect(['sh', '-c', 'npm test > /tmp/test.out 2>/tmp/test.out &>/tmp/test.out >>/tmp/test.out'], { present }), []);
  assert.deepEqual(detect(['sh', '-c', 'cat < /tmp/in.txt > /tmp/test.out'], { present }).map((f) => f.path), ['/tmp/in.txt']);
});

test('an env wrapper around sh -c is inspected like a bare shell', () => {
  const found = detect(['env', '-i', 'A=1', 'sh', '-c', 'bash /tmp/x.sh'], { present: ['/tmp/x.sh'] });
  assert.deepEqual(found.map((f) => f.path), ['/tmp/x.sh']);
});
