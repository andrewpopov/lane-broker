import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  collectInstallPathReferences,
  installPathRoles,
  materializeFromStore,
  publishToStore,
  readRelocation,
  relocateRestoredTree,
  entryDir,
  removeTree,
} from '../src/deps-cache.js';

/** BRAIN-402: a tree naming its install path is stored with a record of where, and rewritten to the new path on restore. */

const made = [];
test.after(() => {
  for (const d of made) removeTree(d);
});
const tmpDir = () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'deps-relocate-'));
  made.push(d);
  return d;
};
const newKey = () => crypto.randomBytes(32).toString('hex');
const rolesFor = (work) => [{ role: 'workDir', path: work }];
const write = (tree, rel, content, mode = 0o644) => {
  fs.mkdirSync(path.dirname(path.join(tree, rel)), { recursive: true });
  fs.writeFileSync(path.join(tree, rel), content, { mode });
};

/** Publish `tree` (installed under `oldWork`), restore it, and relocate it for `newWork`. Returns { restored, relocation, run }. */
function roundTrip(tree, oldWork, newWork, { tamper } = {}) {
  const collected = collectInstallPathReferences(tree, rolesFor(oldWork));
  assert.ok(collected.relocation, 'relocatable');
  const cacheRoot = tmpDir();
  const key = newKey();
  assert.equal(publishToStore(cacheRoot, key, tree, collected.relocation).published, true);
  if (tamper) tamper(path.join(entryDir(cacheRoot, key), 'node_modules'), path.join(entryDir(cacheRoot, key), 'meta.json'));
  const restored = path.join(tmpDir(), 'node_modules');
  materializeFromStore(cacheRoot, key, restored);
  const run = () => relocateRestoredTree(restored, readRelocation(cacheRoot, key), { workDir: newWork });
  return { restored, relocation: collected.relocation, run };
}

const OLD = '/var/tmp/lb-a/tickets/t1/work';
const NEW = '/var/tmp/lb-b/tickets/t2/work';

test('a text file and a symlink naming the install path are recorded, and restored to a different path', () => {
  const tree = path.join(tmpDir(), 'node_modules');
  write(tree, '.prisma/client/edge.js', `module.exports = { dir: "${OLD}/node_modules/.prisma/client" };\n`, 0o755);
  write(tree, 'clean/index.js', 'module.exports = 1;\n');
  fs.symlinkSync(`${OLD}/node_modules/clean`, path.join(tree, 'link'));
  fs.symlinkSync('../clean', path.join(tree, '.prisma', 'relative-link'));

  const { restored, relocation, run } = roundTrip(tree, OLD, NEW);
  assert.deepEqual(relocation.prefixes, [{ role: 'workDir', path: OLD }]);
  assert.deepEqual(relocation.entries.map((e) => `${e.kind}:${e.relPath}`).sort(), ['symlink:link', 'text:.prisma/client/edge.js']);
  run();

  assert.equal(fs.readFileSync(path.join(restored, '.prisma/client/edge.js'), 'utf8'), `module.exports = { dir: "${NEW}/node_modules/.prisma/client" };\n`);
  assert.equal(fs.statSync(path.join(restored, '.prisma/client/edge.js')).mode & 0o7777, 0o755, 'mode kept');
  assert.equal(fs.readlinkSync(path.join(restored, 'link')), `${NEW}/node_modules/clean`);
  assert.equal(fs.readlinkSync(path.join(restored, '.prisma', 'relative-link')), '../clean', 'an unrecorded relative link is untouched');
  assert.equal(fs.readFileSync(path.join(restored, 'clean/index.js'), 'utf8'), 'module.exports = 1;\n');
  assert.deepEqual(fs.readdirSync(path.join(restored, '.prisma/client')), ['edge.js'], 'no temp file left behind');
});

test('a binary file (NUL byte) naming the install path makes the whole tree non-relocatable', () => {
  const tree = path.join(tmpDir(), 'node_modules');
  write(tree, 'text.js', `"${OLD}"`);
  write(tree, 'native/addon.node', Buffer.concat([Buffer.from([0x7f, 0x45, 0, 0x4c]), Buffer.from(OLD)]));
  assert.deepEqual(collectInstallPathReferences(tree, rolesFor(OLD)), { binary: 'native/addon.node' });
});

test('a NUL byte anywhere in the file makes it binary, even past the first chunk', () => {
  const tree = path.join(tmpDir(), 'node_modules');
  const big = Buffer.alloc(2 * 1024 * 1024 + 10, 0x41);
  Buffer.from(OLD).copy(big, 5);
  big[big.length - 1] = 0;
  write(tree, 'big.bin', big);
  assert.deepEqual(collectInstallPathReferences(tree, rolesFor(OLD)), { binary: 'big.bin' });
});

test('a tree that names no install path records an empty relocation', () => {
  const tree = path.join(tmpDir(), 'node_modules');
  write(tree, 'a.js', 'x');
  assert.deepEqual(collectInstallPathReferences(tree, rolesFor(OLD)), { relocation: { prefixes: [], entries: [] } });
});

test('overlapping prefixes: the new path extends the old one, and the reverse, replace exactly', () => {
  for (const [oldWork, newWork] of [['/w/work', '/w/work/sub'], ['/w/work/sub', '/w/work']]) {
    const tree = path.join(tmpDir(), 'node_modules');
    write(tree, 'a.json', `{"p":"${oldWork}/node_modules/x","q":"${oldWork}"}`);
    fs.symlinkSync(`${oldWork}/node_modules/x`, path.join(tree, 'l'));
    const { restored, run } = roundTrip(tree, oldWork, newWork);
    run();
    assert.equal(fs.readFileSync(path.join(restored, 'a.json'), 'utf8'), `{"p":"${newWork}/node_modules/x","q":"${newWork}"}`, `${oldWork} -> ${newWork}`);
    assert.equal(fs.readlinkSync(path.join(restored, 'l')), `${newWork}/node_modules/x`);
  }
});

test('byte for byte: the prefix twice, inside a longer string, next to invalid UTF-8; a longer path name is not touched', () => {
  const tree = path.join(tmpDir(), 'node_modules');
  const bytes = (...parts) => Buffer.concat(parts.map((p) => (typeof p === 'string' ? Buffer.from(p) : Buffer.from(p))));
  const original = bytes([0xff, 0xfe, 0x80], `${OLD}/a ${OLD}`, [0xc3], `--cwd=${OLD}/node_modules/.bin:${OLD}\n`);
  write(tree, 'mixed.sh', original);
  const { restored, run } = roundTrip(tree, OLD, NEW);
  run();
  const expected = bytes([0xff, 0xfe, 0x80], `${NEW}/a ${NEW}`, [0xc3], `--cwd=${NEW}/node_modules/.bin:${NEW}\n`);
  assert.ok(fs.readFileSync(path.join(restored, 'mixed.sh')).equals(expected));

  const glued = path.join(tmpDir(), 'node_modules');
  write(glued, 'g.txt', `${OLD}/ok ${OLD}2/other`);
  const g = roundTrip(glued, OLD, NEW);
  assert.throws(g.run, /still names the old install path/, 'an unreplaced occurrence is caught by verification, not silently kept');
});

test('verification: a recorded file that no longer names the prefix fails, and so does an unrecorded file that does', () => {
  const missing = path.join(tmpDir(), 'node_modules');
  write(missing, 'a.js', `"${OLD}"`);
  const m = roundTrip(missing, OLD, NEW, {
    tamper: (tree) => {
      fs.chmodSync(path.join(tree, 'a.js'), 0o644);
      fs.writeFileSync(path.join(tree, 'a.js'), 'rewritten behind our back');
    },
  });
  assert.throws(m.run, /a\.js no longer names an install path/);

  const gained = path.join(tmpDir(), 'node_modules');
  write(gained, 'a.js', `"${OLD}"`);
  write(gained, 'b.js', 'clean');
  const g = roundTrip(gained, OLD, NEW, {
    tamper: (tree) => {
      fs.chmodSync(path.join(tree, 'b.js'), 0o644);
      fs.writeFileSync(path.join(tree, 'b.js'), `now ${OLD}`);
    },
  });
  assert.throws(g.run, /unrecorded b\.js names the old install path/);

  const noRole = path.join(tmpDir(), 'node_modules');
  write(noRole, 'a.js', `"${OLD}"`);
  const r = roundTrip(noRole, OLD, NEW);
  assert.throws(() => relocateRestoredTree(r.restored, { prefixes: [{ role: 'TMPDIR', path: OLD }], entries: [] }, { workDir: NEW }), /no new path for TMPDIR/);
});

test('a malformed relocation record is refused', () => {
  const tree = path.join(tmpDir(), 'node_modules');
  write(tree, 'a.js', `"${OLD}"`);
  const { run } = roundTrip(tree, OLD, NEW, {
    tamper: (_tree, meta) => {
      const m = JSON.parse(fs.readFileSync(meta, 'utf8'));
      m.relocation.entries = [{ relPath: 'a.js', kind: 'binary' }];
      fs.chmodSync(meta, 0o644);
      fs.writeFileSync(meta, JSON.stringify(m));
    },
  });
  assert.throws(run, /malformed/);
});

test('installPathRoles names the work dir, its real path and each per-run temp dir', () => {
  const real = fs.realpathSync(tmpDir());
  const link = `${tmpDir()}-link`;
  made.push(link);
  fs.symlinkSync(real, link);
  const roles = installPathRoles(link, { TMPDIR: '/t/a', TMP: '', TEMP: '/t/b' });
  assert.deepEqual(roles, [
    { role: 'workDir', path: link },
    { role: 'realWorkDir', path: real },
    { role: 'TMPDIR', path: '/t/a' },
    { role: 'TEMP', path: '/t/b' },
  ]);
});
