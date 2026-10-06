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

/** Publish `tree` (installed under `roles`), restore it, and relocate it onto `newPaths` (role -> path). */
function roundTrip(tree, roles, newPaths, { tamper } = {}) {
  const collected = collectInstallPathReferences(tree, roles);
  assert.ok(collected.relocation, `relocatable, got ${JSON.stringify(collected)}`);
  const cacheRoot = tmpDir();
  const key = newKey();
  assert.equal(publishToStore(cacheRoot, key, tree, collected.relocation).published, true);
  const storedTree = path.join(entryDir(cacheRoot, key), 'node_modules');
  if (tamper) tamper(storedTree, path.join(entryDir(cacheRoot, key), 'meta.json'));
  const restored = path.join(tmpDir(), 'node_modules');
  materializeFromStore(cacheRoot, key, restored);
  const run = () => relocateRestoredTree(restored, readRelocation(cacheRoot, key), newPaths);
  return { restored, storedTree, relocation: collected.relocation, run };
}

const OLD = '/var/tmp/lb-a/tickets/t1/work';
const NEW = '/var/tmp/lb-b/tickets/t2/work';
const one = (oldWork, newWork) => [rolesFor(oldWork), { workDir: newWork }];
const tree = () => path.join(tmpDir(), 'node_modules');

test('a Prisma-shaped edge.js and a symlink are templated in the store, and restored to a different path', () => {
  const t = tree();
  const edge = `config.dirname = "${OLD}/node_modules/.prisma/client";\nconfig.relativePath = "../../";\nconfig.inlineSchema = \`path ${OLD}/prisma/schema.prisma\`;\n`;
  write(t, '.prisma/client/edge.js', edge, 0o755);
  write(t, 'clean/index.js', 'module.exports = 1;\n');
  fs.symlinkSync(`${OLD}/node_modules/clean`, path.join(t, 'link'));
  fs.symlinkSync('../clean', path.join(t, '.prisma', 'relative-link'));

  const { restored, storedTree, relocation, run } = roundTrip(t, ...one(OLD, NEW));
  assert.deepEqual(relocation.prefixes, [{ roles: ['workDir'], path: OLD }]);
  assert.match(relocation.nonce, /^[0-9a-f]{32}$/);
  assert.deepEqual(relocation.entries.map((e) => `${e.kind}:${e.relPath}`).sort(), ['symlink:link', 'text:.prisma/client/edge.js']);
  assert.ok(!fs.readFileSync(path.join(storedTree, '.prisma/client/edge.js'), 'utf8').includes(OLD), 'the store holds the template, not the install path');
  assert.ok(fs.readFileSync(path.join(storedTree, '.prisma/client/edge.js'), 'utf8').includes(`@@LANE_PREFIX_workDir_${relocation.nonce}@@`));
  assert.ok(fs.readFileSync(path.join(t, '.prisma/client/edge.js'), 'utf8').includes(OLD), 'the live tree is untouched');
  run();

  assert.equal(fs.readFileSync(path.join(restored, '.prisma/client/edge.js'), 'utf8'), edge.replaceAll(OLD, NEW));
  assert.equal(fs.statSync(path.join(restored, '.prisma/client/edge.js')).mode & 0o7777, 0o755, 'mode kept');
  assert.equal(fs.readlinkSync(path.join(restored, 'link')), `${NEW}/node_modules/clean`);
  assert.equal(fs.readlinkSync(path.join(restored, '.prisma', 'relative-link')), '../clean');
  assert.deepEqual(fs.readdirSync(path.join(restored, '.prisma/client')), ['edge.js'], 'no temp file left behind');
});

test('a tree that names no install path records an empty relocation', () => {
  const t = tree();
  write(t, 'a.js', 'x');
  const { relocation } = collectInstallPathReferences(t, rolesFor(OLD));
  assert.deepEqual([relocation.prefixes, relocation.entries], [[], []]);
});

test('P1 binary: a file with a NUL byte, or in a non-allowlisted extension, or not UTF-8, makes the tree non-relocatable', () => {
  const nul = tree();
  write(nul, 'text.js', `"${OLD}"`);
  write(nul, 'native/addon.node', Buffer.concat([Buffer.from([0x7f, 0x45, 0, 0x4c]), Buffer.from(OLD)]));
  assert.deepEqual(collectInstallPathReferences(nul, rolesFor(OLD)), { binary: 'native/addon.node' });

  // a MessagePack fixstr: 0xa0|len, then the bytes; no NUL anywhere, and a length that a different path would break
  const msgpack = tree();
  write(msgpack, 'cache/entry.mpack', Buffer.concat([Buffer.from([0x81, 0xa1, 0x70, 0xa0 | OLD.length]), Buffer.from(OLD)]));
  assert.deepEqual(collectInstallPathReferences(msgpack, rolesFor(OLD)), { binary: 'cache/entry.mpack' });

  const latin1 = tree();
  write(latin1, 'a.js', Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(OLD)]));
  assert.deepEqual(collectInstallPathReferences(latin1, rolesFor(OLD)), { binary: 'a.js' });

  const noExt = tree();
  write(noExt, 'LICENSE', OLD);
  assert.deepEqual(collectInstallPathReferences(noExt, rolesFor(OLD)), { binary: 'LICENSE' }, 'extensionless outside .bin is not allowlisted');

  const shim = tree();
  write(shim, '.bin/tool', `#!/bin/sh\nexec node "${OLD}/node_modules/x/cli.js" "$@"\n`, 0o755);
  assert.ok(collectInstallPathReferences(shim, rolesFor(OLD)).relocation, 'a #! script in .bin is rewritable');
  const notScript = tree();
  write(notScript, '.bin/tool', `${OLD}\n`);
  assert.deepEqual(collectInstallPathReferences(notScript, rolesFor(OLD)), { binary: '.bin/tool' });
});

test('a NUL byte anywhere in the file makes it binary, even past the first chunk', () => {
  const t = tree();
  const big = Buffer.alloc(2 * 1024 * 1024 + 10, 0x41);
  Buffer.from(OLD).copy(big, 5);
  big[big.length - 1] = 0;
  write(t, 'big.js', big);
  assert.deepEqual(collectInstallPathReferences(t, rolesFor(OLD)), { binary: 'big.js' });
});

test('P1 ambiguous: an occurrence followed by anything but a path terminator refuses the tree', () => {
  for (const [name, text] of [['+archive', '/runs/job+archive/data'], ['digit', '/runs/job2/file'], ['unicode', '/runs/jobé'], ['preceded by a name', 'x/runs/job/file']]) {
    const t = tree();
    write(t, 'ok.json', '{"p":"/runs/job/ok"}');
    write(t, 'a.js', `"${text}"`);
    assert.deepEqual(collectInstallPathReferences(t, rolesFor('/runs/job')), { ambiguous: 'a.js' }, name);
  }
  const link = tree();
  fs.mkdirSync(link, { recursive: true });
  fs.symlinkSync('/runs/job+archive/x', path.join(link, 'l'));
  assert.deepEqual(collectInstallPathReferences(link, rolesFor('/runs/job')), { ambiguous: 'l' });
  for (const end of ['"', "'", '`', ' ', '\n', ')', ']', '}', ',', ';', ':', '\\', '=', '/', '']) {
    const t = tree();
    write(t, 'a.js', `p=${'/runs/job'}${end}`);
    assert.ok(collectInstallPathReferences(t, rolesFor('/runs/job')).relocation, `terminator ${JSON.stringify(end)}`);
  }
});

test('P1 stale occurrence is never masked: /runs/old to /runs with a /runs/old2 occurrence is ambiguous at store', () => {
  const t = tree();
  write(t, 'a.json', '{"p":"/runs/old/x","q":"/runs/old2/file"}');
  assert.deepEqual(collectInstallPathReferences(t, rolesFor('/runs/old')), { ambiguous: 'a.json' });
});

test('overlapping prefixes: the new path extends the old one, and the reverse, substitute exactly', () => {
  for (const [oldWork, newWork] of [['/w/work', '/w/work/sub'], ['/w/work/sub', '/w/work']]) {
    const t = tree();
    write(t, 'a.json', `{"p":"${oldWork}/node_modules/x","q":"${oldWork}"}`);
    fs.symlinkSync(`${oldWork}/node_modules/x`, path.join(t, 'l'));
    const { restored, run } = roundTrip(t, ...one(oldWork, newWork));
    run();
    assert.equal(fs.readFileSync(path.join(restored, 'a.json'), 'utf8'), `{"p":"${newWork}/node_modules/x","q":"${newWork}"}`, `${oldWork} -> ${newWork}`);
    assert.equal(fs.readlinkSync(path.join(restored, 'l')), `${newWork}/node_modules/x`);
  }
});

test('two install paths where one extends the other are templated longest first', () => {
  const t = tree();
  write(t, 'a.json', `{"w":"${OLD}/x","t":"${OLD}/tmp/y"}`);
  const roles = [{ role: 'workDir', path: OLD }, { role: 'TMPDIR', path: `${OLD}/tmp` }];
  const { restored, run } = roundTrip(t, roles, { workDir: '/n/w', TMPDIR: '/n/t' });
  run();
  assert.equal(fs.readFileSync(path.join(restored, 'a.json'), 'utf8'), '{"w":"/n/w/x","t":"/n/t/y"}');
});

test('byte for byte: the prefix twice, inside a longer string, next to non-ASCII text', () => {
  const t = tree();
  const original = `// é ${OLD}/a ${OLD}\n--cwd=${OLD}/node_modules/.bin:${OLD}\n`;
  write(t, 'mixed.sh', original);
  const { restored, run } = roundTrip(t, ...one(OLD, NEW));
  run();
  assert.ok(fs.readFileSync(path.join(restored, 'mixed.sh')).equals(Buffer.from(original.replaceAll(OLD, NEW))));
});

test('P1 roles: roles sharing a path at store are one group; restoring them onto different paths falls back', () => {
  const t = tree();
  write(t, 'a.js', `"${OLD}/x"`);
  const roles = [{ role: 'TMPDIR', path: OLD }, { role: 'TMP', path: OLD }];
  const same = roundTrip(t, roles, { TMPDIR: NEW, TMP: NEW });
  assert.deepEqual(same.relocation.prefixes, [{ roles: ['TMPDIR', 'TMP'], path: OLD }]);
  same.run();
  assert.equal(fs.readFileSync(path.join(same.restored, 'a.js'), 'utf8'), `"${NEW}/x"`);

  const diverged = roundTrip(t, roles, { TMPDIR: NEW, TMP: '/elsewhere' });
  assert.throws(diverged.run, /relocation: ambiguous roles TMPDIR\+TMP/);
  const missing = roundTrip(t, roles, { TMPDIR: NEW });
  assert.throws(missing.run, /relocation: no new path for TMP/);
});

test('the token never leaks: a restored entry that still holds one, or holds none, fails', () => {
  const t = tree();
  write(t, 'a.js', `"${OLD}"`);
  write(t, 'b.js', `"${OLD}"`);
  const stub = roundTrip(t, ...one(OLD, NEW), {
    tamper: (stored) => {
      fs.chmodSync(path.join(stored, 'a.js'), 0o644);
      fs.writeFileSync(path.join(stored, 'a.js'), 'no placeholder here');
    },
  });
  assert.throws(stub.run, /a\.js holds no placeholder/);

  // a token for another role that the record does not map is left behind by substitution, and caught
  const extra = roundTrip(t, ...one(OLD, NEW), {
    tamper: (stored, meta) => {
      const nonce = JSON.parse(fs.readFileSync(meta, 'utf8')).relocation.nonce;
      fs.chmodSync(path.join(stored, 'b.js'), 0o644);
      fs.appendFileSync(path.join(stored, 'b.js'), `@@LANE_PREFIX_TMPDIR_${nonce}@@`);
    },
  });
  assert.throws(extra.run, /b\.js still holds a placeholder/);
});

test('a nonce that already occurs in the tree is replaced by a fresh one', () => {
  const t = tree();
  write(t, 'a.js', `"${OLD}" @@LANE_PREFIX_aaaa`);
  const nonces = ['aaaa', 'bbbb', 'cccc'];
  const { relocation } = collectInstallPathReferences(t, rolesFor(OLD), { newNonce: () => nonces.shift() });
  assert.equal(relocation.nonce, 'bbbb');
});

test('a malformed relocation record is refused', () => {
  const t = tree();
  write(t, 'a.js', `"${OLD}"`);
  for (const mutate of [(m) => (m.relocation.entries = [{ relPath: 'a.js', kind: 'binary' }]), (m) => (m.relocation.nonce = 'short'), (m) => (m.relocation.prefixes = [{ role: 'workDir', path: OLD }])]) {
    const { run } = roundTrip(t, ...one(OLD, NEW), {
      tamper: (_stored, meta) => {
        const m = JSON.parse(fs.readFileSync(meta, 'utf8'));
        mutate(m);
        fs.chmodSync(meta, 0o644);
        fs.writeFileSync(meta, JSON.stringify(m));
      },
    });
    assert.throws(run, /malformed/);
  }
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
