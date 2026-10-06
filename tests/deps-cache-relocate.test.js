import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
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
import { makeTmpDir } from './helpers/tmp.js';

/** BRAIN-402: a tree naming its install path is stored with a record of where, and rewritten to the new path on restore. */

const made = [];
test.after(() => {
  for (const d of made) removeTree(d);
});
const tmpDir = () => {
  const d = makeTmpDir('deps-relocate-');
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
  for (const [name, text] of [['+archive', '/runs/job+archive/data'], ['digit', '/runs/job2/file'], ['unicode', '/runs/jobé'], ['preceded by a name', 'x/runs/job/file'], ['preceded by a slash', '/backup/runs/job/file'], ['preceded by a second slash', '//runs/job/file']]) {
    const t = tree();
    write(t, 'ok.json', '{"p":"/runs/job/ok"}');
    write(t, 'a.js', `"${text}"`);
    assert.deepEqual(collectInstallPathReferences(t, rolesFor('/runs/job')), { ambiguous: 'a.js' }, name);
  }
  const link = tree();
  fs.mkdirSync(link, { recursive: true });
  fs.symlinkSync('/runs/job+archive/x', path.join(link, 'l'));
  assert.deepEqual(collectInstallPathReferences(link, rolesFor('/runs/job')), { ambiguous: 'l' });
  const leadingSlash = tree();
  fs.mkdirSync(leadingSlash, { recursive: true });
  fs.symlinkSync('//runs/job/x', path.join(leadingSlash, 'l'));
  assert.deepEqual(collectInstallPathReferences(leadingSlash, rolesFor('/runs/job')), { ambiguous: 'l' }, 'a symlink target that is the tail of a longer path');
  for (const lead of ['"', "'", '`', ' ', '\n', '(', '[', '{', '']) {
    const t = tree();
    write(t, 'a.js', `${lead}/runs/job/x`);
    assert.ok(collectInstallPathReferences(t, rolesFor('/runs/job')).relocation, `leader ${JSON.stringify(lead)}`);
  }
  for (const end of ['"', "'", '`', ' ', '\n', ')', ']', '}', '/', '']) {
    const t = tree();
    write(t, 'a.js', `"${'/runs/job'}${end}`);
    assert.ok(collectInstallPathReferences(t, rolesFor('/runs/job')).relocation, `terminator ${JSON.stringify(end)}`);
  }
  // Legal file-name bytes are never boundaries: "/backup:/runs/job/file" names an unrelated path (Codex round 4).
  for (const glued of ['"/backup:/runs/job/file"', '"/x=/runs/job/file"', '"a;/runs/job"', '"a,/runs/job"', '"/runs/job:x"', '"/runs/job=x"', '"/runs/job,x"', '"/runs/job;x"', '"/runs/job\\x"']) {
    const t = tree();
    write(t, 'a.js', glued);
    assert.deepEqual(collectInstallPathReferences(t, rolesFor('/runs/job')), { ambiguous: 'a.js' }, `glued ${glued}`);
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
  const original = `// é ${OLD}/a ${OLD}\nexec "${OLD}/node_modules/.bin/x" '${OLD}'\n`;
  write(t, 'mixed.sh', original);
  const { restored, run } = roundTrip(t, ...one(OLD, NEW));
  run();
  assert.ok(fs.readFileSync(path.join(restored, 'mixed.sh')).equals(Buffer.from(original.replaceAll(OLD, NEW))));
  // PATH-style text (`=` and `:` are legal file-name bytes) is ambiguous: such a tree is installed, never cached.
  const pathStyle = tree();
  write(pathStyle, 'env.sh', `--cwd=${OLD}/node_modules/.bin:${OLD}\n`);
  assert.deepEqual(collectInstallPathReferences(pathStyle, rolesFor(OLD)), { ambiguous: 'env.sh' });
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
  write(t, 'a.js', `"${OLD}" @@LANE_PREFIX_workDir_aaaa@@`);
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

test('restore scans the whole tree: a file the record omits that still holds a token fails', () => {
  const t = tree();
  write(t, 'a.js', `"${OLD}"`);
  write(t, 'b.js', `"${OLD}/b"`);
  const { run } = roundTrip(t, ...one(OLD, NEW), {
    tamper: (_stored, meta) => {
      const m = JSON.parse(fs.readFileSync(meta, 'utf8'));
      m.relocation.entries = m.relocation.entries.filter((e) => e.relPath !== 'b.js');
      fs.chmodSync(meta, 0o644);
      fs.writeFileSync(meta, JSON.stringify(m));
    },
  });
  assert.throws(run, /b\.js still holds a placeholder/);
});

test('publish verifies the cached copy: a file that changed or appeared after collection publishes nothing', () => {
  const cases = {
    'a recorded file gained an ambiguous occurrence': (t) => fs.writeFileSync(path.join(t, 'a.js'), `"${OLD}" "${OLD}+archive/x"`),
    'a new path-bearing file appeared': (t) => write(t, 'late.js', `"${OLD}"`),
    'a new binary file naming the path appeared': (t) => write(t, 'late.node', Buffer.concat([Buffer.from([0]), Buffer.from(OLD)])),
    'a recorded file lost its path': (t) => fs.writeFileSync(path.join(t, 'a.js'), 'nothing here'),
  };
  for (const [name, mutate] of Object.entries(cases)) {
    const t = tree();
    write(t, 'a.js', `"${OLD}"`);
    const { relocation } = collectInstallPathReferences(t, rolesFor(OLD));
    mutate(t);
    const cacheRoot = tmpDir();
    assert.throws(() => publishToStore(cacheRoot, newKey(), t, relocation), /relocation:/, name);
    assert.deepEqual(fs.readdirSync(cacheRoot), [], `${name}: nothing published, no temp left`);
  }
});

test('symlink targets are anchored: only the whole target or its leading directories relocate', () => {
  const bad = tree();
  fs.mkdirSync(bad, { recursive: true });
  fs.symlinkSync('/backup:/runs/job/file', path.join(bad, 'link'));
  assert.deepEqual(collectInstallPathReferences(bad, rolesFor('/runs/job')), { ambiguous: 'link' });

  const good = tree();
  fs.mkdirSync(good, { recursive: true });
  fs.symlinkSync('/runs/job/node_modules/x', path.join(good, 'link'));
  fs.symlinkSync('/runs/job', path.join(good, 'whole'));
  const { restored, run } = roundTrip(good, ...one('/runs/job', '/runs/new'));
  run();
  assert.equal(fs.readlinkSync(path.join(restored, 'link')), '/runs/new/node_modules/x');
  assert.equal(fs.readlinkSync(path.join(restored, 'whole')), '/runs/new');
});

test('publish verifies against every install path of the run, not only the ones collection saw', () => {
  const t = tree();
  write(t, 'a.js', '"/runs/job/file"');
  const roles = [{ role: 'workDir', path: '/runs/job' }, { role: 'TMPDIR', path: '/tmp/job' }];
  const { relocation } = collectInstallPathReferences(t, roles);
  assert.deepEqual(relocation.prefixes.map((p) => p.path), ['/runs/job'], 'collection saw only the work dir');
  assert.deepEqual(relocation.candidates, ['/runs/job', '/tmp/job']);
  write(t, 'late.js', '"/tmp/job/generated"');
  const cacheRoot = tmpDir();
  assert.throws(() => publishToStore(cacheRoot, newKey(), t, relocation), /unrecorded late\.js/);
  assert.deepEqual(fs.readdirSync(cacheRoot), []);
});

test('restore also looks for every old install path, except one the new paths contain', () => {
  const t = tree();
  write(t, 'a.js', '"/runs/job/file"');
  const roles = [{ role: 'workDir', path: '/runs/job' }, { role: 'TMPDIR', path: '/tmp/job' }];
  const stray = roundTrip(t, roles, { workDir: '/runs/new', TMPDIR: '/tmp/new' }, {
    tamper: (stored) => {
      fs.chmodSync(stored, 0o755);
      write(stored, 'late.js', '"/tmp/job/x"');
    },
  });
  assert.throws(stray.run, /late\.js still holds a placeholder or an old install path/);
});

// BRAIN-423: what node-gyp and its python leave behind names the install path but is never loaded.
/** better-sqlite3 as a source build leaves it: binding.gyp, a loadable .node, and build bookkeeping naming `work`. */
function plantNodeGypBuild(t, work, pkg = 'better-sqlite3') {
  write(t, `${pkg}/binding.gyp`, '{}');
  write(t, `${pkg}/lib/index.js`, 'module.exports = require("bindings")("addon");\n');
  write(t, `${pkg}/build/Release/addon.node`, Buffer.from([0x7f, 0x45, 0x4c, 0x46, 0]));
  write(t, `${pkg}/build/Makefile`, `srcdir := ${work}/node_modules/${pkg}/build\nCFLAGS += -I${work}/node_modules/node-gyp/include\n`);
  write(t, `${pkg}/build/binding.Makefile`, `# ${work}\n`);
  write(t, `${pkg}/build/addon.target.mk`, `TOOLSET := target\nobj := ${work}/obj\n`);
  write(t, `${pkg}/build/config.gypi`, `{"variables": {"node_gyp_dir": "${work}/node_modules/node-gyp"}}\n`);
  write(t, `${pkg}/build/Release/.deps/a.intermediate.d`, `cmd_x := cc ${work}/src/a.c\n`);
  write(t, `${pkg}/build/Release/obj.target/addon/src/a.o`, Buffer.concat([Buffer.from([0xcf, 0xfa, 0, 0]), Buffer.from(work)]));
}

test('node-gyp build intermediates are neither a reason to refuse the tree nor part of the stored copy; the .node stays', () => {
  const t = tree();
  plantNodeGypBuild(t, OLD);
  const collected = collectInstallPathReferences(t, rolesFor(OLD));
  assert.deepEqual([collected.relocation?.prefixes, collected.relocation?.entries], [[], []], `got ${JSON.stringify(collected)}`);

  const cacheRoot = tmpDir();
  const key = newKey();
  publishToStore(cacheRoot, key, t, collected.relocation);
  const stored = path.join(entryDir(cacheRoot, key), 'node_modules', 'better-sqlite3');
  assert.deepEqual(fs.readdirSync(path.join(stored, 'build')), ['Release'], 'Makefile, *.mk, config.gypi are not stored');
  assert.deepEqual(fs.readdirSync(path.join(stored, 'build', 'Release')), ['addon.node'], '.deps and obj.target are not stored');
  assert.ok(fs.existsSync(path.join(stored, 'binding.gyp')), 'binding.gyp stays, so npm rebuild can configure again');
  assert.ok(fs.existsSync(path.join(t, 'better-sqlite3', 'build', 'Makefile')), 'the live tree is untouched');
});

test('node-gyp exemption guards: no binding.gyp, a path in a runtime file, or a path in the loadable .node still refuse the tree', () => {
  const lookalike = tree();
  write(lookalike, 'dist-pkg/build/Makefile', `X = ${OLD}/y\n`);
  assert.deepEqual(collectInstallPathReferences(lookalike, rolesFor(OLD)), { binary: 'dist-pkg/build/Makefile' }, 'a build/ dir with no binding.gyp beside it is not node-gyp output');

  const runtimeJs = tree();
  plantNodeGypBuild(runtimeJs, OLD);
  write(runtimeJs, 'better-sqlite3/lib/index.js', `module.exports = require("${OLD}/node_modules/better-sqlite3/build/Release/addon.node");\n`);
  assert.ok(collectInstallPathReferences(runtimeJs, rolesFor(OLD)).relocation, 'a quoted path in a .js file is relocatable');
  write(runtimeJs, 'better-sqlite3/lib/index.js', `const root=${OLD}/node_modules;\n`);
  assert.deepEqual(collectInstallPathReferences(runtimeJs, rolesFor(OLD)), { ambiguous: 'better-sqlite3/lib/index.js' });

  const addon = tree();
  plantNodeGypBuild(addon, OLD);
  write(addon, 'better-sqlite3/build/Release/addon.node', Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46, 0]), Buffer.from(OLD)]));
  assert.deepEqual(collectInstallPathReferences(addon, rolesFor(OLD)), { binary: 'better-sqlite3/build/Release/addon.node' });
});

test('only node-gyp\'s own python bytecode (sources beside it) is dropped; any other __pycache__ naming the path refuses the tree', () => {
  const pyc = (work) => Buffer.concat([Buffer.from([0x2b, 0x0e, 0, 0]), Buffer.from(`${work}/node_modules/node-gyp/gyp/MSVSUtil.py`)]);
  const t = tree();
  write(t, 'node-gyp/gyp/pylib/gyp/__pycache__/MSVSUtil.cpython-314.pyc', pyc(OLD));
  write(t, 'node-gyp/gyp/pylib/gyp/MSVSUtil.py', 'x = 1\n');
  const collected = collectInstallPathReferences(t, rolesFor(OLD));
  assert.deepEqual(collected.relocation.entries, []);
  const cacheRoot = tmpDir();
  const key = newKey();
  publishToStore(cacheRoot, key, t, collected.relocation);
  assert.deepEqual(fs.readdirSync(path.join(entryDir(cacheRoot, key), 'node_modules', 'node-gyp/gyp/pylib/gyp')), ['MSVSUtil.py']);

  const elsewhere = tree();
  write(elsewhere, 'other-tool/__pycache__/x.cpython-314.pyc', pyc(OLD));
  write(elsewhere, 'other-tool/x.py', 'x = 1\n');
  assert.deepEqual(collectInstallPathReferences(elsewhere, rolesFor(OLD)), { binary: 'other-tool/__pycache__/x.cpython-314.pyc' });

  const noSource = tree();
  write(noSource, 'node-gyp/gyp/pylib/gyp/__pycache__/Gone.cpython-314.pyc', pyc(OLD));
  assert.deepEqual(collectInstallPathReferences(noSource, rolesFor(OLD)), { binary: 'node-gyp/gyp/pylib/gyp/__pycache__/Gone.cpython-314.pyc' });
});

test('a runtime file under build/ (build/deps/runtime.json) survives the prune and a hit; a symlink into a pruned path refuses the tree', () => {
  const t = tree();
  plantNodeGypBuild(t, OLD);
  write(t, 'better-sqlite3/build/deps/runtime.json', '{"needed": true}');
  write(t, 'better-sqlite3/build/deps/sqlite3.mk', 'kept: yes\n');
  const { restored, run } = roundTrip(t, ...one(OLD, NEW));
  run();
  assert.equal(fs.readFileSync(path.join(restored, 'better-sqlite3/build/deps/runtime.json'), 'utf8'), '{"needed": true}');
  assert.ok(fs.existsSync(path.join(restored, 'better-sqlite3/build/deps/sqlite3.mk')));
  assert.deepEqual(fs.readdirSync(path.join(restored, 'better-sqlite3/build')).sort(), ['Release', 'deps']);

  for (const target of ['better-sqlite3/build/Makefile', 'better-sqlite3/build/Release/obj.target/addon', 'ABS/better-sqlite3/build/config.gypi']) {
    const linked = tree();
    plantNodeGypBuild(linked, OLD);
    fs.symlinkSync(target.replace('ABS', linked), path.join(linked, 'link'));
    assert.deepEqual(collectInstallPathReferences(linked, rolesFor(OLD)), { prunedLink: 'link' }, target);
  }
  const aliased = tree();
  plantNodeGypBuild(aliased, OLD);
  fs.symlinkSync('better-sqlite3/build', path.join(aliased, 'alias'));
  fs.symlinkSync('alias/Release/obj.target/addon', path.join(aliased, 'addon'));
  assert.deepEqual(collectInstallPathReferences(aliased, rolesFor(OLD)), { prunedLink: 'addon' }, 'a link through a directory symlink into a pruned path is refused');
  const dangling = tree();
  plantNodeGypBuild(dangling, OLD);
  fs.symlinkSync('nowhere/at/all', path.join(dangling, 'broken'));
  assert.deepEqual(collectInstallPathReferences(dangling, rolesFor(OLD)), { prunedLink: 'broken' }, 'a link that does not resolve is refused');
  const escaping = tree();
  plantNodeGypBuild(escaping, OLD);
  fs.symlinkSync(tmpDir(), path.join(escaping, 'out'));
  assert.deepEqual(collectInstallPathReferences(escaping, rolesFor(OLD)), { prunedLink: 'out' }, 'a link that leaves the tree is refused');
  const fine = tree();
  plantNodeGypBuild(fine, OLD);
  fs.symlinkSync('better-sqlite3/build/Release/addon.node', path.join(fine, 'ok-link'));
  assert.ok(collectInstallPathReferences(fine, rolesFor(OLD)).relocation, 'a link to the kept .node is fine');
});

test('the real Prisma 6 generated-client shape (output value, sourceFilePath) is relocated', () => {
  const t = tree();
  const edge = `const config = {\n  "generator": {\n    "output": {\n      "value": "${OLD}/node_modules/@prisma/client",\n      "fromEnvVar": null\n    }\n  },\n  "sourceFilePath": "${OLD}/prisma/schema.prisma"\n}\n`;
  for (const f of ['edge.js', 'index.js', 'wasm.js']) write(t, `.prisma/client/${f}`, edge);
  const { restored, run } = roundTrip(t, ...one(OLD, NEW));
  run();
  assert.equal(fs.readFileSync(path.join(restored, '.prisma/client/edge.js'), 'utf8'), edge.replaceAll(OLD, NEW));
});
