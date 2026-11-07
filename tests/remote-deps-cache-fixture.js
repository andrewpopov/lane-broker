import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { laneRun } from './helpers.js';
import { setup, tmpDir } from './remote-harness.js';
import { paths } from '../src/state.js';

/**
 * BRAIN-389, end to end: a real `lane run` -> fake-ssh transport -> `remote-exec` -> `remote-pipeline` with
 * REAL npm. The fixture installs a tarball dependency that ships a `.bin` command, so `npm ci` needs no
 * network and a hit has a real `.bin` symlink to run. A shim in front of `npm` counts its invocations, which is
 * how "a hit runs no npm ci" is observed.
 */

export const IS_ROOT = typeof process.getuid === 'function' && process.getuid() === 0;
export const REAL_NPM = execFileSync('which', ['npm'], { encoding: 'utf8' }).trim();
export const NPMRC = 'registry=http://127.0.0.1:9\nfetch-retries=0\nfetch-timeout=2000\n';

/** `{ 'package.json', 'package-lock.json', tarball }` for an app depending on a local tarball with a bin, built with real npm, offline. */
export const fixtureBase = (() => {
  let built;
  return () => {
    if (built) return built;
    const dir = tmpDir('deps-cache-fixture');
    const npmEnv = { ...process.env, HOME: dir, npm_config_cache: path.join(dir, 'cache'), npm_config_userconfig: path.join(dir, 'userrc') };
    const tool = path.join(dir, 'tool');
    const app = path.join(dir, 'app');
    fs.mkdirSync(path.join(tool, 'bin'), { recursive: true });
    fs.mkdirSync(app);
    fs.writeFileSync(path.join(tool, 'package.json'), JSON.stringify({ name: 'hello-tool', version: '1.0.0', bin: { 'hello-tool': 'bin/hello.js' } }));
    fs.writeFileSync(path.join(tool, 'bin', 'hello.js'), '#!/usr/bin/env node\nconsole.log("hello from the bin");\n', { mode: 0o755 });
    execFileSync(REAL_NPM, ['pack', '--silent', '--pack-destination', app], { cwd: tool, env: npmEnv });
    fs.writeFileSync(path.join(app, 'package.json'), JSON.stringify({ name: 'app', version: '1.0.0', private: true, dependencies: { 'hello-tool': 'file:./hello-tool-1.0.0.tgz' } }));
    execFileSync(REAL_NPM, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: app, env: npmEnv });
    built = {
      packageJson: fs.readFileSync(path.join(app, 'package.json'), 'utf8'),
      lock: fs.readFileSync(path.join(app, 'package-lock.json'), 'utf8'),
      tarball: fs.readFileSync(path.join(app, 'hello-tool-1.0.0.tgz')),
    };
    return built;
  };
})();

export const POSTINSTALLS = {
  outside: "require('fs').writeFileSync('generated-by-postinstall.txt', process.cwd())",
  embed: "require('fs').writeFileSync('node_modules/hello-tool/where.txt', process.cwd())",
  'embed-big': "require('fs').writeFileSync('node_modules/hello-tool/native.bin', Buffer.concat([Buffer.alloc(3 * 1024 * 1024, 65), Buffer.from([0]), Buffer.from(process.cwd())]))",
  'drop-installed-record': "const f = 'node_modules/.package-lock.json'; const l = JSON.parse(require('fs').readFileSync(f, 'utf8')); delete l.packages['node_modules/hello-tool']; require('fs').writeFileSync(f, JSON.stringify(l))",
  harmless: "process.exit(0)",
  'hook-file': "require('fs').mkdirSync('.git/hooks', { recursive: true }); require('fs').writeFileSync('.git/hooks/pre-commit', '#!/bin/sh\\n')",
  'node-gyp': "const fs = require('fs'); const d = 'node_modules/hello-tool/'; fs.mkdirSync(d + 'build/Release', { recursive: true }); fs.writeFileSync(d + 'binding.gyp', '{}'); fs.writeFileSync(d + 'build/Makefile', 'srcdir := ' + process.cwd() + '/node_modules\\n'); fs.writeFileSync(d + 'build/config.gypi', '{\\\"d\\\": \\\"' + process.cwd() + '\\\"}'); fs.writeFileSync(d + 'build/Release/hello.node', 'loadable')",
  'git-tag': "require('child_process').execFileSync('git', ['tag', 'installed'])",
  'embed-tmpdir': "require('fs').writeFileSync('node_modules/hello-tool/tmp.txt', process.env.TMPDIR)",
};

/**
 * The files of a repo whose lane installs the fixture. `lockEdit` makes a variant lock; `postinstall` names a
 * root script from POSTINSTALLS; `prepare` sets a root prepare script. A lane declaring a postinstall also
 * declares root scripts safe, unless `rootScriptsSafe` says otherwise.
 */
export function repoFiles({ lockEdit, postinstall = null, prepare = null, scripts = {}, npmrcExtra = '', dev = false, rootScriptsSafe = postinstall !== null, tarball, laneExtra = {} } = {}) {
  const base = fixtureBase();
  const pkg = JSON.parse(base.packageJson);
  const lock = JSON.parse(base.lock);
  if (postinstall) {
    pkg.scripts = { ...pkg.scripts, postinstall: `node -e "${POSTINSTALLS[postinstall].replace(/"/g, '\\"')}"` };
    lock.packages[''].hasInstallScript = true;
  }
  if (prepare) pkg.scripts = { ...pkg.scripts, prepare };
  if (Object.keys(scripts).length > 0) pkg.scripts = { ...pkg.scripts, ...scripts };
  if (dev) {
    pkg.devDependencies = pkg.dependencies;
    delete pkg.dependencies;
    lock.packages[''].devDependencies = lock.packages[''].dependencies;
    delete lock.packages[''].dependencies;
    lock.packages['node_modules/hello-tool'].dev = true;
  }
  if (lockEdit) lockEdit(lock);
  return {
    'package.json': JSON.stringify(pkg),
    'package-lock.json': JSON.stringify(lock),
    'hello-tool-1.0.0.tgz': tarball ?? base.tarball,
    '.npmrc': NPMRC + npmrcExtra,
    ...(rootScriptsSafe ? { 'gen-input.txt': 'generator input one' } : {}),
    '.lane-broker.json': JSON.stringify({
      version: 1,
      lanes: { default: { weight: 1, remote: true, remoteDeps: ['.'], ...(rootScriptsSafe ? { remoteDepsCacheRootScriptsSafe: true, remoteDepsCacheKeyFiles: ['gen-input.txt'] } : {}), ...laneExtra } },
    }),
  };
}

/** A `setup()` harness whose PATH has an `npm` shim that logs each invocation's argv before running real npm. */
export function setupWithNpmSpy(opts) {
  const s = setup(opts);
  const spyDir = tmpDir('deps-cache-npm-spy');
  const log = path.join(spyDir, 'npm-calls.log');
  const envLog = path.join(spyDir, 'npm-env.log');
  const shim = path.join(spyDir, 'npm');
  fs.writeFileSync(shim, `#!/bin/sh\necho "$@" >> ${JSON.stringify(log)}\necho "ssh_auth_sock=$SSH_AUTH_SOCK" >> ${JSON.stringify(envLog)}\nexec ${JSON.stringify(REAL_NPM)} "$@"\n`, { mode: 0o755 });
  s.env = { ...s.env, PATH: `${spyDir}${path.delimiter}${s.env.PATH}` };
  s.npmCiCalls = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter((l) => l.startsWith('ci ')).length : 0);
  s.npmEnvLog = () => (fs.existsSync(envLog) ? fs.readFileSync(envLog, 'utf8') : '');
  s.storeDir = path.join(s.runnerRoot, 'deps-cache');
  s.storedKeys = () => (fs.existsSync(s.storeDir) ? fs.readdirSync(s.storeDir).filter((n) => /^[0-9a-f]{64}$/.test(n)) : []);
  return s;
}

/**
 * The command run on the runner after deps: it must find a working `.bin` command, and it records what an
 * in-place write to an installed file does (written to `marker`, an absolute path outside the work dir).
 */
export function probeCommand(marker) {
  const script = `
    const fs = require('fs');
    const { execFileSync } = require('child_process');
    const out = execFileSync('node_modules/.bin/hello-tool', { encoding: 'utf8' }).trim();
    let inPlace = 'wrote';
    try { fs.appendFileSync('node_modules/hello-tool/package.json', ' '); } catch (e) { inPlace = e.code; }
    fs.writeFileSync('node_modules/hello-tool/new-file.txt', 'creating a file works');
    let hooksPath = null;
    try { hooksPath = execFileSync('git', ['config', '--get', 'core.hooksPath'], { encoding: 'utf8' }).trim(); } catch {}
    fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify({ out, inPlace, hooksPath, cwd: process.cwd(), tmpdir: process.env.TMPDIR }));
  `;
  return ['node', '-e', script];
}

export const lastRow = (state) => fs.readFileSync(paths(state).history, 'utf8').trim().split('\n').map((l) => JSON.parse(l)).at(-1);

export async function runLane(s, repoDir, { repo = 'r', marker, env = s.env } = {}) {
  const markerPath = marker ?? path.join(tmpDir('deps-cache-marker'), 'probe.json');
  const r = await laneRun(['run', '--repo', repo, '--lane', 'default', '--', ...probeCommand(markerPath)], { env, cwd: repoDir });
  const probe = fs.existsSync(markerPath) ? JSON.parse(fs.readFileSync(markerPath, 'utf8')) : null;
  return { ...r, probe, row: r.code === 0 || fs.existsSync(paths(s.state).history) ? lastRow(s.state) : null };
}
