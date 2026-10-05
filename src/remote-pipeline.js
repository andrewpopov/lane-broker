import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { atomicWriteFile, atomicWriteJson, readJsonSafe, withLock, ensureStateDirs } from './state.js';
import {
  computeDepsKey,
  entryTree,
  materializeFromStore,
  publishToStore,
  touchLastUsed,
  acquireLease,
  trimStore,
  findMissingInstalled,
  currentSystem,
  snapshotOutsideNodeModules,
  snapshotChanges,
  findInstallPathReference,
  scrubDepsEnv,
  allowlistedRootEvents,
  unexplainedChanges,
  PER_RUN_PATH_ENV_NAMES,
} from './deps-cache.js';

/**
 * BRAIN-320 S1b (1b): `lane remote-pipeline <ticketDir>` is the hidden
 * command `remote-exec` runs (in place of the caller's own argv) as the
 * SINGLE broker ticket's child, for a protocol-2 header. Running deps,
 * setup and the command as its own children -- rather than remote-exec
 * running them directly -- means everything downstream of admission (the
 * CPU/memory grant, nice, and a process-group cancel) covers every phase
 * and every descendant, since the broker only ever knows about the one
 * ticket that spawned this process.
 */

const NPM_CONFIG_ENV_RE = /^npm_config_/i;

const DEPS_INSTALL_ARGV = ['npm', 'ci', '--no-audit', '--no-fund'];

function writePhase(ticketDir, phase) {
  atomicWriteFile(path.join(ticketDir, 'phase'), phase);
}

/** Create an empty, 0600 npmrc file if one doesn't already exist -- never
 *  overwrites a pre-existing one (the runner's own npmrc is normally empty,
 *  but a later run must not clobber whatever a previous one left there). */
function ensureNpmrcFile(file) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  if (!fs.existsSync(file)) {
    fs.writeFileSync(file, '', { mode: 0o600 });
  }
}

/**
 * The deps phase's env is CONTROLLED, never ambient (1b): start from
 * `process.env`, strip every `npm_config_*` and `NODE_ENV` the runner
 * process itself was started with, then pin the cache/userconfig/
 * globalconfig this runner owns. The repo's own tracked `.npmrc`, if any,
 * still applies underneath this -- npm reads that from the deps dir itself,
 * not from anything here. BRAIN-389: the per-run variables that reach no cache
 * key (shell bookkeeping, ssh, lane's own ids) are removed too, so nothing an
 * install reads can differ between two runs that share a key.
 */
function buildDepsEnv(npmCacheDir, npmUserConfig, npmGlobalConfig) {
  const env = { ...process.env };
  for (const key of Object.keys(env)) {
    if (NPM_CONFIG_ENV_RE.test(key)) delete env[key];
  }
  delete env.NODE_ENV;
  env.npm_config_cache = npmCacheDir;
  env.npm_config_userconfig = npmUserConfig;
  env.npm_config_globalconfig = npmGlobalConfig;
  return scrubDepsEnv(env);
}

/**
 * Spawn one phase child. NOT detached -- it must stay in the ticket's own
 * process group (the broker's cancel path kills `-pgid`), so a cancel
 * during any phase reaches this child and everything it spawns (npm's own
 * lifecycle-script children included). stdio is inherited so its output
 * streams straight through, exactly like the plain-argv case did before
 * this slice.
 */
function spawnPhaseChild(argv, cwd, env) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(argv[0], argv.slice(1), { cwd, env, stdio: 'inherit', detached: false });
    } catch (err) {
      resolve({ spawnError: err });
      return;
    }
    child.on('error', (err) => resolve({ spawnError: err }));
    child.on('exit', (code, signal) => resolve({ code, signal }));
  });
}

/** Run one phase-child argv to completion, mapping its outcome down to a
 *  single exit code per the pipeline's own rules: a spawn error is exit
 *  127 (after printing it to stderr), a signal is 128+signo, otherwise the
 *  child's own exit code (or 1 if somehow neither is set). */
async function runPhaseCommand(argv, cwd, env) {
  const r = await spawnPhaseChild(argv, cwd, env);
  if (r.spawnError) {
    process.stderr.write(`lane remote-pipeline: failed to start "${argv[0]}": ${r.spawnError.message}\n`);
    return 127;
  }
  if (r.signal) {
    const signo = os.constants.signals[r.signal] ?? 0;
    return 128 + signo;
  }
  return r.code ?? 1;
}

/** BRAIN-389: the one log line per deps dir, on the pipeline's stderr, which flows to the submitter. */
function logDepsCache(outcome, dir, ms, { key, published, reason, file, missing } = {}) {
  const fields = [
    key ? `key=${key.slice(0, 12)}` : null,
    `dir=${dir}`,
    `ms=${ms}`,
    published === undefined ? null : `published=${published ? 'yes' : 'no'}`,
    reason ? `reason=${/^[\w-]+$/.test(reason) ? reason : JSON.stringify(reason)}` : null,
    file ? `file=${file}` : null,
    missing ? `missing=${missing.slice(0, 5).join(',')}${missing.length > 5 ? ',...' : ''}` : null,
  ].filter(Boolean);
  process.stderr.write(`deps-cache ${outcome} ${fields.join(' ')}\n`);
}

/** What an install in `cwd` could change outside node_modules: `cwd` itself, and the work dir's `.git` (shared by every dir). */
function snapshotInstallSurface(cwd, workDir) {
  const surface = snapshotOutsideNodeModules(cwd);
  if (cwd === workDir) return surface;
  const gitDir = path.join(workDir, '.git');
  const st = fs.lstatSync(gitDir);
  surface.set('.git', `d:${st.size}:${st.mtimeMs}`);
  for (const [rel, sig] of snapshotOutsideNodeModules(gitDir)) surface.set(`.git/${rel}`, sig);
  return surface;
}

function readTextOrNull(file) {
  try {
    return fs.readFileSync(file, 'utf8');
  } catch {
    return null;
  }
}

/** Leases and eviction serialize on the broker's own lock, the same one admission uses. */
const brokerLock = (fn) => withLock(ensureStateDirs().root, fn);

/** Hit: copy the stored tree into place. False (with the partial tree removed) means install instead. */
async function materializeHit(depsCache, key, cwd, releaseLeases) {
  const tree = path.join(cwd, 'node_modules');
  if (!fs.existsSync(entryTree(depsCache.root, key))) return false;
  try {
    const release = await acquireLease(depsCache.root, key, depsCache.maxBytes, brokerLock);
    releaseLeases.push(release);
    materializeFromStore(depsCache.root, key, tree);
    touchLastUsed(depsCache.root, key);
    return true;
  } catch (err) {
    process.stderr.write(`deps-cache materialize failed, installing instead: ${err.message}\n`);
    fs.rmSync(tree, { recursive: true, force: true });
    return false;
  }
}

/**
 * npm's own answer for the settings that decide whether root scripts run and under which shell, in the install's
 * env and cwd (so `.npmrc`, env and flags are resolved exactly as `npm ci` will): `npm config get ignore-scripts script-shell`.
 */
function effectiveNpmConfig(cwd, depsEnv) {
  const out = execFileSync('npm', ['config', 'get', 'ignore-scripts', 'script-shell'], { cwd, env: depsEnv, encoding: 'utf8' });
  const value = (key) => out.split('\n').find((l) => l.startsWith(`${key}=`))?.slice(key.length + 1).trim();
  const ignoreScripts = value('ignore-scripts');
  const scriptShell = value('script-shell');
  if (ignoreScripts === undefined || scriptShell === undefined) throw new Error(`unreadable npm config: ${out.slice(0, 80)}`);
  return { ignoreScripts, scriptShell };
}

/** The directory of the npm on PATH (its `bin/npm-cli.js` is two levels down), skipping wrappers that are not npm itself. */
function findNpmRoot(env) {
  for (const dir of (env.PATH ?? '').split(path.delimiter)) {
    try {
      const real = fs.realpathSync(path.join(dir, 'npm'));
      const root = path.dirname(path.dirname(real));
      if (path.basename(real) === 'npm-cli.js' && fs.existsSync(path.join(root, 'node_modules', '@npmcli', 'run-script'))) return root;
    } catch {
      // not on this PATH entry
    }
  }
  return null;
}

// What `npm ci --no-audit --no-fund` puts in a lifecycle script's environment on top of the env it was given
// (npm 11.9.0, compared against a real `npm ci` by dumping `process.env` from a prepare script): `npm_command`
// (lib/npm.js), `INIT_CWD` and the non-default CLI flags as empty `npm_config_*` (@npmcli/config/lib/set-envs.js),
// `COLOR`, `NODE`, `npm_execpath`, `npm_node_execpath` and `npm_config_local_prefix`. Not reproduced, being
// informational echoes of npm's own config that no allowlisted command reads: `npm_config_{prefix,global_prefix,
// init_module,noproxy,npm_version,user_agent}`. The rest comes from @npmcli/run-script, which the replay uses unchanged.
function replayEnv(depsEnv, cwd, npmRoot) {
  return {
    ...depsEnv,
    npm_command: 'ci',
    INIT_CWD: cwd,
    npm_config_audit: '',
    npm_config_fund: '',
    npm_config_local_prefix: cwd,
    npm_execpath: path.join(npmRoot, 'bin', 'npm-cli.js'),
    npm_node_execpath: process.execPath,
    NODE: process.execPath,
    COLOR: '0',
  };
}

const REPLAY_RUN_SCRIPT = `
const run = require(process.argv[1]);
run({ path: process.cwd(), args: [], scriptShell: process.argv[3] || undefined, stdio: 'inherit', event: process.argv[2] })
  .catch((err) => process.exit(typeof err.code === 'number' ? err.code : 1));
`;

/**
 * A hit skips `npm ci`, and with it the allowlisted root scripts whose effect (the work dir's `.git/config`)
 * lives outside the cached tree. Replay each, in npm's order, through npm's own `@npmcli/run-script` called as
 * `lib/commands/ci.js` calls it, with the install's env plus what `npm ci` adds (above). Not replayed at all
 * when npm's effective `ignore-scripts` is true (a miss would not have run them). False means "could not
 * replay": the caller installs instead.
 */
async function replayRootScripts(cwd, scripts, depsEnv, npmConfig) {
  if (npmConfig.ignoreScripts === 'true') return true;
  const events = allowlistedRootEvents(scripts);
  if (events.length === 0) return true;
  const npmRoot = findNpmRoot(depsEnv);
  if (!npmRoot) return false;
  const env = replayEnv(depsEnv, cwd, npmRoot);
  const runScriptModule = path.join(npmRoot, 'node_modules', '@npmcli', 'run-script');
  const shell = npmConfig.scriptShell === 'null' ? '' : npmConfig.scriptShell;
  for (const event of events) {
    const code = await runPhaseCommand([process.execPath, '-e', REPLAY_RUN_SCRIPT, runScriptModule, event, shell], cwd, env);
    if (code !== 0) return false;
  }
  return true;
}

/** Copy the freshly installed tree into the store, then trim the store to its bound under the broker lock. */
async function publishAndEvict(depsCache, key, cwd, releaseLeases) {
  const result = publishToStore(depsCache.root, key, path.join(cwd, 'node_modules'));
  if (!result.published) return result;
  touchLastUsed(depsCache.root, key);
  try {
    releaseLeases.push(await acquireLease(depsCache.root, key, depsCache.maxBytes, brokerLock));
    await trimStore(depsCache.root, depsCache.maxBytes, brokerLock);
  } catch (err) {
    // this run has its own tree whatever happens to the store; the next publish or release trims it
    process.stderr.write(`deps-cache lease or eviction skipped: ${err.message}\n`);
  }
  return result;
}

/** Install one deps dir: from the store on a hit, else `npm ci` (then publish when that is safe). */
async function installDepsDir({ dir, workDir, depsEnv, depsCache, releaseLeases, npmVersion }) {
  const started = Date.now();
  const cwd = dir === '.' ? workDir : path.join(workDir, dir);
  const done = (outcome, fields = {}) => {
    const ms = Date.now() - started;
    logDepsCache(outcome, dir, ms, fields);
    return {
      dir,
      outcome,
      ms,
      ...(fields.key ? { key: fields.key.slice(0, 12) } : {}),
      ...(fields.reason ? { reason: fields.reason } : {}),
      ...(fields.file ? { file: fields.file } : {}),
      ...(fields.missing ? { missing: fields.missing } : {}),
    };
  };

  let keyed = { key: null, reason: 'disabled' };
  let npmConfig;
  if (depsCache?.enabled) {
    try {
      npmConfig = effectiveNpmConfig(cwd, depsEnv);
      keyed = computeDepsKey({
        npmConfig,
        dir: cwd,
        rootDir: workDir,
        installArgv: DEPS_INSTALL_ARGV,
        env: depsEnv,
        npmVersion: npmVersion(),
        rootScriptsSafe: depsCache.rootScriptsSafe === true,
      });
    } catch (err) {
      keyed = { key: null, reason: `key not computed: ${err.message}` };
    }
  }
  const { key } = keyed;

  if (key && (await materializeHit(depsCache, key, cwd, releaseLeases))) {
    if (await replayRootScripts(cwd, keyed.scripts, depsEnv, npmConfig)) return { exitCode: 0, record: done('hit', { key }) };
    process.stderr.write('deps-cache replay of the root scripts failed, installing instead\n');
    fs.rmSync(path.join(cwd, 'node_modules'), { recursive: true, force: true });
  }

  const before = key ? snapshotInstallSurface(cwd, workDir) : null;
  const configBefore = key ? readTextOrNull(path.join(workDir, '.git', 'config')) : null;
  const exitCode = await runPhaseCommand(DEPS_INSTALL_ARGV, cwd, depsEnv);
  if (!key) return { exitCode, record: done('skip', { reason: keyed.reason }) };
  if (exitCode !== 0) return { exitCode, record: done('miss', { key, published: false, reason: 'install failed' }) };

  const configAfter = readTextOrNull(path.join(workDir, '.git', 'config'));
  const outside = unexplainedChanges(snapshotChanges(before, snapshotInstallSurface(cwd, workDir)), { scripts: keyed.scripts, configBefore, configAfter });
  if (outside.length > 0) {
    const shown = outside.slice(0, 3).join(', ');
    return { exitCode, record: done('miss', { key, published: false, reason: `install changed files outside node_modules: ${shown}${outside.length > 3 ? ', ...' : ''}` }) };
  }
  if (!fs.existsSync(path.join(cwd, 'node_modules'))) {
    return { exitCode, record: done('miss', { key, published: false, reason: 'install produced no node_modules' }) };
  }
  const missing = missingInstalled(path.join(cwd, 'node_modules'), keyed.lock);
  if (missing) return { exitCode, record: done('skip', { key, reason: 'incomplete-optional', missing }) };
  const runPaths = PER_RUN_PATH_ENV_NAMES.map((name) => depsEnv[name]).filter(Boolean);
  const installPaths = [...new Set([workDir, fs.realpathSync(workDir), ...runPaths])];
  const embedded = findInstallPathReference(path.join(cwd, 'node_modules'), installPaths);
  if (embedded) return { exitCode, record: done('skip', { key, reason: 'absolute-install-path', file: embedded }) };
  try {
    const result = await publishAndEvict(depsCache, key, cwd, releaseLeases);
    return { exitCode, record: done('miss', { key, published: result.published, ...(result.published ? {} : { reason: 'another run published this key first' }) }) };
  } catch (err) {
    // the tree is installed and usable for this run whatever happened to the store
    return { exitCode, record: done('miss', { key, published: false, reason: `publish failed: ${err.message}` }) };
  }
}

/** Lock entries for this platform that npm's own record says it did not install (or `['.package-lock.json']` if it left no record). */
function missingInstalled(tree, lock) {
  const hidden = readJsonSafe(path.join(tree, '.package-lock.json'));
  if (!hidden?.packages) return ['.package-lock.json'];
  const { libc } = currentSystem();
  const missing = findMissingInstalled(lock, hidden.packages, { platform: process.platform, arch: process.arch, libc });
  return missing.length > 0 ? missing : null;
}

function aggregateDepsOutcome(records) {
  if (records.some((r) => r.outcome === 'miss')) return 'miss';
  return records.length > 0 && records.every((r) => r.outcome === 'hit') ? 'hit' : 'skip';
}

/**
 * The deps phase: every dir in order, stopping at the first failed install. Records
 * `<ticketDir>/deps.json` (outcome, wall ms, per-dir detail) for `remote-exec` to put in the result.
 */
async function runDepsPhase({ ticketDir, workDir, remoteDeps, depsEnv, depsCache, releaseLeases }) {
  const phaseStart = Date.now();
  let npmVersionMemo;
  const npmVersion = () => {
    npmVersionMemo ??= execFileSync('npm', ['--version'], { cwd: workDir, env: depsEnv, encoding: 'utf8' }).trim();
    return npmVersionMemo;
  };
  const records = [];
  let exitCode = 0;
  for (const dir of remoteDeps) {
    const r = await installDepsDir({ dir, workDir, depsEnv, depsCache, releaseLeases, npmVersion });
    records.push(r.record);
    if (r.exitCode !== 0) {
      exitCode = r.exitCode;
      break;
    }
  }
  atomicWriteJson(path.join(ticketDir, 'deps.json'), { outcome: aggregateDepsOutcome(records), ms: Date.now() - phaseStart, dirs: records });
  return exitCode;
}

/**
 * `lane remote-pipeline <ticketDir>`: read `<ticketDir>/pipeline.json`
 * (written by remote-exec before this process is ever spawned) and run, in
 * order, deps (if any) -> setup (if any) -> command, recording `phase`
 * durably right before each one starts. The first nonzero exit stops the
 * pipeline and becomes this process's own exit code.
 */
export async function remotePipelineCommand(ticketDir) {
  const payload = readJsonSafe(path.join(ticketDir, 'pipeline.json'));
  if (!payload) {
    process.stderr.write(`lane remote-pipeline: missing or invalid pipeline.json in ${ticketDir}\n`);
    return { exitCode: 1 };
  }
  const { workDir, relCwd, remoteDeps = [], remoteSetup = [], argv, npmCacheDir, npmUserConfig, depsCache } = payload;
  const npmGlobalConfig = path.join(path.dirname(npmUserConfig), 'npmrc-global');
  ensureNpmrcFile(npmUserConfig);
  ensureNpmrcFile(npmGlobalConfig);

  // A materialized tree is leased until the whole pipeline ends, so eviction never takes a key a running lane holds.
  const releaseLeases = [];
  try {
    if (remoteDeps.length > 0) {
      writePhase(ticketDir, 'deps');
      const depsEnv = buildDepsEnv(npmCacheDir, npmUserConfig, npmGlobalConfig);
      const code = await runDepsPhase({ ticketDir, workDir, remoteDeps, depsEnv, depsCache, releaseLeases });
      if (code !== 0) return { exitCode: code };
    }

    if (remoteSetup.length > 0) {
      writePhase(ticketDir, 'setup');
      for (const setupArgv of remoteSetup) {
        const code = await runPhaseCommand(setupArgv, workDir, process.env);
        if (code !== 0) return { exitCode: code };
      }
    }

    writePhase(ticketDir, 'command');
    const cmdCwd = path.join(workDir, relCwd || '');
    const code = await runPhaseCommand(argv, cmdCwd, process.env);
    return { exitCode: code };
  } finally {
    for (const release of releaseLeases) {
      try {
        await release();
      } catch (err) {
        process.stderr.write(`deps-cache lease release skipped: ${err.message}\n`);
      }
    }
  }
}
