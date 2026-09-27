import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { atomicWriteFile, readJsonSafe } from './state.js';

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
 * not from anything here.
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
  return env;
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
  const { workDir, relCwd, remoteDeps = [], remoteSetup = [], argv, npmCacheDir, npmUserConfig } = payload;
  const npmGlobalConfig = path.join(path.dirname(npmUserConfig), 'npmrc-global');
  ensureNpmrcFile(npmUserConfig);
  ensureNpmrcFile(npmGlobalConfig);

  if (remoteDeps.length > 0) {
    writePhase(ticketDir, 'deps');
    const depsEnv = buildDepsEnv(npmCacheDir, npmUserConfig, npmGlobalConfig);
    for (const dir of remoteDeps) {
      const cwd = dir === '.' ? workDir : path.join(workDir, dir);
      const code = await runPhaseCommand(['npm', 'ci', '--no-audit', '--no-fund'], cwd, depsEnv);
      if (code !== 0) return { exitCode: code };
    }
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
}
