import { randomUUID } from 'node:crypto';
import { loadGlobalConfigOrError, loadRepoConfig, resolveTicketConfig, ConfigError } from './config.js';
import { dispatchRemote } from './remote-client.js';
import { gitRevParse } from './run.js';

/**
 * BRAIN-649: `lane runner prewarm [--lane <name>] [--runner <name>] [--dry-run]`, run from a repo checkout. A lane that sets
 * `prewarm: true` has `remoteDeps`/`remoteSetup` worth warming (towerpower's `art:pull`, a cold dependency install). This sends that
 * lane's snapshot to each runner with the command `true` at LOW priority, so the runner's own broker runs the setup through the same
 * path a real ticket takes and leaves its per-machine caches warm. It is opt-in per lane and never automatic: nothing schedules it yet.
 * A lane for this should not be `remoteExclusive` (the run would hold the runner); give it a dedicated lane with the same setup.
 */

const PREWARM_WEIGHT = 1;
const PREWARM_MEMORY_BYTES = 1073741824;

export function parsePrewarmArgs(args) {
  const out = { lane: undefined, runner: undefined, dryRun: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dry-run') out.dryRun = true;
    else if (args[i] === '--lane' && args[i + 1]) out.lane = args[++i];
    else if (args[i] === '--runner' && args[i + 1]) out.runner = args[++i];
    else return { error: `unknown argument "${args[i]}"` };
  }
  return out;
}

/** The lane to prewarm: `--lane`, else the repo's only `prewarm: true` lane; `{ error }` when that is not unambiguous. */
export function pickPrewarmLane(repoConfig, requested) {
  const flagged = Object.entries(repoConfig.fileLanes ?? repoConfig.lanes ?? {}).filter(([, l]) => l.prewarm === true).map(([name]) => name);
  if (requested) return flagged.includes(requested) ? { lane: requested } : { error: `lane "${requested}" does not set prewarm: true` };
  if (flagged.length === 1) return { lane: flagged[0] };
  return { error: flagged.length === 0 ? 'no lane sets prewarm: true' : `several lanes set prewarm: true (${flagged.join(', ')}); pass --lane` };
}

export async function runnerPrewarmCommand(
  args,
  { cwd = process.cwd(), dispatch = dispatchRemote, cfg, out = (t) => process.stdout.write(`${t}\n`), err = (t) => process.stderr.write(`${t}\n`) } = {},
) {
  const parsed = parsePrewarmArgs(args);
  if (parsed.error) {
    err(`lane runner prewarm: ${parsed.error}`);
    return { exitCode: 2 };
  }
  let globalCfg = cfg;
  if (!globalCfg) {
    const loaded = loadGlobalConfigOrError();
    if (loaded.error) {
      err(`lane runner prewarm: global config unreadable: ${loaded.error}`);
      return { exitCode: 1 };
    }
    globalCfg = loaded.config;
  }
  let resolved;
  try {
    const picked = pickPrewarmLane(loadRepoConfig(cwd), parsed.lane);
    if (picked.error) {
      err(`lane runner prewarm: ${picked.error}`);
      return { exitCode: 64 };
    }
    resolved = resolveTicketConfig({ cwd, lane: picked.lane });
  } catch (e) {
    if (!(e instanceof ConfigError)) throw e;
    err(`lane runner prewarm: ${e.message}`);
    return { exitCode: 64 };
  }
  if (!resolved.remoteSetup && !resolved.remoteDeps) {
    err(`lane runner prewarm: lane "${resolved.lane}" declares neither remoteDeps nor remoteSetup, so there is nothing to warm`);
    return { exitCode: 64 };
  }
  const worktreeRoot = gitRevParse(cwd, '--show-toplevel');
  if (!worktreeRoot) {
    err('lane runner prewarm: not inside a git checkout');
    return { exitCode: 64 };
  }
  const runners = (globalCfg.runners ?? []).filter((r) => !r.offline && (!parsed.runner || r.name === parsed.runner));
  if (runners.length === 0) {
    err(`lane runner prewarm: no ${parsed.runner ? `runner named "${parsed.runner}"` : 'usable runner'} configured`);
    return { exitCode: 64 };
  }
  let failed = 0;
  for (const runner of runners) {
    if (parsed.dryRun) {
      out(`${runner.name}: would prewarm lane ${resolved.lane} (remoteDeps ${JSON.stringify(resolved.remoteDeps)}, ${resolved.remoteSetup?.length ?? 0} setup command(s)) at low priority`);
      continue;
    }
    const startedAt = Date.now();
    const result = await dispatch({
      runner,
      worktreeRoot,
      relCwd: '',
      repoKey: resolved.repoId,
      lane: resolved.lane,
      weight: PREWARM_WEIGHT,
      cpuCores: PREWARM_WEIGHT,
      memoryBytes: PREWARM_MEMORY_BYTES,
      argv: ['true'],
      ticketId: randomUUID(),
      generation: 0,
      remoteDeps: resolved.remoteDeps,
      remoteSetup: resolved.remoteSetup,
      remoteDepsCache: resolved.remoteDepsCache,
      remoteDepsCacheRootScriptsSafe: resolved.remoteDepsCacheRootScriptsSafe,
      remoteOmitEscapingSymlinks: resolved.remoteOmitEscapingSymlinks,
      priorityRequested: 'low',
    });
    const ok = result.outcome === 'confirmed' && result.exitCode === 0;
    if (!ok) failed++;
    const detail = `${result.outcome}${result.exitCode !== undefined ? ` exit ${result.exitCode}` : ''}${result.reason ? `: ${result.reason}` : ''}`;
    out(`${runner.name}: ${ok ? 'warm' : `not warm (${detail})`} in ${Math.round((Date.now() - startedAt) / 1000)}s`);
  }
  return { exitCode: failed === 0 ? 0 : 1 };
}
