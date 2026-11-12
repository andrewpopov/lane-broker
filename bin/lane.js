#!/usr/bin/env node
// First import on purpose (ESM hoists imports): BRAIN-577, see src/node-exit-safe.js.
import '../src/node-exit-safe-boot.js';
import { runCommand } from '../src/run.js';
import { statusCommand } from '../src/status.js';
import { suggestCommand } from '../src/suggest.js';
import { estimatesCommand } from '../src/estimates-history.js';
import { cancelCommand } from '../src/cancel.js';
import { priorityCommand } from '../src/retier.js';
import { waitCommand } from '../src/wait.js';
import { pauseCommand, resumeCommand, drainCommand, drainShutdownCommand, undrainCommand } from '../src/pause.js';
import { migrateSchedulerCommand } from '../src/migrate.js';
import { parseByteSize } from '../src/resources.js';
import { MIN_CPU_GRANT_CORES } from '../src/cpu-limit.js';
import { isTicketId, stateHome } from '../src/state.js';
import { writeBrokerLog } from '../src/admission.js';
import { leaseCheckCommand } from '../src/lease.js';
import { parseTransferFlag } from '../src/remote-gzip.js';
import { parseTransferCacheFlag } from '../src/remote-blobs.js';
import {
  remoteExecCommand,
  remoteHaveCommand,
  remoteProbeCommand,
  remoteResultCommand,
  remoteCancelCommand,
  remoteWithdrawCommand,
  remoteArtifactsCommand,
  remoteArtifactsReleaseCommand,
  defaultRemoteRoot,
} from '../src/remote-runner.js';
import { parseReadinessArgs } from '../src/runner-readiness.js';
import { gcCommand } from '../src/gc.js';
import { loadGlobalConfig } from '../src/config.js';
import { runnerWatchCommand, runnerStatusCommand } from '../src/runner-watch.js';
import { runnerPrewarmCommand } from '../src/runner-prewarm.js';
import { reconcileCommand } from '../src/reconcile-sweep.js';
import { capabilitiesCommand } from '../src/capabilities.js';
import { remotePipelineCommand } from '../src/remote-pipeline.js';
import { costReportCommand } from '../src/cost-report.js';
import { phasesReportCommand } from '../src/phases-report.js';
import { lossesReportCommand } from '../src/losses-report.js';
import { placementReportCommand } from '../src/placement-report.js';

function usage() {
  return `Usage:
  lane run [--repo <name>] [--lane <name>] [--weight <n>] [--cpu <cores>] [--memory <size>] [--priority high|medium|low] [--group <id>] [--exclusive [--exclusive-purpose upgrade|calibration] [--exclusive-deadline <dur>]] [--no-cache] [--no-reuse] [--retry "<why>"] [--detach [--id <ticket-id>]]
           [--timeout <duration>] [--allow-local-sim] [--local] [--log <path>] -- <command...>
  lane status [--json]
  lane capabilities --json
  lane suggest [--repo <name>] [--days <n>] [--json]
  lane estimates [--json] [--root <stateHome>]
  lane cancel <id>
  lane lease-check <id>
  lane priority <id> high|medium|low
  lane wait <id> [--timeout <duration>]
  lane pause ["reason"]
  lane resume
  lane drain ["reason"]
  lane drain --shutdown ["reason"] [--wait-ms N]
  lane undrain
  lane migrate-scheduler [--dry-run | --when-idle [--timeout <duration>]]
  lane gc [--dry-run] [--max <n>] [--root <remoteRoot>]
  lane reconcile [--now] [--json]
  lane runner [status] [--json]
  lane runner watch
  lane runner prewarm [--lane <name>] [--runner <name>] [--dry-run]
  lane report phases --repo <a[,b]> --since <iso> --until <iso> [--history <file>] [--json]
  lane report losses --since <iso> --until <iso> [--repo <a[,b]>] [--history <file>] [--json]
  lane report cost --repo <a[,b]> --since <iso> --until <iso> [--prs <file | repo=file,...>] [--journal-root <dir>] [--history <file>] [--json]
  lane report placement --since <iso> [--until <iso>] [--history <file>] [--json]
`;
}

function parseRunArgs(args) {
  const sep = args.indexOf('--');
  const flagArgs = sep === -1 ? args : args.slice(0, sep);
  const cmd = sep === -1 ? [] : args.slice(sep + 1);
  const opts = { detach: false, allowLocalSim: false };
  for (let i = 0; i < flagArgs.length; i += 1) {
    const a = flagArgs[i];
    switch (a) {
      case '--repo':
        opts.repo = flagArgs[++i];
        break;
      case '--lane':
        opts.lane = flagArgs[++i];
        break;
      case '--weight': {
        const raw = flagArgs[++i];
        const n = Number(raw);
        if (!Number.isFinite(n) || n <= 0) {
          throw new Error(`--weight must be a positive number (got "${raw}")`);
        }
        opts.weightOverride = n;
        break;
      }
      case '--cpu': {
        const raw = flagArgs[++i];
        const n = Number(raw);
        if (!Number.isFinite(n) || n < MIN_CPU_GRANT_CORES) throw new Error(`--cpu must be a number >= ${MIN_CPU_GRANT_CORES} (got "${raw}")`);
        opts.cpuOverride = n;
        break;
      }
      case '--memory': {
        const raw = flagArgs[++i];
        const bytes = parseByteSize(raw);
        if (bytes === null) throw new Error(`--memory must be a positive size such as 2GiB or 512MiB (got "${raw}")`);
        opts.memoryOverride = bytes;
        break;
      }
      // Validated (exit 64) by runCommand, together with the env and config sources; a missing value is invalid, not absent.
      case '--priority':
        opts.priority = flagArgs[++i] ?? '';
        break;
      // Validated (exit 64) by runCommand.
      case '--group':
        opts.group = flagArgs[++i] ?? '';
        break;
      case '--exclusive':
        opts.exclusive = true;
        break;
      // Validated (exit 64) by runCommand, together with the lane config's keys.
      case '--exclusive-purpose':
        opts.exclusivePurpose = flagArgs[++i] ?? '';
        break;
      case '--exclusive-deadline':
        opts.exclusiveDeadline = flagArgs[++i] ?? '';
        break;
      case '--no-cache':
        opts.noCache = true;
        break;
      // BRAIN-700: a hypothesis for re-running content that already failed; blank is a usage error
      case '--retry': {
        const raw = flagArgs[++i];
        if (!raw || !raw.trim() || raw.startsWith('--')) throw new Error('--retry needs a non-empty reason');
        opts.retryReason = raw;
        break;
      }
      case '--no-reuse':
        opts.noReuse = true;
        break;
      case '--detach':
        opts.detach = true;
        break;
      case '--timeout':
        opts.timeout = flagArgs[++i];
        break;
      case '--allow-local-sim':
        opts.allowLocalSim = true;
        break;
      // BRAIN-319 T3b-1: forces a remote-eligible lane to run locally.
      case '--local':
        opts.local = true;
        break;
      // BRAIN-547: on a remote-eligible lane `--local` is honoured only with a reason; a reason alone implies local.
      case '--local-reason': {
        const raw = flagArgs[++i];
        if (!raw || raw.startsWith('--')) throw new Error('--local-reason needs a non-empty text');
        opts.localReason = raw;
        break;
      }
      case '--log':
        opts.log = flagArgs[++i];
        break;
      case '--id': {
        const raw = flagArgs[++i];
        if (!isTicketId(raw)) throw new Error(`--id must be 1-128 characters of [A-Za-z0-9._-] (got "${raw}")`);
        opts.ticketId = raw;
        break;
      }
      default:
        throw new Error(`lane run: unknown flag "${a}"`);
    }
  }
  if (opts.ticketId !== undefined && !opts.detach) throw new Error('--id requires --detach');
  return { opts, cmd };
}

/** BRAIN-319 T2 hidden remote-* subcommands: `--root <dir>` only. */
function parseRootFlag(args) {
  const idx = args.indexOf('--root');
  return idx === -1 ? undefined : args[idx + 1];
}

/** BRAIN-1035 `remote-exec` flags a pull agent passes: `--epoch N` (a positive integer), `--runner-name S`, `--runner-speed-factor X`. Values are validated downstream by the header rules. */
function parseAgentExecFlags(args) {
  const options = {};
  for (const [flag, key, convert] of [['--epoch', 'epoch', Number], ['--runner-name', 'runnerName', String], ['--runner-speed-factor', 'runnerSpeedFactor', Number]]) {
    const idx = args.indexOf(flag);
    if (idx === -1) continue;
    if (args[idx + 1] === undefined) return { ok: false, reason: `${flag} needs a value` };
    options[key] = convert(args[idx + 1]);
  }
  if (options.epoch !== undefined && !(Number.isSafeInteger(options.epoch) && options.epoch >= 1)) return { ok: false, reason: '--epoch needs a positive integer' };
  return { ok: true, options };
}

function parseDurationMs(spec) {
  const m = String(spec).trim().match(/^(\d+(?:\.\d+)?)(ms|s|m|h)?$/);
  if (!m) throw new Error(`invalid duration "${spec}" (expected e.g. 30s, 5m, 500ms)`);
  const n = Number(m[1]);
  const unit = m[2] || 's';
  const mult = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[unit];
  return n * mult;
}

async function main() {
  const [sub, ...rest] = process.argv.slice(2);

  if (!sub || sub === '--help' || sub === '-h') {
    process.stdout.write(usage());
    return 0;
  }

  switch (sub) {
    case 'run': {
      let opts;
      let cmd;
      try {
        ({ opts, cmd } = parseRunArgs(rest));
      } catch (err) {
        process.stderr.write(`lane run: ${err.message}\n`);
        return 2;
      }
      const result = await runCommand({
        repo: opts.repo,
        lane: opts.lane,
        weightOverride: opts.weightOverride,
        cpuOverride: opts.cpuOverride,
        memoryOverride: opts.memoryOverride,
        priority: opts.priority,
        group: opts.group,
        exclusive: opts.exclusive,
        exclusivePurpose: opts.exclusivePurpose,
        exclusiveDeadline: opts.exclusiveDeadline,
        noCache: opts.noCache,
        noReuse: opts.noReuse,
        retryReason: opts.retryReason,
        detach: opts.detach,
        timeoutMs: opts.timeout ? parseDurationMs(opts.timeout) : undefined,
        allowLocalSim: opts.allowLocalSim,
        local: opts.local,
        localReason: opts.localReason,
        log: opts.log,
        ticketId: opts.ticketId,
        cmd,
      });
      return result.exitCode;
    }
    case 'status': {
      const json = rest.includes('--json');
      const result = await statusCommand({ json });
      return result.exitCode;
    }
    case 'suggest': {
      const flag = (name) => (rest.indexOf(name) === -1 ? undefined : rest[rest.indexOf(name) + 1]);
      const days = flag('--days') === undefined ? 7 : Number(flag('--days'));
      if (!(days > 0)) {
        process.stderr.write('lane suggest: --days must be a positive number\n');
        return 2;
      }
      const result = await suggestCommand({ repo: flag('--repo'), days, json: rest.includes('--json') });
      return result.exitCode;
    }
    case 'estimates': {
      const rootAt = rest.indexOf('--root');
      const result = await estimatesCommand({ root: rootAt === -1 ? undefined : rest[rootAt + 1], json: rest.includes('--json') });
      return result.exitCode;
    }
    case 'cancel': {
      const id = rest[0];
      if (!id) {
        process.stderr.write('lane cancel: missing <id>\n');
        return 2;
      }
      const result = await cancelCommand(id);
      return result.exitCode;
    }
    case 'lease-check':
      return leaseCheckCommand(rest).exitCode;
    case 'priority': {
      const unknownOption = rest.find((arg) => arg.startsWith('-'));
      if (unknownOption !== undefined) {
        process.stderr.write(`lane priority: unknown option "${unknownOption}"\n${usage()}`);
        return 2;
      }
      if (rest.length !== 2) {
        process.stderr.write('lane priority: usage: lane priority <id> high|medium|low (missing <id>|<tier>, or extra arguments)\n');
        return 2;
      }
      const result = await priorityCommand(rest[0], rest[1]);
      return result.exitCode;
    }
    case 'wait': {
      const id = rest[0];
      if (!id) {
        process.stderr.write('lane wait: missing <id>\n');
        return 2;
      }
      const timeoutFlagIdx = rest.indexOf('--timeout');
      const timeoutMs = timeoutFlagIdx !== -1 ? parseDurationMs(rest[timeoutFlagIdx + 1]) : undefined;
      const result = await waitCommand(id, { timeoutMs });
      return result.exitCode;
    }
    case 'pause': {
      const reason = rest.join(' ');
      const result = await pauseCommand(reason);
      return result.exitCode;
    }
    case 'resume': {
      const result = await resumeCommand();
      return result.exitCode;
    }
    case 'drain': {
      if (rest.includes('--shutdown')) {
        const startedAt = Date.now();
        let waitMs = 60_000;
        let remoteRoot;
        const args = [];
        for (let i = 0; i < rest.length; i += 1) {
          if (rest[i] === '--shutdown') continue;
          if (rest[i] === '--wait-ms') {
            waitMs = Number(rest[i + 1]);
            i += 1;
            if (!Number.isFinite(waitMs) || waitMs < 0) {
              process.stderr.write(`lane drain: --wait-ms needs a non-negative number\n${usage()}`);
              return 2;
            }
          } else if (rest[i] === '--root' && rest[i + 1]) {
            remoteRoot = rest[i + 1];
            i += 1;
          } else args.push(rest[i]);
        }
        const bad = args.find((arg) => arg.startsWith('-'));
        if (bad !== undefined || args.length > 1) {
          process.stderr.write(`lane drain --shutdown: ${bad !== undefined ? `unknown option "${bad}"` : 'unexpected argument (quote a multi-word reason)'}\n${usage()}`);
          return 2;
        }
        const deadline = startedAt + waitMs;
        const result = await drainShutdownCommand(args[0] ?? '', { waitMs, root: remoteRoot, deadline });
        if (Number(process.env.LANE_BROKER_TEST_STDOUT_FILL_BYTES) > 0) process.stdout.write('x'.repeat(Number(process.env.LANE_BROKER_TEST_STDOUT_FILL_BYTES))); // test seam: a stdout nobody reads
        await flushStdio(stateHome(), Math.max(0, deadline - Date.now()));
        process.exit(result.exitCode);
      }
      const unknownOption = rest.find((arg) => arg.startsWith('-'));
      if (unknownOption !== undefined) {
        process.stderr.write(`lane drain: unknown option "${unknownOption}"\n${usage()}`);
        return 2;
      }
      if (rest.length > 1) {
        process.stderr.write(`lane drain: unexpected argument "${rest[1]}" (quote a multi-word reason)\n${usage()}`);
        return 2;
      }
      const result = await drainCommand(rest[0] ?? '');
      return result.exitCode;
    }
    case 'undrain': {
      if (rest.length > 0) {
        process.stderr.write(`lane undrain: unexpected argument "${rest[0]}"\n${usage()}`);
        return 2;
      }
      const result = await undrainCommand();
      return result.exitCode;
    }
    case 'migrate-scheduler': {
      const timeoutIdx = rest.indexOf('--timeout');
      const flags = rest.filter((a, i) => !(timeoutIdx !== -1 && (i === timeoutIdx || i === timeoutIdx + 1)));
      const unknown = flags.filter((a) => a !== '--dry-run' && a !== '--when-idle');
      if (unknown.length > 0) {
        process.stderr.write(`lane migrate-scheduler: unknown argument "${unknown[0]}"\n`);
        return 2;
      }
      const dryRun = flags.includes('--dry-run');
      const whenIdle = flags.includes('--when-idle');
      if ((dryRun && whenIdle) || (timeoutIdx !== -1 && !whenIdle)) {
        process.stderr.write('lane migrate-scheduler: --when-idle excludes --dry-run, and --timeout needs --when-idle\n');
        return 2;
      }
      let timeoutMs;
      if (timeoutIdx !== -1) {
        try {
          timeoutMs = parseDurationMs(rest[timeoutIdx + 1]);
        } catch (err) {
          process.stderr.write(`lane migrate-scheduler: ${err.message}\n`);
          return 2;
        }
      }
      const result = await migrateSchedulerCommand({ dryRun, whenIdle, ...(timeoutMs === undefined ? {} : { timeoutMs }) });
      return result.exitCode;
    }
    case 'gc': {
      const result = await gcCommand(rest, { remoteRoot: parseRootFlag(rest) || defaultRemoteRoot(), cfg: loadGlobalConfig() });
      return result.exitCode;
    }
    case 'runner': {
      const [sub = 'status', ...args] = rest;
      const bad = (msg) => {
        process.stderr.write(`lane runner: ${msg}\n${usage()}`);
        return 2;
      };
      if (sub === 'prewarm') return (await runnerPrewarmCommand(args)).exitCode;
      if (sub === 'watch') return args.length === 0 ? (await runnerWatchCommand()).exitCode : bad(`unexpected argument "${args[0]}"`);
      if (sub === 'status' || sub === '--json') {
        const flags = sub === '--json' ? ['--json', ...args] : args;
        const unknown = flags.find((a) => a !== '--json');
        return unknown === undefined ? runnerStatusCommand({ json: flags.includes('--json') }).exitCode : bad(`unknown argument "${unknown}"`);
      }
      return bad(`unknown subcommand "${sub}"`);
    }
    case 'reconcile': {
      const result = await reconcileCommand({ force: rest.includes('--now'), json: rest.includes('--json') });
      return result.exitCode;
    }
    case 'report': {
      if (rest[0] === 'phases') return (await phasesReportCommand(rest.slice(1))).exitCode;
      if (rest[0] === 'losses') return (await lossesReportCommand(rest.slice(1))).exitCode;
      if (rest[0] === 'placement') return (await placementReportCommand(rest.slice(1))).exitCode;
      if (rest[0] !== 'cost') {
        process.stderr.write('lane report: only `cost`, `phases`, `losses` and `placement` are supported\n');
        return 2;
      }
      return (await costReportCommand(rest.slice(1))).exitCode;
    }
    case 'capabilities': {
      if (!rest.includes('--json')) {
        process.stderr.write('lane capabilities: only --json is supported\n');
        return 2;
      }
      return capabilitiesCommand().exitCode;
    }
    // BRAIN-319 T2: hidden runner-side subcommands, invoked by a client Mac
    // over ssh — deliberately not listed in usage() above.
    case 'remote-probe': {
      const result = await remoteProbeCommand({ root: parseRootFlag(rest) || defaultRemoteRoot(), needs: parseReadinessArgs(rest) });
      return result.exitCode;
    }
    case 'remote-exec': {
      const root = parseRootFlag(rest) || defaultRemoteRoot();
      const flag = parseTransferFlag(rest);
      if (!flag.ok) {
        process.stderr.write(`lane remote-exec: ${flag.reason}\n`);
        return 2;
      }
      const cacheFlag = parseTransferCacheFlag(rest);
      if (!cacheFlag.ok) {
        process.stderr.write(`lane remote-exec: ${cacheFlag.reason}\n`);
        return 2;
      }
      const agentFlags = parseAgentExecFlags(rest);
      if (!agentFlags.ok) {
        process.stderr.write(`lane remote-exec: ${agentFlags.reason}\n`);
        return 2;
      }
      if (process.env.LANE_BROKER_TEST_RETAIN_HANDLE) setInterval(() => {}, 1000);
      const result = await remoteExecCommand({ root, transfer: flag.transfer, transferCache: cacheFlag.transferCache, ...agentFlags.options });
      await flushStdio(root);
      process.exit(result.exitCode);
    }
    case 'remote-have': {
      const result = await remoteHaveCommand({ root: parseRootFlag(rest) || defaultRemoteRoot() });
      return result.exitCode;
    }
    case 'remote-result': {
      const id = rest[0];
      if (!id) {
        process.stderr.write('lane remote-result: missing <ticketId>\n');
        return 2;
      }
      const root = parseRootFlag(rest.slice(1)) || defaultRemoteRoot();
      const result = await remoteResultCommand(id, { root });
      return result.exitCode;
    }
    case 'remote-artifacts':
    case 'remote-artifacts-release': {
      const id = rest[0];
      if (!id) {
        process.stderr.write(`lane ${sub}: missing <ticketId>\n`);
        return 2;
      }
      const root = parseRootFlag(rest.slice(1)) || defaultRemoteRoot();
      const result = await (sub === 'remote-artifacts' ? remoteArtifactsCommand : remoteArtifactsReleaseCommand)(id, { root });
      return result.exitCode;
    }
    case 'remote-cancel': {
      const id = rest[0];
      if (!id) {
        process.stderr.write('lane remote-cancel: missing <ticketId>\n');
        return 2;
      }
      const root = parseRootFlag(rest.slice(1)) || defaultRemoteRoot();
      const result = await remoteCancelCommand(id, { root });
      return result.exitCode;
    }
    case 'remote-withdraw': {
      const id = rest[0];
      if (!id) {
        process.stderr.write('lane remote-withdraw: missing <ticketId>\n');
        return 2;
      }
      const root = parseRootFlag(rest.slice(1)) || defaultRemoteRoot();
      const result = await remoteWithdrawCommand(id, { root });
      return result.exitCode;
    }
    // BRAIN-320 S1b: hidden -- the pipeline `remote-exec` spawns as a
    // protocol-2 ticket's own child, never invoked by a human or a client.
    case 'remote-pipeline': {
      const ticketDir = rest[0];
      if (!ticketDir) {
        process.stderr.write('lane remote-pipeline: missing <ticketDir>\n');
        return 2;
      }
      const result = await remotePipelineCommand(ticketDir);
      return result.exitCode;
    }
    default:
      process.stderr.write(`lane: unknown command "${sub}"\n\n${usage()}`);
      return 2;
  }
}

// A runner's remote-exec has published its result by the time it returns; an unidentified live handle must not keep the
// ssh channel open (BRAIN-850). Flush both streams (EPIPE tolerated) so no output is truncated, then the caller exits.
// The bound is generous (a slow but healthy reader is waited for); when it expires the result stays published and the exit code
// stays, but the truncation is recorded in the broker log, where it is visible even if stdout and stderr are the stuck streams.
// Test-only seam: LANE_BROKER_TEST_FLUSH_MS.
async function flushStdio(root, timeoutMs = Number(process.env.LANE_BROKER_TEST_FLUSH_MS) || 60_000) {
  if (timeoutMs <= 0) {
    writeBrokerLog(root, 'lane: stdout/stderr flush skipped: the deadline had passed; output may be truncated\n');
    return;
  }
  const flush = (stream) =>
    new Promise((resolve) => {
      stream.on('error', () => resolve());
      try {
        stream.write('', () => resolve());
      } catch {
        resolve();
      }
    });
  let timer;
  const timedOut = Symbol('flush-timed-out');
  const bound = new Promise((resolve) => {
    timer = setTimeout(() => resolve(timedOut), timeoutMs);
  });
  const outcome = await Promise.race([Promise.all([flush(process.stdout), flush(process.stderr)]), bound]);
  clearTimeout(timer);
  if (outcome === timedOut) {
    const line = `lane: remote-exec: stdout/stderr did not flush within ${timeoutMs}ms; exiting with the result already published, output may be truncated\n`;
    writeBrokerLog(root, line);
    try {
      process.stderr.write(line);
    } catch {
      // the stream that is stuck is the reason for this line
    }
  }
}

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((err) => {
    process.stderr.write(`lane: ${err.message}\n`);
    process.exitCode = 1;
  });
