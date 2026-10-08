#!/usr/bin/env node
import { runCommand } from '../src/run.js';
import { statusCommand } from '../src/status.js';
import { suggestCommand } from '../src/suggest.js';
import { estimatesCommand } from '../src/estimates-history.js';
import { cancelCommand } from '../src/cancel.js';
import { waitCommand } from '../src/wait.js';
import { pauseCommand, resumeCommand } from '../src/pause.js';
import { migrateSchedulerCommand } from '../src/migrate.js';
import { parseByteSize } from '../src/resources.js';
import {
  remoteExecCommand,
  remoteProbeCommand,
  remoteResultCommand,
  remoteCancelCommand,
  remoteWithdrawCommand,
  remoteArtifactsCommand,
  remoteArtifactsReleaseCommand,
  defaultRemoteRoot,
} from '../src/remote-runner.js';
import { gcCommand } from '../src/gc.js';
import { loadGlobalConfig } from '../src/config.js';
import { capabilitiesCommand } from '../src/capabilities.js';
import { remotePipelineCommand } from '../src/remote-pipeline.js';

function usage() {
  return `Usage:
  lane run [--repo <name>] [--lane <name>] [--weight <n>] [--cpu <cores>] [--memory <size>] [--priority high|medium|low] [--exclusive] [--detach]
           [--timeout <duration>] [--allow-local-sim] [--local] [--log <path>] -- <command...>
  lane status [--json]
  lane capabilities --json
  lane suggest [--repo <name>] [--days <n>] [--json]
  lane estimates [--json] [--root <stateHome>]
  lane cancel <id>
  lane wait <id> [--timeout <duration>]
  lane pause ["reason"]
  lane resume
  lane migrate-scheduler [--dry-run | --when-idle [--timeout <duration>]]
  lane gc [--dry-run] [--max <n>] [--root <remoteRoot>]
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
        if (!Number.isFinite(n) || n <= 0) throw new Error(`--cpu must be a positive number (got "${raw}")`);
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
      case '--exclusive':
        opts.exclusive = true;
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
      case '--log':
        opts.log = flagArgs[++i];
        break;
      default:
        throw new Error(`lane run: unknown flag "${a}"`);
    }
  }
  return { opts, cmd };
}

/** BRAIN-319 T2 hidden remote-* subcommands: `--root <dir>` only. */
function parseRootFlag(args) {
  const idx = args.indexOf('--root');
  return idx === -1 ? undefined : args[idx + 1];
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
        exclusive: opts.exclusive,
        detach: opts.detach,
        timeoutMs: opts.timeout ? parseDurationMs(opts.timeout) : undefined,
        allowLocalSim: opts.allowLocalSim,
        local: opts.local,
        log: opts.log,
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
      const result = await remoteProbeCommand({ root: parseRootFlag(rest) || defaultRemoteRoot() });
      return result.exitCode;
    }
    case 'remote-exec': {
      const root = parseRootFlag(rest) || defaultRemoteRoot();
      const result = await remoteExecCommand({ root });
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

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((err) => {
    process.stderr.write(`lane: ${err.message}\n`);
    process.exitCode = 1;
  });
