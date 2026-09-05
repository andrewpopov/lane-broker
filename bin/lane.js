#!/usr/bin/env node
import { runCommand } from '../src/run.js';
import { statusCommand } from '../src/status.js';
import { cancelCommand } from '../src/cancel.js';
import { waitCommand } from '../src/wait.js';
import { pauseCommand, resumeCommand } from '../src/pause.js';

function usage() {
  return `Usage:
  lane run [--repo <name>] [--lane <name>] [--weight <n>] [--detach]
           [--timeout <duration>] [--allow-local-sim] [--log <path>] -- <command...>
  lane status [--json]
  lane cancel <id>
  lane wait <id> [--timeout <duration>]
  lane pause ["reason"]
  lane resume
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
      case '--weight':
        opts.weightOverride = Number(flagArgs[++i]);
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
      case '--log':
        opts.log = flagArgs[++i];
        break;
      default:
        throw new Error(`lane run: unknown flag "${a}"`);
    }
  }
  return { opts, cmd };
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
      const { opts, cmd } = parseRunArgs(rest);
      const result = await runCommand({
        repo: opts.repo,
        lane: opts.lane,
        weightOverride: opts.weightOverride,
        detach: opts.detach,
        timeoutMs: opts.timeout ? parseDurationMs(opts.timeout) : undefined,
        allowLocalSim: opts.allowLocalSim,
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
      const result = pauseCommand(reason);
      return result.exitCode;
    }
    case 'resume': {
      const result = resumeCommand();
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
