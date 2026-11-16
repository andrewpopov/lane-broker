import { brokerHome, loadEffectiveGlobalConfig, ConfigError, LIVE_RELOAD_KEYS } from './config.js';

export function configCommand(args, { out = process.stdout, err = process.stderr } = {}) {
  const [sub, ...flags] = args;
  const unknown = flags.find((flag) => flag !== '--json');
  if (sub !== 'effective' || unknown !== undefined) {
    err.write(`lane config: usage: lane config effective [--json]${unknown !== undefined ? ` (unknown argument "${unknown}")` : ''}\n`);
    return { exitCode: 2 };
  }
  let effective;
  try {
    effective = loadEffectiveGlobalConfig();
  } catch (error) {
    if (!(error instanceof ConfigError)) throw error;
    err.write(`lane config effective: ${error.message}\n`);
    return { exitCode: 1 };
  }
  const { config, sources, files, warnings } = effective;
  const keys = Object.keys(config).sort();
  if (flags.includes('--json')) {
    out.write(`${JSON.stringify({ home: brokerHome(), files, warnings, config, sources, liveReload: LIVE_RELOAD_KEYS }, null, 2)}\n`);
    return { exitCode: 0 };
  }
  const lines = [...warnings.map((warning) => `WARNING: ${warning}`), `files: ${files.length ? files.join(', ') : 'none (defaults)'}`, ...keys.map((key) => `${key} = ${JSON.stringify(config[key])}  [${sources[key]}]`), `live reload (no restart): ${LIVE_RELOAD_KEYS.join(', ')}`];
  out.write(`${lines.join('\n')}\n`);
  return { exitCode: 0 };
}
