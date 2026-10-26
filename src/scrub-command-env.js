/**
 * TPWR-674: a client that needs its own credentials (an ssh agent for runner ssh) but runs untrusted commands lists the variable
 * names in LANE_BROKER_SCRUB_COMMAND_ENV. They travel on the ticket and are removed from the env of a command exec'd locally;
 * the client's and supervisor's own env keep them.
 */
export const SCRUB_COMMAND_ENV_VAR = 'LANE_BROKER_SCRUB_COMMAND_ENV';

const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** The valid names in a comma-separated list; anything else is dropped. */
export function parseScrubCommandEnv(raw) {
  if (typeof raw !== 'string') return [];
  return [...new Set(raw.split(',').map((name) => name.trim()).filter((name) => ENV_NAME.test(name)))];
}

/** `env` unchanged when `names` is empty, else a copy without those names and the control variable itself. */
export function scrubCommandEnv(env, names) {
  if (!Array.isArray(names) || names.length === 0) return env;
  const scrubbed = { ...env };
  for (const name of [...names, SCRUB_COMMAND_ENV_VAR]) delete scrubbed[name];
  return scrubbed;
}
