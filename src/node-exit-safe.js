// BRAIN-577: Node 24 can deadlock in process.exit() when a concurrent Maglev/Sparkplug compile job is
// still running (nodejs/node#64274). Every lane-broker node process turns both compilers off.
import v8 from 'node:v8';

const SWITCHES = ['maglev', 'concurrent-sparkplug'];

export const NODE_EXIT_SAFE_FLAGS = SWITCHES.map((name) => `--no-${name}`);

const isSet = (arg, name) => arg === `--${name}` || arg === `--no-${name}`;

/** The exit-safe flags the operator has not already chosen a value for, in `argv`/`NODE_OPTIONS`. */
export function exitSafeFlagsFor(argv = process.execArgv, nodeOptions = process.env.NODE_OPTIONS ?? '') {
  const given = [...argv, ...nodeOptions.split(/\s+/)];
  return NODE_EXIT_SAFE_FLAGS.filter((flag, i) => !given.some((arg) => isSet(arg, SWITCHES[i])));
}

/** For a node child this process spawns: a child inherits NODE_OPTIONS but never our execArgv, so only an
 *  operator's NODE_OPTIONS choice counts (a parent started with --no-maglev must still pass it on). */
export const childExitSafeFlags = () => exitSafeFlagsFor([], process.env.NODE_OPTIONS ?? '');

/** In-process form, for entrypoints whose shebang cannot carry flags. Only affects compilations that
 *  start after this call (node's own bootstrap code is already compiled). */
export function disableConcurrentJit() {
  for (const flag of exitSafeFlagsFor()) v8.setFlagsFromString(flag);
}
