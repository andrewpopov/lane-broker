import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { ELASTIC_CLAIMS_CAPABILITY } from './resources.js';
import { PRIORITY_CAPABILITY } from './priority.js';
import { ARTIFACTS_CAPABILITY } from './remote-artifacts.js';
import { EXCLUSIVE_CAPABILITY } from './exclusive.js';
import { stateHome } from './state.js';
import { readSchedulerFence } from './fairness.js';
import { loadGlobalConfig, DEFAULT_GLOBAL_CONFIG } from './config.js';
import { CLASSES_CAPABILITY, resolveClasses } from './classes.js';

export const SIM_SAFE_BACKFILL_CAPABILITY = 'sim-safe-backfill/1';
export const LANE_AGING_CAPABILITY = 'lane-aging/1';
export const GROUP_REAP_CAPABILITY = 'group-reap/1';
// BRAIN-436: `lane remote-withdraw`, the irreversible take-back of a ticket still queued on this runner.
export const REMOTE_WITHDRAW_CAPABILITY = 'remote-withdraw/1';
// BRAIN-462: attempt records carry the submitter's logPath and cwd.
export const ATTEMPT_LOGPATH_CAPABILITY = 'attempt-logpath/1';

/** The ONE list: `lane remote-probe` advertises it and `lane capabilities --json` prints it. */
export const CAPABILITIES = [
  ELASTIC_CLAIMS_CAPABILITY,
  PRIORITY_CAPABILITY,
  ARTIFACTS_CAPABILITY,
  SIM_SAFE_BACKFILL_CAPABILITY,
  LANE_AGING_CAPABILITY,
  GROUP_REAP_CAPABILITY,
  REMOTE_WITHDRAW_CAPABILITY,
  EXCLUSIVE_CAPABILITY,
  ATTEMPT_LOGPATH_CAPABILITY,
  CLASSES_CAPABILITY,
];

/** `lane capabilities --json`: what this install can do and which scheduler/admission mode it is in. */
export function capabilitiesCommand({ root = stateHome() } = {}) {
  const pkg = JSON.parse(fs.readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8'));
  // A config that cannot be read at all reports `classes.valid: false` rather than crashing the probe the rouge gate depends on.
  let cfg;
  try {
    cfg = loadGlobalConfig();
  } catch (err) {
    cfg = { ...DEFAULT_GLOBAL_CONFIG, classesInvalid: err.message };
  }
  const { mode, configHash, stateRoot, valid } = resolveClasses(cfg, root);
  const payload = {
    version: pkg.version,
    capabilities: CAPABILITIES,
    schedulerMode: readSchedulerFence(root).status === 'valid' ? 'priority' : 'legacy',
    admissionMode: cfg.schedulerMode,
    classes: { mode, configHash, stateRoot, valid },
  };
  process.stdout.write(`${JSON.stringify(payload)}\n`);
  return { exitCode: 0 };
}
