import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { ELASTIC_CLAIMS_CAPABILITY } from './resources.js';
import { PRIORITY_CAPABILITY } from './priority.js';
import { ARTIFACTS_CAPABILITY } from './remote-artifacts.js';
import { stateHome } from './state.js';
import { readSchedulerFence } from './fairness.js';
import { loadGlobalConfig } from './config.js';

export const SIM_SAFE_BACKFILL_CAPABILITY = 'sim-safe-backfill/1';
export const LANE_AGING_CAPABILITY = 'lane-aging/1';
export const GROUP_REAP_CAPABILITY = 'group-reap/1';
// BRAIN-436: `lane remote-withdraw`, the irreversible take-back of a ticket still queued on this runner.
export const REMOTE_WITHDRAW_CAPABILITY = 'remote-withdraw/1';

/** The ONE list: `lane remote-probe` advertises it and `lane capabilities --json` prints it. */
export const CAPABILITIES = [
  ELASTIC_CLAIMS_CAPABILITY,
  PRIORITY_CAPABILITY,
  ARTIFACTS_CAPABILITY,
  SIM_SAFE_BACKFILL_CAPABILITY,
  LANE_AGING_CAPABILITY,
  GROUP_REAP_CAPABILITY,
  REMOTE_WITHDRAW_CAPABILITY,
];

/** `lane capabilities --json`: what this install can do and which scheduler/admission mode it is in. */
export function capabilitiesCommand({ root = stateHome() } = {}) {
  const pkg = JSON.parse(fs.readFileSync(fileURLToPath(new URL('../package.json', import.meta.url)), 'utf8'));
  const payload = {
    version: pkg.version,
    capabilities: CAPABILITIES,
    schedulerMode: readSchedulerFence(root).status === 'valid' ? 'priority' : 'legacy',
    admissionMode: loadGlobalConfig().schedulerMode,
  };
  process.stdout.write(`${JSON.stringify(payload)}\n`);
  return { exitCode: 0 };
}
