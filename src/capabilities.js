import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { ELASTIC_CLAIMS_CAPABILITY } from './resources.js';
import { PRIORITY_CAPABILITY } from './priority.js';
import { ARTIFACTS_CAPABILITY } from './remote-artifacts.js';
import { SNAPSHOT_GZIP_CAPABILITY } from './remote-gzip.js';
import { BLOB_CACHE_CAPABILITY } from './remote-blobs.js';
import { EXCLUSIVE_CAPABILITY } from './exclusive.js';
import { LEASE_CHECK_CAPABILITY } from './lease.js';
import { stateHome } from './state.js';
import { readSchedulerFence } from './fairness.js';
import { loadGlobalConfig, DEFAULT_GLOBAL_CONFIG, isDispatchOnly } from './config.js';
import { CLASSES_CAPABILITY, resolveClasses } from './classes.js';
import { CPU_LIMIT_CAPABILITY, resolveCpuLimit } from './cpu-limit.js';

export const EXECUTION_CEILING_CAPABILITY = 'execution-cpu-ceiling/1';
/** BRAIN-649: this runner honours `remoteExclusive` on intake (holds the whole runner while the lane runs). */
export const REMOTE_EXCLUSIVE_CAPABILITY = 'remote-exclusive/1';
export const SIM_SAFE_BACKFILL_CAPABILITY = 'sim-safe-backfill/1';
export const LANE_AGING_CAPABILITY = 'lane-aging/1';
export const GROUP_REAP_CAPABILITY = 'group-reap/1';
// BRAIN-436: `lane remote-withdraw`, the irreversible take-back of a ticket still queued on this runner.
export const REMOTE_WITHDRAW_CAPABILITY = 'remote-withdraw/1';
// BRAIN-462: attempt records carry the submitter's logPath and cwd.
export const ATTEMPT_LOGPATH_CAPABILITY = 'attempt-logpath/1';
// BRAIN-520: `lane drain`/`undrain`; a probe may carry `remoteDrain`. Informational: selection keys off the probe field.
export const REMOTE_DRAIN_CAPABILITY = 'remote-drain/1';
// BRAIN-518: `lane priority` re-tiers a ticket still queued on this runner; a submitter checks this before sending one.
// BRAIN-634: advertised only while the global config sets `localExecution: "refused"`.
export const DISPATCH_ONLY_CAPABILITY = 'dispatch-only/1';
export const QUEUE_RETIER_CAPABILITY = 'queue-retier/1';

// BRAIN-597: `lane runner watch|status`, the runner heartbeat feeding runner-unreachable.json.
export const RUNNER_HEARTBEAT_CAPABILITY = 'runner-heartbeat/1';

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
  REMOTE_DRAIN_CAPABILITY,
  QUEUE_RETIER_CAPABILITY,
  LEASE_CHECK_CAPABILITY,
  RUNNER_HEARTBEAT_CAPABILITY,
  SNAPSHOT_GZIP_CAPABILITY,
  REMOTE_EXCLUSIVE_CAPABILITY,
];

/** `CAPABILITIES` plus what depends on this host's config: `blob-cache/1` only while the runner blob store is enabled, `cpu-limit/1` only when `cpuLimit` is not "off" and its probe passed. */
export function advertisedCapabilities(cfg, cpuLimit = { mode: 'off', available: false }) {
  return [
    ...CAPABILITIES,
    ...(cfg.remoteBlobCache ? [BLOB_CACHE_CAPABILITY] : []),
    ...(cpuLimit.mode !== 'off' && cpuLimit.available ? [CPU_LIMIT_CAPABILITY, EXECUTION_CEILING_CAPABILITY] : []),
  ];
}

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
  const cpuLimit = resolveCpuLimit(cfg);
  const payload = {
    version: pkg.version,
    capabilities: [...advertisedCapabilities(cfg, cpuLimit), ...(isDispatchOnly(cfg) ? [DISPATCH_ONLY_CAPABILITY] : [])],
    schedulerMode: readSchedulerFence(root).status === 'valid' ? 'priority' : 'legacy',
    admissionMode: cfg.schedulerMode,
    classes: { mode, configHash, stateRoot, valid },
    cpuLimit,
  };
  process.stdout.write(`${JSON.stringify(payload)}\n`);
  return { exitCode: 0 };
}
