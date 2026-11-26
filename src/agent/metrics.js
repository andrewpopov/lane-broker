import path from 'node:path';
import { atomicWriteFile } from '../state.js';
import { SUBPHASES } from './store.js';
import { BUDGET_FIGURES } from './budget.js';

export const CLAIM_ANSWERS = ['claimed', 'none', 'upgrade', 'error'];
export const METRICS_FILE = 'lane-agent.prom';

const escapeLabel = (value) => String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
const labelled = (name, labels) => `${name}{${Object.entries(labels).map(([k, v]) => `${k}="${escapeLabel(v)}"`).join(',')}}`;

const MEMORY_SIGNALS = [['vm_pressure_level', 'vmPressureLevel'], ['psi_some_avg10', 'psiSomeAvg10'], ['swap_mib_per_second', 'swapMiBPerSec']];

function sensorFamily(deviceGates) {
  const reporting = Object.entries(deviceGates).filter(([, state]) => state.sensor !== undefined);
  return reporting.length ? [['lane_agent_device_gate_sensor_unavailable', 'gauge', '1 while a gate\'s last sample read nothing it can judge by (sensor-unavailable).', reporting.map(([gate, state]) => [labelled('lane_agent_device_gate_sensor_unavailable', { gate }), state.sensor === 'unavailable' ? 1 : 0])]] : [];
}

function memoryPressureFamily(reading) {
  const known = MEMORY_SIGNALS.filter(([, key]) => Number.isFinite(reading?.[key]));
  if (known.length === 0) return [];
  return [['lane_agent_memory_pressure', 'gauge', 'The memory gate\'s last readings: macOS vm pressure level, Linux PSI some avg10, swap traffic in MiB/s.', known.map(([signal, key]) => [labelled('lane_agent_memory_pressure', { signal }), reading[key]])]];
}

/** The agent's Prometheus text exposition for a node_exporter-style textfile collector: every subphase and claim answer is present, 0 when empty. */
export function renderAgentMetrics({ now, records, claimed, claimAttempts, listening, registered, scopeAvailable, version, deviceGates = {}, budgetShadow }) {
  const count = (subphase) => records.filter((r) => r.subphase === subphase).length;
  const families = [
    ['lane_agent_last_tick_timestamp_seconds', 'gauge', 'Unix time of the agent loop\'s last tick.', [['lane_agent_last_tick_timestamp_seconds', now / 1000]]],
    ['lane_agent_executions', 'gauge', 'Executions on this runner by subphase.', SUBPHASES.map((s) => [labelled('lane_agent_executions', { subphase: s }), count(s)])],
    ['lane_agent_claims_total', 'counter', 'Executions this agent claimed since it started.', [['lane_agent_claims_total', claimed]]],
    ['lane_agent_claim_attempts_total', 'counter', 'ld_claim answers since this agent started, by answer.', CLAIM_ANSWERS.map((a) => [labelled('lane_agent_claim_attempts_total', { answer: a }), claimAttempts[a] ?? 0])],
    ['lane_agent_listen_up', 'gauge', '1 while the LISTEN connection is up.', [['lane_agent_listen_up', listening ? 1 : 0]]],
    ['lane_agent_registered', 'gauge', '1 once the queue has registered this runner.', [['lane_agent_registered', registered ? 1 : 0]]],
    ['lane_agent_scope_available', 'gauge', '1 while remote-exec supervisors can start in their own user scope.', [['lane_agent_scope_available', scopeAvailable ? 1 : 0]]],
    ...(Object.keys(deviceGates).length ? [['lane_agent_device_gate_closed', 'gauge', '1 while a device gate (thermal, power, memory, load) refuses new claims.', Object.entries(deviceGates).map(([gate, state]) => [labelled('lane_agent_device_gate_closed', { gate }), state.closed ? 1 : 0])]] : []),
    ...(budgetShadow ? [['lane_agent_budget_shadow', 'gauge', 'The budget this agent would run to: bosun\'s grant (source=bosun) or its own standalone figures (source=standalone). cpu in cores, mem in bytes. Shadow only: claims do not read it.', BUDGET_FIGURES.map((figure) => [labelled('lane_agent_budget_shadow', { figure, source: budgetShadow.source }), budgetShadow.figures[figure]])]] : []),
    ...memoryPressureFamily(deviceGates.memory?.reading),
    ...sensorFamily(deviceGates),
    ['lane_agent_info', 'gauge', 'Always 1; the agent version is the label.', [[labelled('lane_agent_info', { version }), 1]]],
  ];
  return `${families.map(([name, type, help, samples]) => [`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`, ...samples.map(([series, value]) => `${series} ${value}`)].join('\n')).join('\n')}\n`;
}

export const writeAgentMetrics = (dir, snapshot) => atomicWriteFile(path.join(dir, METRICS_FILE), renderAgentMetrics(snapshot));
