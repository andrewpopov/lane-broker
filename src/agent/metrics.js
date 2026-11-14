import path from 'node:path';
import { atomicWriteFile } from '../state.js';
import { SUBPHASES } from './store.js';

export const CLAIM_ANSWERS = ['claimed', 'none', 'upgrade', 'error'];
export const METRICS_FILE = 'lane-agent.prom';

const escapeLabel = (value) => String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
const labelled = (name, labels) => `${name}{${Object.entries(labels).map(([k, v]) => `${k}="${escapeLabel(v)}"`).join(',')}}`;

/** The agent's Prometheus text exposition for bosun's textfile collector: every subphase and claim answer is present, 0 when empty. */
export function renderAgentMetrics({ now, records, claims, listening, registered, scopeAvailable, version }) {
  const count = (subphase) => records.filter((r) => r.subphase === subphase).length;
  const families = [
    ['lane_agent_last_tick_timestamp_seconds', 'gauge', 'Unix time of the agent loop\'s last tick.', [['lane_agent_last_tick_timestamp_seconds', now / 1000]]],
    ['lane_agent_executions', 'gauge', 'Executions on this runner by subphase.', SUBPHASES.map((s) => [labelled('lane_agent_executions', { subphase: s }), count(s)])],
    ['lane_agent_claims_total', 'counter', 'ld_claim answers since this agent started.', CLAIM_ANSWERS.map((a) => [labelled('lane_agent_claims_total', { answer: a }), claims[a] ?? 0])],
    ['lane_agent_listen_up', 'gauge', '1 while the LISTEN connection is up.', [['lane_agent_listen_up', listening ? 1 : 0]]],
    ['lane_agent_registered', 'gauge', '1 once the queue has registered this runner.', [['lane_agent_registered', registered ? 1 : 0]]],
    ['lane_agent_scope_available', 'gauge', '1 while remote-exec supervisors can start in their own user scope.', [['lane_agent_scope_available', scopeAvailable ? 1 : 0]]],
    ['lane_agent_info', 'gauge', 'Always 1; the agent version is the label.', [[labelled('lane_agent_info', { version }), 1]]],
  ];
  return `${families.map(([name, type, help, samples]) => [`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`, ...samples.map(([series, value]) => `${series} ${value}`)].join('\n')).join('\n')}\n`;
}

export const writeAgentMetrics = (dir, snapshot) => atomicWriteFile(path.join(dir, METRICS_FILE), renderAgentMetrics(snapshot));
