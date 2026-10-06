import { readReplicationState } from './replication-state.js';

/**
 * Prometheus text exposition, served by the store itself so the age is live (a textfile written by the replicator would
 * freeze when it dies). It reads only the small watermark file and the journal's in-memory tail, never `journal.log`.
 */
export function renderMetrics(store, now = Date.now()) {
  const mark = readReplicationState(store.root);
  const oldest = store.journal.oldestPendingCreatedAt();
  const age = oldest === null ? 0 : Math.max(0, (now - oldest) / 1000);
  const lines = [
    ['oldest_unreplicated_object_age_seconds', 'Age of the first journal record above the replicated watermark; 0 when fully replicated.', age],
    ['lane_store_replicated_seq', 'Journal seq up to which every record is verified at the replica.', mark.replicatedSeq],
    ['lane_store_journal_head_seq', 'Highest committed journal seq.', store.journal.seq],
    ['lane_store_replication_failed_rounds', 'Consecutive failed replication rounds.', mark.failedRounds],
    ['lane_store_lost_objects', 'Journaled objects missing from disk with no delete intent (never tombstoned; operator recovery).', store.lost.size],
    ['lane_store_bytes', 'Bytes held in blobs/ and manifests/.', store.bytes],
  ];
  return `${lines.map(([name, help, v]) => `# HELP ${name} ${help}\n# TYPE ${name} gauge\n${name} ${v}`).join('\n')}\n`;
}
