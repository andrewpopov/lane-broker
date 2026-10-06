import { replicationLag } from './replicate.js';

/** Prometheus text exposition. Served by the store itself so the age is live (a textfile written by the replicator would freeze when it dies). */
export function renderMetrics(store, now = Date.now()) {
  const lag = replicationLag(store.root, now);
  const lines = [
    ['oldest_unreplicated_object_age_seconds', 'Age of the first journal entry above the replicated watermark; 0 when fully replicated.', lag.oldestUnreplicatedAgeSeconds],
    ['lane_store_replicated_seq', 'Journal seq up to which every object is hash-verified at the replica.', lag.replicatedSeq],
    ['lane_store_journal_head_seq', 'Highest committed journal seq.', lag.headSeq],
    ['lane_store_replication_failed_rounds', 'Consecutive failed replication rounds.', lag.failedRounds],
    ['lane_store_blob_bytes', 'Bytes held in blobs/.', store.bytes],
  ];
  return `${lines.map(([name, help, v]) => `# HELP ${name} ${help}\n# TYPE ${name} gauge\n${name} ${v}`).join('\n')}\n`;
}
