-- BRAIN-1108: the live log stream (ld_log, ld_log_read, ld_log_ack, ld_prune_chunks) never carried a byte, and the pull client stopped calling it in 006's follow-up.
DROP TABLE lane_dispatch.log_chunks;
ALTER TABLE lane_dispatch.attempts DROP COLUMN log_bytes;
