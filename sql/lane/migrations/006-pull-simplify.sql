-- BRAIN-1035 slice 1 (plan rev 6): the pull queue loses quarantine, reassert and claim_lock, and gains the requeue cap, protocol and scheduling columns.
-- Kept until migration 007, after the pull client stops calling them: tickets.urls, attempts.log_bytes and log_chunks (ld_attach_urls, ld_log, ld_log_read, ld_log_ack).

UPDATE lane_dispatch.tickets SET state = 'lost', finished_at = coalesce(finished_at, now()),
       result = jsonb_build_object('kind', 'lost', 'reason', 'queue-restored', 'author', 'queue', 'epoch', epoch)
 WHERE state = 'quarantined';
ALTER TABLE lane_dispatch.tickets DROP CONSTRAINT tickets_state_check;
ALTER TABLE lane_dispatch.tickets ADD CONSTRAINT tickets_state_check CHECK (state IN ('queued','claimed','running','finished','cancelled','withdrawn','lost'));

ALTER TABLE lane_dispatch.tickets
  ADD COLUMN requeues int NOT NULL DEFAULT 0,
  ADD COLUMN not_before timestamptz NOT NULL DEFAULT '-infinity',
  ADD COLUMN min_protocol int NOT NULL DEFAULT 1,
  ADD COLUMN ages boolean NOT NULL DEFAULT true,
  ADD COLUMN source text,
  ADD COLUMN est_run_s real,
  ADD COLUMN reserved_host text,
  ADD COLUMN reserved_at timestamptz;
ALTER TABLE lane_dispatch.runners ADD COLUMN protocol int NOT NULL DEFAULT 1;

DROP TABLE lane_dispatch.claim_lock;
