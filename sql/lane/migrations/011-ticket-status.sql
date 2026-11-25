-- PKG-437: merge-train's read contract for lane ticket status. A result that arrives after the ticket settled is kept, not applied; a finished ticket whose log never arrived is marked.
-- The view lane.ticket_status_v1, the lane_ticket_events notifications and the grants are in ../functions.sql.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'train_reader') THEN
    CREATE ROLE train_reader NOLOGIN;
  END IF;
END $$;

ALTER TABLE lane_dispatch.tickets
  ADD COLUMN late_result jsonb,
  ADD COLUMN late_result_at timestamptz,
  ADD COLUMN log_state text CHECK (log_state IS NULL OR log_state = 'missing');

-- The reaper's log-missing scan reads every 15 s; only a finished ticket with no logSha256 and no mark is in it, so the index stays small.
CREATE INDEX tickets_log_unmarked ON lane_dispatch.tickets (finished_at) WHERE state = 'finished' AND log_state IS NULL AND (result ->> 'logSha256') IS NULL;
