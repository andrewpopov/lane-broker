-- PKG-437: merge-train's read contract for lane ticket status. A result that arrives after the ticket settled is kept, not applied; a finished ticket whose log never arrived is marked.
-- The view lane.ticket_status_v1, the lane_ticket_events notifications and the grants are in ../functions.sql.
-- train_reader is created here only when the applier can: lane_migrate on pitelite is NOCREATEROLE, and a failed CREATE ROLE would roll the columns back with it.
-- Otherwise a superuser creates the role once and the next apply of functions.sql grants it (docs/lane-dispatch-db-runbook.md).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'train_reader') THEN
    NULL;
  ELSIF (SELECT r.rolsuper OR r.rolcreaterole FROM pg_roles r WHERE r.rolname = session_user) THEN
    CREATE ROLE train_reader NOLOGIN;
  ELSE
    RAISE NOTICE 'train_reader not created: % cannot create roles; create it as a superuser and re-apply functions.sql', session_user;
  END IF;
END $$;

ALTER TABLE lane_dispatch.tickets
  ADD COLUMN late_result jsonb,
  ADD COLUMN late_result_at timestamptz,
  ADD COLUMN log_state text CHECK (log_state IS NULL OR log_state = 'missing'),
  ADD COLUMN status_changed_at timestamptz NOT NULL DEFAULT now();

-- status_changed_at is stamped at every transition; rows that exist now start at their latest known change.
UPDATE lane_dispatch.tickets SET status_changed_at = coalesce(finished_at, started_at, created_at);

-- The reaper's log-missing scan reads every 15 s; only a finished ticket with no logSha256 and no mark is in it, so the index stays small.
CREATE INDEX tickets_log_unmarked ON lane_dispatch.tickets (finished_at) WHERE state = 'finished' AND log_state IS NULL AND (result ->> 'logSha256') IS NULL;
