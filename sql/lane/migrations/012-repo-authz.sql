-- BRAIN-1116 S1: default-deny repo authorization for pull dispatch. The predicate, the setters, the guard trigger and the audit view are in ../functions.sql.
-- Every agent that exists now keeps claiming whatever its allowed_repos list says: it is backfilled 'open', so this migration changes nothing a runner sees.
-- A principal created afterwards starts at 'enforce'. open and audit ignore allowed_repos; enforce claims only the repos the list names.
ALTER TABLE lane_dispatch.principals
  ADD COLUMN repo_mode text NOT NULL DEFAULT 'enforce' CHECK (repo_mode IN ('open','audit','enforce'));
UPDATE lane_dispatch.principals SET repo_mode = 'open' WHERE kind = 'agent';

-- The audit view reads these states every minute: queued and held have indexes already (005), the other two states it reads get partial ones.
CREATE INDEX tickets_quarantined ON lane_dispatch.tickets (status_changed_at) WHERE state = 'quarantined';
CREATE INDEX tickets_terminal_recent ON lane_dispatch.tickets (finished_at) WHERE state IN ('finished','cancelled','withdrawn','lost');
