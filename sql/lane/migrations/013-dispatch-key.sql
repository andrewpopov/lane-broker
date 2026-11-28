-- PKG-438: an optional caller-supplied dispatch key makes a submission idempotent across a killed `lane run`. It is global (callers namespace it, e.g. train:<repo>:<attempt>:<shard>).
-- A withdrawn row frees its key; a terminal ticket keeps it, so a resubmit finds the ticket's result. No roles are touched, so a NOCREATEROLE migrator applies this.
ALTER TABLE lane_dispatch.tickets
  ADD COLUMN dispatch_key text CHECK (dispatch_key IS NULL OR dispatch_key ~ '^[A-Za-z0-9:._/-]{1,200}$');

CREATE UNIQUE INDEX tickets_dispatch_key ON lane_dispatch.tickets (dispatch_key) WHERE dispatch_key IS NOT NULL AND state <> 'withdrawn';
