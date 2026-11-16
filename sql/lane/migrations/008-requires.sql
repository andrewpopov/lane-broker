-- BRAIN-1135: pull dispatch honours a lane's `requires` tool list. ld_claim skips a ticket unless the agent names every tool in it as present on its own PATH.
ALTER TABLE lane_dispatch.tickets ADD COLUMN requires text[] NOT NULL DEFAULT '{}';
