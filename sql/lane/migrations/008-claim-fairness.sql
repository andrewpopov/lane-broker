-- BRAIN-1106: pull dispatch reserves a host for the oldest ticket it could ever fit once that ticket has waited reserve_after_s (0 turns the reservation off).
ALTER TABLE lane_dispatch.cluster ADD COLUMN reserve_after_s int NOT NULL DEFAULT 900 CHECK (reserve_after_s >= 0);
