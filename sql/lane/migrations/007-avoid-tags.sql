-- BRAIN-1089: pull dispatch honours a lane's avoidTags softly. A runner whose tags overlap a ticket's avoid_tags skips it until it has waited avoid_grace_s.
ALTER TABLE lane_dispatch.tickets ADD COLUMN avoid_tags text[] NOT NULL DEFAULT '{}';
ALTER TABLE lane_dispatch.cluster ADD COLUMN avoid_grace_s int NOT NULL DEFAULT 120 CHECK (avoid_grace_s >= 0);
