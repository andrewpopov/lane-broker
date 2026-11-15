-- BRAIN-1099: an elastic pull ticket carries its floor (min_cpu_cores) and may be claimed at any integer grant from ceil(floor) to cpu_cores once it has waited
-- cluster.elastic_floor_after_s; claimed_cpu_cores is the grant of the current claim. cluster first, the order live calls lock in (see 007).
ALTER TABLE lane_dispatch.cluster ADD COLUMN elastic_floor_after_s int NOT NULL DEFAULT 600 CHECK (elastic_floor_after_s >= 0);
ALTER TABLE lane_dispatch.tickets ADD COLUMN min_cpu_cores real CHECK (min_cpu_cores IS NULL OR (min_cpu_cores > 0 AND min_cpu_cores <= cpu_cores)),
                                  ADD COLUMN claimed_cpu_cores real;
