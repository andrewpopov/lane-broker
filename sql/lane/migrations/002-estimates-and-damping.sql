-- BRAIN-400 review F7/F8: what the remaining-work residual and the deadline damping need to remember.
-- jobs.max_ms: the wall-time kill limit from the template (spec 5.7 overrun cap); NULL means no cap is known.
ALTER TABLE lane.jobs ADD COLUMN max_ms int;
-- group_eta keeps the damped urgency and its hysteresis/dwell state between forecast cycles (spec 5.8).
ALTER TABLE lane.group_eta
  ADD COLUMN cycle bigint NOT NULL DEFAULT 0,
  ADD COLUMN urgency real,
  ADD COLUMN boost_on boolean NOT NULL DEFAULT false,
  ADD COLUMN last_flip_cycle bigint;
