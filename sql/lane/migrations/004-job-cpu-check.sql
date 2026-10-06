-- BRAIN-400 round 3 (N7): a job's CPU is finite and positive and its minimum never exceeds its request, so a grant
-- least(cpu_req, rooms) can never fall below cpu_min.
ALTER TABLE lane.jobs ADD CONSTRAINT jobs_cpu_valid CHECK (cpu_req > 0 AND cpu_req < 'Infinity' AND cpu_min > 0 AND cpu_min <= cpu_req);
