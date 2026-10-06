-- BRAIN-400: claim path (spec rev11 sections 5.1-5.5), re-applied after the table migrations. Lock order: (1) lane_sched, (3) hosts, (4) groups,
-- (5) jobs, then outbox rows. Candidates are chosen with plain reads; the winner is locked group-then-job and
-- re-validated; a failed re-validation returns no claim. Every function derives the caller from session_user.
-- Re-applied after every migration run (src/lane-db.js). A function whose signature changes needs a DROP of the old one here.
DROP FUNCTION IF EXISTS lane.refresh_rem_ref();

CREATE OR REPLACE FUNCTION lane.require_generation(p_gen bigint) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  IF p_gen IS DISTINCT FROM (SELECT generation FROM lane.cluster) THEN
    RAISE EXCEPTION 'STALE_GENERATION' USING ERRCODE = 'LN001';
  END IF;
END $$;

-- The calling principal, by session_user and kind. Internal: no role is granted execute.
CREATE OR REPLACE FUNCTION lane.principal_of(p_kind text) RETURNS lane.principals
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_me lane.principals;
BEGIN
  SELECT * INTO v_me FROM lane.principals WHERE login_role = session_user AND kind = p_kind;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no % principal for %', p_kind, session_user USING ERRCODE = '42501';
  END IF;
  RETURN v_me;
END $$;

CREATE OR REPLACE FUNCTION lane.dest_ok(p_owner text, p_host text) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT EXISTS (SELECT 1 FROM lane.principals p WHERE p.login_role = p_owner AND p_host = ANY (p.allowed_dest_hosts))
$$;

CREATE OR REPLACE FUNCTION lane.template_ok(p_id text, p_version int) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT EXISTS (SELECT 1 FROM lane.command_templates t WHERE t.template_id = p_id AND t.version = p_version AND NOT t.revoked)
$$;

-- The minimum stage over a group's unfinished jobs. A job is in an open stage iff its stage equals this: every earlier
-- stage is terminal. claim_next reads it once per group, never once per job. Each half reads one index (see 001-schema.sql).
CREATE OR REPLACE FUNCTION lane.open_stage(p_group uuid) RETURNS smallint
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT least((SELECT min(q.stage) FROM lane.jobs q WHERE q.group_id = p_group AND q.state = 'queued'),
               (SELECT min(f.stage) FROM lane.jobs f WHERE f.group_id = p_group AND f.state IN ('claimed','preparing','running','held')))
$$;

-- Job-level eligibility on one host, WITHOUT the size test (CPU/memory/floor/cap fit is applied after, so a group's
-- 9-core head can never hide its fitting 1-core job, spec 5.5). The single definition used to choose and to re-validate.
CREATE OR REPLACE FUNCTION lane.job_eligible(j lane.jobs, h lane.hosts, p_open_stage smallint, p_held_keys text[], p_now timestamptz)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT j.state = 'queued' AND j.not_before <= p_now AND j.stage = p_open_stage
     AND (j.host_pin IS NULL OR j.host_pin = h.host_id) AND j.host_tags_req <@ h.tags
     AND NOT (j.conflict_scope = 'host' AND j.conflict_keys && p_held_keys)
     AND NOT (j.conflict_scope = 'global' AND EXISTS (SELECT 1 FROM lane.exclusions x WHERE x.key = ANY (j.conflict_keys)))
     AND lane.template_ok(j.template_id, j.template_version)
     AND NOT EXISTS (SELECT 1 FROM lane.host_reservations r WHERE r.host_id = h.host_id AND r.owner_job_id <> j.id)
$$;

-- Re-validation under the group and job locks. VOLATILE on purpose: each inner statement takes a fresh snapshot.
-- Both rooms (floor and class cap) are carried in from selection and checked again here.
CREATE OR REPLACE FUNCTION lane.revalidate_claim(j lane.jobs, p_gid uuid, h lane.hosts, p_floor_room real, p_cap_room real, p_mem bigint, p_held_keys text[], p_now timestamptz)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE g lane.groups;
BEGIN
  SELECT * INTO g FROM lane.groups WHERE id = p_gid;
  RETURN FOUND AND j.group_id = g.id AND g.state = 'active' AND NOT g.cancel_requested
     AND lane.dest_ok(g.owner, h.host_id) AND lane.job_eligible(j, h, lane.open_stage(j.group_id), p_held_keys, p_now)
     AND j.cpu_min <= p_floor_room AND j.cpu_min <= p_cap_room AND j.mem_bytes <= p_mem;
END $$;

-- Scheduling parents (spec 5.3) --------------------------------------------------------------------------------------

-- Core-seconds of recent use, decayed with a 30 minute half-life to p_now.
CREATE OR REPLACE FUNCTION lane.decayed(p lane.sched_parents, p_now timestamptz) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT p.usage_decayed * power(0.5, greatest(0, extract(epoch FROM p_now - coalesce(p.usage_at, p_now))) / 1800)
$$;

-- U_p: cores-equivalent held recently (Slurm-style decayed usage).
CREATE OR REPLACE FUNCTION lane.usage_cores(p lane.sched_parents, p_now timestamptz) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT p.running_cpu + lane.decayed(p, p_now) / 1800
$$;

-- Change the cores a parent holds, first settling what it held until now into decayed usage: the integral of the decayed
-- contribution over the interval, rate x (H / ln 2) x (1 - 2^(-dt / H)) with H = 30 min, added after decaying the old total. The result
-- does not depend on how often it is settled.
CREATE OR REPLACE FUNCTION lane.add_running(p_parent uuid, p_delta real, p_now timestamptz) RETURNS void
LANGUAGE sql SET search_path = pg_catalog, lane, pg_temp AS $$
  UPDATE lane.sched_parents p
     SET usage_decayed = lane.decayed(p, p_now)
           + p.running_cpu::double precision * (1800 / ln(2::double precision))
             * (1 - power(0.5, greatest(0, extract(epoch FROM p_now - coalesce(p.usage_at, p_now))) / 1800)),
         usage_at = p_now, running_cpu = greatest(0, p.running_cpu + p_delta)
   WHERE p.id = p_parent
$$;

-- R_p: the parent's remaining work in slot-minutes on this host, each (work class, bucket) with its own factor (5.7).
CREATE OR REPLACE FUNCTION lane.rem_min(p lane.sched_parents, p_host text) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT coalesce(sum(b.value::double precision * coalesce(f.factor, 1)), 0) / 60
    FROM jsonb_each(coalesce(p.rem_ref, '{}')) wc
   CROSS JOIN LATERAL jsonb_each_text(wc.value) b
    LEFT JOIN lane.host_factors f ON f.host_id = p_host AND f.class = wc.key AND f.bucket = b.key
$$;

-- age_p + fair_p + wsjf_p, from PARENT quantities only (5.1). Group age, urgency and deadlines never enter here.
CREATE OR REPLACE FUNCTION lane.parent_score(p lane.sched_parents, p_host text, p_now timestamptz) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT least(100, 5 * greatest(0, extract(epoch FROM p_now - coalesce(p.last_claim_at, p.aging_anchor))) / 60)
       + 40 / (1 + lane.usage_cores(p, p_now) / 4)
       + CASE WHEN cfg.wsjf_enabled THEN least(60, 6 * pc.cod_per_min / greatest(lane.rem_min(p, p_host), 0.5)) ELSE 0 END
    FROM lane.config cfg, lane.priority_classes pc WHERE pc.name = p.prio_class
$$;

CREATE OR REPLACE FUNCTION lane.interactive_limit() RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT coalesce((SELECT interactive_max_cores FROM lane.config),
                  0.5 * (SELECT sum(budget) FROM lane.hosts WHERE can_compute), 'Infinity'::double precision)
$$;

-- The effective rank (5.3): the class rank, except an interactive parent over the limit is ranked as gate. The state is
-- rewritten by refresh_demotions under lane_sched before every selection, so claim_next and the ETA simulator read the same value.
CREATE OR REPLACE FUNCTION lane.eff_rank(g lane.groups, p lane.sched_parents) RETURNS smallint
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT CASE WHEN g.prio_class = 'interactive' AND p.demoted THEN (SELECT rank FROM lane.priority_classes WHERE name = 'gate') ELSE pc.rank END
    FROM lane.priority_classes pc WHERE pc.name = g.prio_class
$$;

-- Hysteresis: demoted above the limit, restored below 0.8 x the limit.
CREATE OR REPLACE FUNCTION lane.refresh_demotions(p_now timestamptz) RETURNS void
LANGUAGE sql SET search_path = pg_catalog, lane, pg_temp AS $$
  UPDATE lane.sched_parents p SET demoted = nx.v
    FROM (SELECT q.id, (u.cores > lim.v OR (q.demoted AND u.cores >= 0.8 * lim.v)) AS v
            FROM lane.sched_parents q, (SELECT lane.interactive_limit() AS v) lim,
                 LATERAL (SELECT lane.usage_cores(q, p_now) AS cores) u
           WHERE q.prio_class = 'interactive') nx
   WHERE p.id = nx.id AND p.demoted IS DISTINCT FROM nx.v
$$;

-- Deadlines (5.8). The scheduler never reads deadline_at, only these two, by the runtime precedence: invalid -> nothing;
-- missed -> urgency 75 and no EDF; valid -> urgency from slack against the frozen ETA (x 0.5 when low-confidence), EDF on.
-- A group with no forecast yet is treated as having nothing left to run, and its urgency is undamped.
CREATE OR REPLACE FUNCTION lane.deadline_eff(g lane.groups, p_now timestamptz) RETURNS timestamptz
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT CASE WHEN cfg.deadlines_enabled AND g.deadline_valid AND g.deadline_at >= p_now THEN g.deadline_at END FROM lane.config cfg
$$;

CREATE OR REPLACE FUNCTION lane.urgency(g lane.groups, p_now timestamptz) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT CASE
           WHEN NOT cfg.deadlines_enabled OR g.deadline_at IS NULL OR NOT g.deadline_valid THEN 0
           WHEN g.deadline_at < p_now THEN 75
           WHEN e.group_id IS NOT NULL THEN coalesce(e.urgency, 0)         -- damped by record_eta (hysteresis, dwell, slew)
           ELSE (CASE WHEN coalesce(e.low_confidence, false) THEN 0.5 ELSE 1 END)
                * 150 * least(1, greatest(0, 1 - x.slack / x.s0))
         END
    FROM lane.config cfg
    LEFT JOIN lane.group_eta e ON e.group_id = g.id
   CROSS JOIN LATERAL (SELECT greatest(900, 0.5 * coalesce(e.eta_p90_s, 0)) AS s0,
                              extract(epoch FROM g.deadline_at - p_now) - coalesce(e.eta_p90_s, 0) AS slack) x
$$;

-- One forecast cycle for a group (the ETA timer's write, spec 5.8): stores the frozen forecast and the DAMPED urgency that
-- lane.urgency reads. Hysteresis (on at slack < S0, off only at slack > 1.25 S0), a dwell of one on/off change per 3 cycles
-- (so 7 changes in any 20 cycles), and a slew limit of 25 points per cycle. No scheduling lock: this is the outbox tier.
CREATE OR REPLACE FUNCTION lane.record_eta(p_group uuid, p_eta_p90_s real, p_low_confidence boolean, p_now timestamptz) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE
  g lane.groups; e lane.group_eta; v_s0 double precision; v_slack double precision; v_raw double precision;
  v_on boolean; v_cycle bigint; v_prev double precision;
BEGIN
  SELECT * INTO g FROM lane.groups WHERE id = p_group;
  IF NOT FOUND OR g.deadline_at IS NULL THEN RETURN; END IF;
  SELECT * INTO e FROM lane.group_eta WHERE group_id = p_group;
  v_cycle := coalesce(e.cycle, 0) + 1;
  v_prev := coalesce(e.urgency, 0);
  v_s0 := greatest(900, 0.5 * p_eta_p90_s);
  v_slack := extract(epoch FROM g.deadline_at - p_now) - p_eta_p90_s;
  v_raw := (CASE WHEN p_low_confidence THEN 0.5 ELSE 1 END) * 150 * least(1, greatest(0, 1 - v_slack / v_s0));
  v_on := CASE WHEN coalesce(e.boost_on, false) THEN NOT (v_slack > 1.25 * v_s0) ELSE v_slack < v_s0 END;
  IF v_on <> coalesce(e.boost_on, false) AND e.last_flip_cycle IS NOT NULL AND v_cycle - e.last_flip_cycle < 3 THEN
    v_on := e.boost_on;                                                         -- dwell: too soon to change state again
  END IF;
  INSERT INTO lane.group_eta (group_id, eta_p90_s, low_confidence, cycle, urgency, boost_on, last_flip_cycle)
    VALUES (p_group, p_eta_p90_s, p_low_confidence, v_cycle,
            v_prev + least(25, greatest(-25, CASE WHEN v_on THEN v_raw ELSE 0 END - v_prev)),
            v_on, CASE WHEN v_on <> coalesce(e.boost_on, false) THEN v_cycle ELSE e.last_flip_cycle END)
    ON CONFLICT (group_id) DO UPDATE
      SET eta_p90_s = EXCLUDED.eta_p90_s, low_confidence = EXCLUDED.low_confidence, cycle = EXCLUDED.cycle,
          urgency = EXCLUDED.urgency, boost_on = EXCLUDED.boost_on, last_flip_cycle = EXCLUDED.last_flip_cycle;
END $$;

-- Floors (5.2). The CPU reserved against a candidate of work class p_class and rank p_rank: each OTHER work class w with waiting
-- work of rank rho_w >= p_rank reserves its unmet floor; the candidate's OWN class reserves only against lower rank.
-- p_rho is {"<work class>": highest effective rank among its eligible waiting candidates}, computed before any room is applied.
CREATE OR REPLACE FUNCTION lane.reserved_floor(p_host text, p_budget real, p_used jsonb, p_rho jsonb, p_class text, p_rank int, p_closed text[])
RETURNS real LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT coalesce(sum(greatest(0, coalesce(c.floor_cpu_pct, 0) / 100 * p_budget - coalesce((p_used ->> c.class)::real, 0))), 0)::real
    FROM lane.host_class_policy c
   WHERE c.host_id = p_host AND c.enabled AND c.weight > 0 AND NOT (c.class = ANY (p_closed)) AND (p_rho ->> c.class) IS NOT NULL
     AND ((c.class <> p_class AND (p_rho ->> c.class)::int >= p_rank) OR (c.class = p_class AND p_rank < (p_rho ->> c.class)::int))
$$;

-- The CPU left to a job after floors. Overdue candidates are exempt from every floor, and an exclusive job is never overdue (A1).
CREATE OR REPLACE FUNCTION lane.job_floor_room(p_parent_overdue boolean, p_exclusive boolean, p_free real, p_room_after_floors real) RETURNS real
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT CASE WHEN p_parent_overdue AND NOT p_exclusive THEN p_free ELSE p_room_after_floors END
$$;

-- Host leases: jobs handed to this host and not yet finished.
CREATE OR REPLACE FUNCTION lane.host_leases(p_host text) RETURNS bigint
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT count(*) FROM lane.jobs WHERE host = p_host AND state IN ('claimed','preparing','running')
$$;

-- An exclusive job was picked (5.9): with no running leases the `active` fence is inserted and the claim proceeds; otherwise a
-- `draining` fence is inserted and there is no claim (in-flight jobs finish, none are killed). Returns whether to claim now.
CREATE OR REPLACE FUNCTION lane.reservation_activate_or_drain(h lane.hosts, j lane.jobs, p_now timestamptz) RETURNS boolean
LANGUAGE plpgsql SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_idle boolean := lane.host_leases(h.host_id) = 0;
BEGIN
  INSERT INTO lane.host_reservations (host_id, owner_job_id, state, window_id, created_at, drain_deadline, active_deadline)
    VALUES (h.host_id, j.id, CASE WHEN v_idle THEN 'active' ELSE 'draining' END, 'job:' || j.id, p_now,
            p_now + interval '2 hours', p_now + interval '3 hours')
    ON CONFLICT (host_id) DO NOTHING;
  RETURN v_idle;
END $$;

-- p_free: CPU free before floors (budget - used - external busy); p_used: CPU used per work class; p_closed: work classes closed by
-- LOCAL gates (idle gate, class cap exhausted). Floors and the class cap are applied HERE, per candidate (5.2).
CREATE OR REPLACE FUNCTION lane.claim_next(p_gen bigint, p_free real, p_used jsonb, p_closed text[], p_mem bigint, p_held_keys text[], p_token uuid)
RETURNS SETOF lane.claim_result LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE
  v_me lane.principals; v_host lane.hosts; v_pick record; v_job lane.jobs; v_now timestamptz;
  v_grant real; v_seq bigint; v_res lane.host_reservations;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1) first
  v_now := clock_timestamp();                                                  -- the ONE clock, read after the lock
  v_me := lane.principal_of('agent');
  PERFORM lane.require_generation(p_gen);
  p_used := coalesce(p_used, '{}'); p_closed := coalesce(p_closed, '{}');
  SELECT * INTO v_host FROM lane.hosts WHERE host_id = v_me.host_id FOR UPDATE;   -- (3)
  IF NOT FOUND OR NOT v_host.can_compute OR v_host.state <> 'active'
     OR coalesce(v_host.breaker_until, '-infinity') > v_now
     OR NOT (SELECT claims_enabled FROM lane.cluster) THEN RETURN; END IF;
  SELECT * INTO v_res FROM lane.host_reservations WHERE host_id = v_host.host_id;   -- (3): the host fence
  IF FOUND AND v_res.state = 'draining' THEN
    IF lane.host_leases(v_host.host_id) > 0 THEN RETURN; END IF;                -- a draining host claims nothing
    UPDATE lane.host_reservations SET state = 'active' WHERE host_id = v_host.host_id;
  END IF;
  PERFORM lane.refresh_demotions(v_now);                                       -- the effective rank is settled BEFORE selection

  WITH pol AS (
    SELECT c.class, c.weight, c.cap_cpu_pct, coalesce(v.vtime, 0) AS vtime, coalesce(v.active, false) AS was_active
      FROM lane.host_class_policy c
      LEFT JOIN lane.class_vtime v ON v.host_id = c.host_id AND v.class = c.class
     WHERE c.host_id = v_host.host_id AND c.enabled AND c.weight > 0 AND NOT (c.class = ANY (p_closed))),
  par AS (                        -- parent quantities, once per parent: the only things the cross-parent comparator sees
    SELECT p.id AS pid, coalesce(p.last_claim_at, p.aging_anchor) AS since_p,
           (v_now - coalesce(p.last_claim_at, p.aging_anchor) >= interval '20 minutes') AS p_overdue,
           lane.parent_score(p, v_host.host_id, v_now) AS score_base
      FROM lane.sched_parents p WHERE p.aging_anchor IS NOT NULL),
  grp AS MATERIALIZED (           -- per group, once per claim: its open stage and its sibling-order keys. No locks
    SELECT g.id AS gid, g.parent_id AS pid, g.last_claim_at AS g_last, lane.open_stage(g.id) AS open_stage,
           lane.eff_rank(g, p)::int AS band,
           coalesce(g.last_claim_at, g.aging_anchor) AS g_since,
           (v_now - coalesce(g.last_claim_at, g.aging_anchor) >= interval '20 minutes') AS g_overdue,
           lane.urgency(g, v_now) AS urg_g, lane.deadline_eff(g, v_now) AS dl,
           par.since_p, par.p_overdue, par.score_base
      FROM lane.groups g
      JOIN lane.sched_parents p ON p.id = g.parent_id
      JOIN par ON par.pid = p.id
     WHERE g.state = 'active' AND NOT g.cancel_requested AND lane.dest_ok(g.owner, v_host.host_id)),
  elig AS (                       -- one row per (group, work class) with >= 1 ELIGIBLE queued job; NO size test
    SELECT grp.*, pol.class, pol.weight, pol.vtime, pol.was_active
      FROM pol CROSS JOIN grp
      JOIN LATERAL (SELECT 1 FROM lane.jobs j                   -- LIMIT keeps this a per-row index probe, never a flattened semi-join
                     WHERE j.group_id = grp.gid AND j.stage = grp.open_stage AND j.class = pol.class AND j.state = 'queued'
                       AND lane.job_eligible(j, v_host, grp.open_stage, p_held_keys, v_now)
                     LIMIT 1) h ON true),
  rho AS (                        -- reservation DEMAND: the rank of each work class's best waiting work, of any size
    SELECT coalesce(jsonb_object_agg(w.class, w.rk), '{}') AS rho
      FROM (SELECT class, max(band) AS rk FROM elig GROUP BY class) w),
  cand AS (                       -- room per row: floors and, separately, the class cap. Overdue exemption is applied per JOB, below
    SELECT e.*,
           (p_free - lane.reserved_floor(v_host.host_id, v_host.budget, p_used, r.rho, e.class, e.band, p_closed))::real AS floor_room_held,
           coalesce((SELECT pw.cap_cpu_pct / 100 * v_host.budget - coalesce((p_used ->> pw.class)::real, 0) FROM pol pw WHERE pw.class = e.class),
                    'Infinity'::real)::real AS cap_room
      FROM elig e CROSS JOIN rho r),
  fit AS (                        -- the longest FITTING eligible job per row, chosen AFTER the CPU/memory/floor/cap filter
    SELECT c.*, j.id AS job_id, j.est_p50_s, j.seq, j.exclusive,
           (c.p_overdue AND NOT j.exclusive) AS overdue,                       -- A1: an exclusive job is never promoted by the overdue rule
           j.floor_room, c.score_base + max(c.urg_g) OVER (PARTITION BY c.pid) AS score   -- urg_p = max over the parent's fitting groups
      FROM cand c
      JOIN LATERAL (SELECT x.*                   -- one probe per kind (ordinary, exclusive), then the choice: A1 must never hide an overdue ordinary job
                      FROM unnest(ARRAY[false, true]) AS ex(v)
                     CROSS JOIN LATERAL (SELECT j.id, j.est_p50_s, j.seq, j.exclusive,
                                                lane.job_floor_room(c.p_overdue, j.exclusive, p_free, c.floor_room_held) AS floor_room
                                           FROM lane.jobs j
                                          WHERE j.group_id = c.gid AND j.stage = c.open_stage AND j.class = c.class AND j.state = 'queued'
                                            AND j.exclusive = ex.v
                                            AND lane.job_eligible(j, v_host, c.open_stage, p_held_keys, v_now)
                                            AND j.cpu_min <= least(lane.job_floor_room(c.p_overdue, j.exclusive, p_free, c.floor_room_held), c.cap_room)
                                            AND j.mem_bytes <= p_mem
                                          ORDER BY j.est_p50_s DESC, j.seq LIMIT 1) x
                     ORDER BY (c.p_overdue AND x.exclusive), x.est_p50_s DESC, x.seq LIMIT 1) j ON true),
  rep AS (                        -- ONE representative group per (parent, work class): sibling order only (5.3)
    SELECT DISTINCT ON (pid, class) * FROM fit
     ORDER BY pid, class, (p_overdue AND exclusive), g_overdue DESC, CASE WHEN g_overdue THEN g_since END ASC, urg_g DESC, g_since ASC,
              dl NULLS LAST, g_last NULLS FIRST, est_p50_s DESC, seq, gid),
  floored AS (
    SELECT rep.*, min(vtime) FILTER (WHERE was_active) OVER () AS active_floor, array_agg(class) OVER () AS present
      FROM rep),
  stride AS (                     -- 5.2 catch-up: a class returning from idle starts at the minimum of the active classes
    SELECT floored.*, CASE WHEN was_active THEN vtime ELSE greatest(vtime, coalesce(active_floor, vtime)) END AS eff_vtime
      FROM floored)
  SELECT * INTO v_pick FROM stride
   ORDER BY overdue DESC,
            CASE WHEN overdue THEN since_p END ASC, CASE WHEN overdue THEN pid END ASC,   -- 1. overdue parents: total order (since_p, pid)
            band DESC,                                  -- 2. STRICT effective rank: nothing but overdue crosses it
            round(eff_vtime::numeric, 9) ASC,           -- 3. work-class stride within that rank (rounded: float noise must not break true ties)
            score DESC,                                 -- 4. PARENT age + fair share + WSJF + the parent's best urgency
            dl NULLS LAST,                              -- 5. EDF, only on a validated enabled deadline
            g_last NULLS FIRST,                         -- 6. group rotation
            est_p50_s DESC, seq,                        -- 7. longest first, then arrival
            gid                                         -- 8. deterministic fallback LAST
   LIMIT 1;
  IF NOT FOUND THEN RETURN; END IF;

  PERFORM 1 FROM lane.groups WHERE id = v_pick.gid FOR UPDATE;                 -- (4)
  SELECT * INTO v_job FROM lane.jobs WHERE id = v_pick.job_id FOR UPDATE;      -- (5)
  IF NOT FOUND OR NOT lane.revalidate_claim(v_job, v_pick.gid, v_host, v_pick.floor_room, v_pick.cap_room, p_mem, p_held_keys, v_now) THEN
    RETURN;                                                                    -- no loop, no re-lock
  END IF;

  IF v_job.exclusive AND NOT lane.reservation_activate_or_drain(v_host, v_job, v_now) THEN RETURN; END IF;   -- 5.9: may return with no claim
  v_grant := least(v_job.cpu_req, v_pick.floor_room, v_pick.cap_room);         -- never above either room
  UPDATE lane.jobs SET state = 'claimed', epoch = epoch + 1, claim_token = p_token, host = v_host.host_id,
         lease_until = v_now + interval '30 seconds', grant_cpu = v_grant
   WHERE id = v_job.id RETURNING * INTO v_job;
  IF v_job.conflict_scope = 'global' THEN
    INSERT INTO lane.exclusions (key, job_id, epoch, host)
      SELECT DISTINCT k, v_job.id, v_job.epoch, v_host.host_id FROM unnest(v_job.conflict_keys) AS k;
  END IF;
  UPDATE lane.groups SET last_claim_at = v_now WHERE id = v_pick.gid;
  -- A parent's turn is claimed once, whichever sibling was served: it takes the service stamp and the running cores.
  UPDATE lane.sched_parents SET last_claim_at = v_now WHERE id = v_pick.pid;
  PERFORM lane.add_running(v_pick.pid, v_grant, v_now);
  -- Activity bookkeeping for the next claim: classes with eligible work now are active, the rest are idle. A class
  -- that was idle is raised to the active floor before this grant is charged to it. Same lock as the claim, so no race.
  INSERT INTO lane.class_vtime AS v (host_id, class, vtime, active)
    SELECT v_host.host_id, c, coalesce(v_pick.active_floor, 0), true FROM unnest(v_pick.present) AS c GROUP BY c
    ON CONFLICT (host_id, class) DO UPDATE
      SET vtime = CASE WHEN v.active THEN v.vtime ELSE greatest(v.vtime, coalesce(v_pick.active_floor, v.vtime)) END, active = true;
  UPDATE lane.class_vtime SET active = false WHERE host_id = v_host.host_id AND active AND class <> ALL (v_pick.present);
  IF NOT v_pick.overdue THEN               -- overdue claims are excluded from share accounting (5.2)
    INSERT INTO lane.class_vtime (host_id, class, vtime)
      VALUES (v_host.host_id, v_job.class, v_grant::double precision / v_pick.weight)
      ON CONFLICT (host_id, class) DO UPDATE SET vtime = lane.class_vtime.vtime + EXCLUDED.vtime;
  END IF;
  INSERT INTO lane.job_events (job_id, epoch, kind, host, at) VALUES (v_job.id, v_job.epoch, 'claimed', v_host.host_id, v_now);
  SELECT coalesce(max(seq), 0) + 1 INTO v_seq FROM lane.transition_events WHERE job_id = v_job.id;
  INSERT INTO lane.transition_events (job_id, epoch, seq, kind, created_at)       -- outbox insert LAST (5.5 a)
    VALUES (v_job.id, v_job.epoch, v_seq, 'claimed', v_now);
  RETURN QUERY SELECT v_job.id, v_job.group_id, v_job.epoch, v_job.claim_token, v_job.lease_until,
                      v_job.grant_cpu, v_job.class, v_job.template_id, v_job.template_version, v_job.params;
END $$;

-- Submission -------------------------------------------------------------------------------------------------------

-- May this principal submit into the class? max_class bounds the rank; a class that names a capability needs that flag.
CREATE OR REPLACE FUNCTION lane.class_allowed(me lane.principals, p_class text) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT EXISTS (SELECT 1 FROM lane.priority_classes pc, lane.priority_classes cap
                  WHERE pc.name = p_class AND cap.name = me.max_class AND pc.rank <= cap.rank
                    AND (pc.requires_capability IS NULL OR coalesce((to_jsonb(me) ->> pc.requires_capability)::boolean, false)))
$$;

-- The scheduling parent for (account, class): derived here, never client-chosen. Creating one takes lane_sched FIRST: an
-- uncommitted parent row blocks any scheduler-locked function that wants the same (account, class), and that function would then
-- wait inside lane_sched for a submitter who is itself about to ask for it (a deadlock). Only the first-ever submit for a pair
-- pays this; every later one finds the row and takes no lock.
CREATE OR REPLACE FUNCTION lane.parent_for(p_account text, p_class text) RETURNS uuid
LANGUAGE plpgsql SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_pid uuid;
BEGIN
  SELECT id INTO v_pid FROM lane.sched_parents WHERE account = p_account AND prio_class = p_class;
  IF v_pid IS NULL THEN
    PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1)
    INSERT INTO lane.sched_parents (account, prio_class) VALUES (p_account, p_class) ON CONFLICT (account, prio_class) DO NOTHING;
    SELECT id INTO v_pid FROM lane.sched_parents WHERE account = p_account AND prio_class = p_class;
  END IF;
  RETURN v_pid;
END $$;

-- Deadline validity, judged once (submit, reclass): enabled, the principal may, and not earlier than now + the longest
-- job's p50 at the fastest factor. Remaining work over slots is not modelled yet.
CREATE OR REPLACE FUNCTION lane.judge_deadline(p_group uuid, p_deadline timestamptz, p_can_deadline boolean, p_now timestamptz) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT p_deadline IS NOT NULL AND p_can_deadline AND (SELECT deadlines_enabled FROM lane.config)
     AND p_deadline >= p_now + make_interval(secs => coalesce((SELECT max(j.est_p50_s) FROM lane.jobs j WHERE j.group_id = p_group), 0)
                                                    * coalesce((SELECT min(factor) FROM lane.host_factors), 1))
$$;

-- The server's estimate for a job with no usable history (spec 5.7 cold start: 300 s test, 900 s sim). The seam for the real
-- estimator (BRAIN-408): a submitter's est_* values are only hints, never trusted below this.
CREATE OR REPLACE FUNCTION lane.class_default_est(p_class text) RETURNS real
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT CASE p_class WHEN 'sim' THEN 900 ELSE 300 END::real
$$;

-- p_jobs: JSON array of {idem_key, stage, class, template_id, template_version, params, est_p50_s, est_p90_s (hints), exclusive, max_ms, cpu_req,
-- cpu_min, mem_bytes, conflict_keys, conflict_scope, dup_safe, host_pin, host_tags_req, max_infra, max_work}.
-- No scheduling lock: the group is inserted `open` and only activate_group makes it visible.
CREATE OR REPLACE FUNCTION lane.submit_group(p_submit_uuid uuid, p_kind text, p_account text, p_class text,
                                             p_snapshot jsonb, p_aggregator text, p_jobs jsonb, p_deadline timestamptz DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_me lane.principals; v_gid uuid;
BEGIN
  v_me := lane.principal_of('submit');
  IF NOT (p_account = ANY (v_me.allowed_accounts)) THEN
    RAISE EXCEPTION 'account % not allowed for %', p_account, session_user USING ERRCODE = '42501';
  END IF;
  IF NOT lane.class_allowed(v_me, p_class) THEN
    RAISE EXCEPTION 'class % not allowed for %', p_class, session_user USING ERRCODE = '42501';
  END IF;
  INSERT INTO lane.groups (id, submit_uuid, kind, account, owner, parent_id, prio_class, snapshot, aggregator, deadline_at)
    VALUES (gen_random_uuid(), p_submit_uuid, p_kind, p_account, session_user, lane.parent_for(p_account, p_class), p_class,
            p_snapshot, p_aggregator, p_deadline)
    ON CONFLICT (submit_uuid) DO NOTHING RETURNING id INTO v_gid;
  IF v_gid IS NULL THEN
    SELECT id INTO v_gid FROM lane.groups WHERE submit_uuid = p_submit_uuid AND owner = session_user;
    IF v_gid IS NULL THEN RAISE EXCEPTION 'submit_uuid belongs to another principal' USING ERRCODE = '42501'; END IF;
    RETURN v_gid;
  END IF;
  INSERT INTO lane.jobs (group_id, seq, idem_key, stage, class, template_id, template_version, params, est_p50_s, est_p90_s, est_source,
                         exclusive, cpu_req, cpu_min, mem_bytes, conflict_keys, conflict_scope, dup_safe, host_pin,
                         host_tags_req, max_infra, max_work, max_ms)
    SELECT v_gid, r.ord, r.idem_key, coalesce(r.stage, 0), r.class, r.template_id, r.template_version,
           coalesce(r.params, '{}'), e.p50, greatest(coalesce(r.est_p90_s, 0), 2 * e.p50),
           CASE WHEN coalesce(r.est_p50_s, 0) >= d.v THEN 'hint' ELSE 'default' END,
           coalesce(r.exclusive, false),
           r.cpu_req, coalesce(r.cpu_min, r.cpu_req), coalesce(r.mem_bytes, 0),
           coalesce(r.conflict_keys, '{}'), coalesce(r.conflict_scope, 'host'), coalesce(r.dup_safe, false), r.host_pin,
           coalesce(r.host_tags_req, '{}'), coalesce(r.max_infra, 3), coalesce(r.max_work, 2), r.max_ms
      FROM (SELECT x.*, row_number() OVER () AS ord
              FROM jsonb_to_recordset(p_jobs) AS x(idem_key text, stage smallint, class text, template_id text,
                   template_version int, params jsonb, est_p50_s real, est_p90_s real, exclusive boolean, cpu_req real, cpu_min real, mem_bytes bigint,
                   conflict_keys text[], conflict_scope text, dup_safe boolean, host_pin text, host_tags_req text[],
                   max_infra smallint, max_work smallint, max_ms int)) r
      CROSS JOIN LATERAL (SELECT lane.class_default_est(r.class) AS v) d
      CROSS JOIN LATERAL (SELECT greatest(coalesce(r.est_p50_s, 0), d.v) AS p50) e;
  UPDATE lane.groups SET deadline_valid = lane.judge_deadline(v_gid, p_deadline, v_me.can_deadline, clock_timestamp()) WHERE id = v_gid;
  RETURN v_gid;
END $$;

-- The first active group of a parent starts the parent's service clock; later siblings join it (spec 5.3).
CREATE OR REPLACE FUNCTION lane.start_parent_clock(p_parent uuid, p_group uuid, p_now timestamptz) RETURNS void
LANGUAGE sql SET search_path = pg_catalog, lane, pg_temp AS $$
  UPDATE lane.sched_parents SET aging_anchor = p_now, last_claim_at = NULL
   WHERE id = p_parent AND NOT EXISTS (SELECT 1 FROM lane.groups o WHERE o.parent_id = p_parent AND o.state = 'active' AND o.id <> p_group)
$$;

-- Serialized activation: the aging origins are the post-lock clock, so no earlier-committing transaction
-- can ever sort ahead of a group (or a parent) that is already active.
CREATE OR REPLACE FUNCTION lane.activate_group(p_group uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_now timestamptz; v_owner text; v_pid uuid;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1)
  v_now := clock_timestamp();
  SELECT owner, parent_id INTO v_owner, v_pid FROM lane.groups WHERE id = p_group;
  IF v_owner IS NULL THEN RETURN false; END IF;
  IF v_owner <> session_user AND NOT EXISTS (SELECT 1 FROM lane.principals WHERE login_role = session_user AND kind = 'admin') THEN
    RAISE EXCEPTION 'not the group owner' USING ERRCODE = '42501';
  END IF;
  UPDATE lane.groups SET state = 'active', aging_anchor = v_now WHERE id = p_group AND state = 'open';   -- (4)
  IF NOT FOUND THEN RETURN false; END IF;                                      -- a retry is not an activation: no clock moves
  PERFORM lane.start_parent_clock(v_pid, p_group, v_now);
  RETURN true;
END $$;

-- `lane reclass`: the only way to a higher class. The OWNER's capability decides (an admin cannot escalate), the group is
-- re-parented and its deadline re-judged.
CREATE OR REPLACE FUNCTION lane.reclass(p_group uuid, p_class text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_now timestamptz; g lane.groups; v_owner lane.principals; v_pid uuid; v_held real;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1)
  v_now := clock_timestamp();
  SELECT * INTO g FROM lane.groups WHERE id = p_group;
  IF NOT FOUND THEN RETURN false; END IF;
  IF g.owner <> session_user AND NOT EXISTS (SELECT 1 FROM lane.principals WHERE login_role = session_user AND kind = 'admin') THEN
    RAISE EXCEPTION 'not the group owner' USING ERRCODE = '42501';
  END IF;
  SELECT * INTO v_owner FROM lane.principals WHERE login_role = g.owner;
  IF NOT FOUND OR NOT lane.class_allowed(v_owner, p_class) THEN
    RAISE EXCEPTION 'class % not allowed for %', p_class, g.owner USING ERRCODE = '42501';
  END IF;
  v_pid := lane.parent_for(g.account, p_class);
  UPDATE lane.groups SET prio_class = p_class, parent_id = v_pid,
         deadline_valid = lane.judge_deadline(p_group, g.deadline_at, v_owner.can_deadline, v_now)
   WHERE id = p_group;                                                          -- (4)
  IF v_pid <> g.parent_id THEN                                                  -- a real parent change; a same-class reclass moves nothing
    IF g.state = 'active' THEN PERFORM lane.start_parent_clock(v_pid, p_group, v_now); END IF;
    -- The cores the group holds right now follow it, so the new parent's effective rank sees them. Decayed usage is per parent
    -- and not attributable to one group, so it stays where it was earned.
    SELECT coalesce(sum(grant_cpu), 0) INTO v_held FROM lane.jobs WHERE group_id = p_group AND state IN ('claimed','preparing','running');
    PERFORM lane.add_running(g.parent_id, -v_held, v_now);
    PERFORM lane.add_running(v_pid, v_held, v_now);
  END IF;
  RETURN true;
END $$;

-- The 15 s timer's job: remaining work of each parent by (work class, ref-s bucket), from the unfinished jobs of its active groups.
CREATE OR REPLACE FUNCTION lane.refresh_rem_ref(p_now timestamptz DEFAULT clock_timestamp()) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1)
  WITH b AS (
    SELECT g.parent_id AS pid, j.class, w.bucket, sum(r.rem) AS s
      FROM lane.jobs j JOIN lane.groups g ON g.id = j.group_id
     CROSS JOIN LATERAL (SELECT CASE WHEN j.est_p50_s < 120 THEN 'short' WHEN j.est_p50_s <= 600 THEN 'mid' ELSE 'long' END AS bucket) w
     CROSS JOIN LATERAL (SELECT extract(epoch FROM p_now - coalesce(j.started_at, p_now)) AS wall,
                                coalesce((SELECT f.factor FROM lane.host_factors f WHERE f.host_id = j.host AND f.class = j.class AND f.bucket = w.bucket), 1) AS factor) t
     -- Remaining work (5.7): a job not yet running counts in full; a running one counts what is left of its p50, or, past its p90,
     -- half its elapsed reference time capped by what max_ms still allows (max_ms is milliseconds, converted once).
     CROSS JOIN LATERAL (SELECT CASE WHEN j.state <> 'running' THEN j.est_p50_s::double precision
                                     WHEN t.wall / t.factor > j.est_p90_s
                                       THEN least(0.5 * t.wall / t.factor, greatest(0, (coalesce(j.max_ms::double precision / 1000, 'Infinity') - t.wall) / t.factor))
                                     ELSE greatest(0, j.est_p50_s - t.wall / t.factor) END AS rem) r
     WHERE g.state = 'active' AND j.state IN ('queued','claimed','preparing','running')
     GROUP BY 1, 2, 3),
  c AS (SELECT pid, class, jsonb_object_agg(bucket, s) AS v FROM b GROUP BY pid, class),
  d AS (SELECT pid, jsonb_object_agg(class, v) AS v FROM c GROUP BY pid)
  UPDATE lane.sched_parents p SET rem_ref = coalesce((SELECT d.v FROM d WHERE d.pid = p.id), '{}');
END $$;

-- Ownership and grants for everything in the schema. Re-applied on every run so drift is corrected.
DO $$
DECLARE r record;
BEGIN
  ALTER SCHEMA lane OWNER TO lane_definer;
  FOR r IN SELECT c.relname, c.relkind FROM pg_class c WHERE c.relnamespace = 'lane'::regnamespace AND c.relkind IN ('r','v','c') LOOP
    EXECUTE format('ALTER %s lane.%I OWNER TO lane_definer',
                   CASE r.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE 'TYPE' END, r.relname);
  END LOOP;
  FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p WHERE p.pronamespace = 'lane'::regnamespace LOOP
    EXECUTE format('ALTER FUNCTION %s OWNER TO lane_definer', r.sig);
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', r.sig);
  END LOOP;
END $$;

REVOKE ALL ON SCHEMA lane FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA lane FROM lane_agent, lane_submit, lane_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA lane FROM lane_agent, lane_submit, lane_reader;
GRANT USAGE ON SCHEMA lane TO lane_admin, lane_agent, lane_submit, lane_reader;
GRANT ALL ON ALL TABLES IN SCHEMA lane TO lane_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA lane TO lane_admin;
GRANT SELECT ON lane.job_state_counts TO lane_reader;
GRANT EXECUTE ON FUNCTION lane.claim_next(bigint, real, jsonb, text[], bigint, text[], uuid) TO lane_agent, lane_admin;
GRANT EXECUTE ON FUNCTION lane.submit_group(uuid, text, text, text, jsonb, text, jsonb, timestamptz) TO lane_submit, lane_admin;
GRANT EXECUTE ON FUNCTION lane.activate_group(uuid) TO lane_submit, lane_admin;
GRANT EXECUTE ON FUNCTION lane.reclass(uuid, text) TO lane_submit, lane_admin;
GRANT EXECUTE ON FUNCTION lane.refresh_rem_ref(timestamptz) TO lane_admin;
GRANT EXECUTE ON FUNCTION lane.record_eta(uuid, real, boolean, timestamptz) TO lane_admin;
