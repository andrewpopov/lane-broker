-- BRAIN-400: claim path (spec rev11 sections 5.1-5.5), re-applied after the table migrations. Lock order: (1) lane_sched, (3) hosts, (4) groups,
-- (5) jobs, then outbox rows. Candidates are chosen with plain reads; the winner is locked group-then-job and
-- re-validated; a failed re-validation returns no claim. Every function derives the caller from session_user.
-- Re-applied after every migration run (src/lane-db.js). A function whose signature changes needs a DROP of the old one here.
DROP FUNCTION IF EXISTS lane.refresh_rem_ref();
DROP FUNCTION IF EXISTS lane.parent_for(text, text);

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

-- Usage as of p_now: the decayed history PLUS what the cores held since usage_at have accrued, the integral of their decayed
-- contribution, rate x (H / ln 2) x (1 - 2^(-dt / H)) with H = 30 min. Reads (demotion, scoring) and settlement both use this one
-- function, so a read before a settlement and the value the settlement stores are the same number, counted once.
CREATE OR REPLACE FUNCTION lane.usage_now(p lane.sched_parents, p_now timestamptz) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT lane.decayed(p, p_now)
       + p.running_cpu::double precision * (1800 / ln(2::double precision))
         * (1 - power(0.5, greatest(0, extract(epoch FROM p_now - coalesce(p.usage_at, p_now))) / 1800))
$$;

-- U_p: cores-equivalent held recently (Slurm-style decayed usage), including the unsettled interval.
CREATE OR REPLACE FUNCTION lane.usage_cores(p lane.sched_parents, p_now timestamptz) RETURNS double precision
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT p.running_cpu + lane.usage_now(p, p_now) / 1800
$$;

-- Change the cores a parent holds, first settling usage up to p_now (usage_now), so the result does not depend on how often it runs.
CREATE OR REPLACE FUNCTION lane.add_running(p_parent uuid, p_delta real, p_now timestamptz) RETURNS void
LANGUAGE sql SET search_path = pg_catalog, lane, pg_temp AS $$
  UPDATE lane.sched_parents p
     SET usage_decayed = lane.usage_now(p, p_now),
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
           WHEN e.urgency IS NOT NULL THEN e.urgency                      -- damped by record_eta (hysteresis, dwell, slew)
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
  -- One writer at a time per group: make sure the row exists (an ETA-0 placeholder equals "no forecast yet"), then lock it, so a
  -- concurrent forecast computes from this one's result and the 25-point slew holds across writers.
  INSERT INTO lane.group_eta (group_id, eta_p90_s) VALUES (p_group, 0) ON CONFLICT (group_id) DO NOTHING;
  SELECT * INTO e FROM lane.group_eta WHERE group_id = p_group FOR UPDATE;
  v_cycle := e.cycle + 1;
  v_prev := coalesce(e.urgency, lane.urgency(g, p_now));      -- bootstrap: what the scheduler is using right now (undamped ETA-0 fallback)
  v_s0 := greatest(900, 0.5 * p_eta_p90_s);
  v_slack := extract(epoch FROM g.deadline_at - p_now) - p_eta_p90_s;
  v_raw := (CASE WHEN p_low_confidence THEN 0.5 ELSE 1 END) * 150 * least(1, greatest(0, 1 - v_slack / v_s0));
  v_on := CASE WHEN e.boost_on THEN NOT (v_slack > 1.25 * v_s0) ELSE v_slack < v_s0 END;
  IF v_on <> e.boost_on AND e.last_flip_cycle IS NOT NULL AND v_cycle - e.last_flip_cycle < 3 THEN
    v_on := e.boost_on;                                                         -- dwell: too soon to change state again
  END IF;
  INSERT INTO lane.group_eta (group_id, eta_p90_s, low_confidence, cycle, urgency, boost_on, last_flip_cycle)
    VALUES (p_group, p_eta_p90_s, p_low_confidence, v_cycle,
            v_prev + least(25, greatest(-25, CASE WHEN v_on THEN v_raw ELSE 0 END - v_prev)),
            v_on, CASE WHEN v_on <> e.boost_on THEN v_cycle ELSE e.last_flip_cycle END)
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

-- Provisioning triggers (migration 003 creates the triggers; these bodies are the live ones). Principal and class changes take ONE
-- lock before scanning, so the later of two overlapping transactions scans after the earlier committed and sees its rows: neither
-- can miss the (account, class) pair the other creates. The key is its own, not lane_sched. The scan is a new statement after the
-- lock, so under Read Committed it gets a fresh snapshot.
-- Only READ COMMITTED gives the scan after the lock a fresh snapshot; at a stronger level it would keep the transaction's first snapshot
-- and could still miss the other transaction's committed rows.
CREATE OR REPLACE FUNCTION lane.require_read_committed() RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'lane provisioning must run at READ COMMITTED, not %', current_setting('transaction_isolation') USING ERRCODE = '25000';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION lane.provision_account_parents() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  PERFORM lane.require_read_committed();
  PERFORM pg_advisory_xact_lock(hashtext('lane_provision'));
  INSERT INTO lane.sched_parents (account, prio_class)
    SELECT a, c.name FROM unnest(NEW.allowed_accounts) AS a CROSS JOIN lane.priority_classes c
    ON CONFLICT (account, prio_class) DO NOTHING;
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION lane.provision_class_parents() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
BEGIN
  PERFORM lane.require_read_committed();
  PERFORM pg_advisory_xact_lock(hashtext('lane_provision'));
  INSERT INTO lane.sched_parents (account, prio_class)
    SELECT DISTINCT a, NEW.name FROM lane.principals p, unnest(p.allowed_accounts) AS a
    ON CONFLICT (account, prio_class) DO NOTHING;
  RETURN NULL;
END $$;

-- The scheduling parent for (account, class). It was provisioned with the account (migration 003 triggers), so this only READS:
-- submit takes no scheduling lock, and a missing parent is an error, never created on the fly.
CREATE OR REPLACE FUNCTION lane.parent_of(p_account text, p_class text) RETURNS uuid
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_pid uuid;
BEGIN
  SELECT id INTO v_pid FROM lane.sched_parents WHERE account = p_account AND prio_class = p_class;
  IF v_pid IS NULL THEN
    RAISE EXCEPTION 'no scheduling parent for (%, %): parents are provisioned with the account, never created at submit', p_account, p_class USING ERRCODE = 'LN003';
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
  IF EXISTS (SELECT 1 FROM jsonb_to_recordset(p_jobs) AS x(cpu_req real, cpu_min real)
              WHERE NOT coalesce(x.cpu_req > 0 AND x.cpu_req < 'Infinity' AND coalesce(x.cpu_min, x.cpu_req) > 0
                                 AND coalesce(x.cpu_min, x.cpu_req) <= x.cpu_req, false)) THEN
    RAISE EXCEPTION 'job cpu_req and cpu_min must be finite, positive and cpu_min <= cpu_req' USING ERRCODE = '22023';
  END IF;
  INSERT INTO lane.groups (id, submit_uuid, kind, account, owner, parent_id, prio_class, snapshot, aggregator, deadline_at)
    VALUES (gen_random_uuid(), p_submit_uuid, p_kind, p_account, session_user, lane.parent_of(p_account, p_class), p_class,
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
  v_pid := lane.parent_of(g.account, p_class);
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

-- BRAIN-930 S1: pull dispatch (lane_dispatch). Every body is plpgsql so these apply before migration 005's tables exist; the schema
-- is created here for that reason. ld_entry is called first by every ld_* function.
CREATE SCHEMA IF NOT EXISTS lane_dispatch;

-- Functions removed or re-signed by BRAIN-1035 (rev 6). ld_reap keeps its argument types but renames its parameters, which CREATE OR REPLACE refuses.
DROP FUNCTION IF EXISTS lane_dispatch.ld_reassert(jsonb);
DROP FUNCTION IF EXISTS lane_dispatch.ld_cancel_quarantined(uuid);
DROP FUNCTION IF EXISTS lane_dispatch.ld_register(text, jsonb);
DROP FUNCTION IF EXISTS lane_dispatch.ld_enqueue(uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int);
DROP FUNCTION IF EXISTS lane_dispatch.ld_enqueue_fenced(text, uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int);
DROP FUNCTION IF EXISTS lane_dispatch.ld_reap(interval, interval, interval);

-- Every clock read in lane_dispatch goes through now(). The GUC lane.test_now moves it for tests, but only when the value is the one stored by ALTER DATABASE or
-- ALTER ROLE (pg_db_role_setting): a session SET to anything else, which any login can issue, is ignored, so a runner cannot move the queue's clock.
CREATE OR REPLACE FUNCTION lane_dispatch.now() RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_set text := coalesce(current_setting('lane.test_now', true), '');
BEGIN
  IF v_set <> '' AND EXISTS (
       SELECT 1 FROM pg_db_role_setting s
        WHERE s.setdatabase IN (0, (SELECT d.oid FROM pg_database d WHERE d.datname = current_database()))
          AND s.setrole IN (0, (SELECT r.oid FROM pg_roles r WHERE r.rolname = session_user))
          AND 'lane.test_now=' || v_set = ANY (s.setconfig)) THEN
    RETURN v_set::timestamptz;
  END IF;
  RETURN clock_timestamp();
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.protocol_min() RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  RETURN 1;
END $$;

-- The one queue-authored terminal result: restore marking, the release cap and the reaper all settle a row lost with it.
CREATE OR REPLACE FUNCTION lane_dispatch.lost_result(p_reason text, p_epoch int) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  RETURN jsonb_build_object('kind', 'lost', 'reason', p_reason, 'author', 'queue', 'epoch', p_epoch);
END $$;

-- A restore settles every non-terminal ticket lost and forgets every claim request; runners are fenced on their next heartbeat. The caller holds the cluster row lock.
CREATE OR REPLACE FUNCTION lane_dispatch.settle_restore(p_timeline int) RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_lost int;
BEGIN
  UPDATE lane_dispatch.cluster SET timeline = p_timeline, incarnation = incarnation + 1 WHERE id = 1;
  UPDATE lane_dispatch.tickets SET state = 'lost', finished_at = lane_dispatch.now(), result = lane_dispatch.lost_result('queue-restored', epoch)
   WHERE state NOT IN ('finished','cancelled','withdrawn','lost');
  GET DIAGNOSTICS v_lost = ROW_COUNT;
  DELETE FROM lane_dispatch.claim_requests;
  RETURN v_lost;
END $$;

-- A restore ends on a new timeline: the first call after it settles the queue (design section 3, invariant 3). Returns false when this call found the mismatch.
-- The cluster row is locked waiting and re-read under the lock, so concurrent callers settle once.
CREATE OR REPLACE FUNCTION lane_dispatch.ld_timeline_ok() RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_timeline int;
BEGIN
  v_timeline := (SELECT c.timeline_id FROM pg_control_checkpoint() c);
  IF v_timeline = (SELECT c.timeline FROM lane_dispatch.cluster c WHERE c.id = 1) THEN RETURN true; END IF;
  PERFORM 1 FROM lane_dispatch.cluster WHERE id = 1 FOR UPDATE;
  IF v_timeline <> (SELECT c.timeline FROM lane_dispatch.cluster c WHERE c.id = 1) THEN
    PERFORM lane_dispatch.settle_restore(v_timeline);
  END IF;
  RETURN false;
END $$;

-- A restore that keeps the timeline (docs/lane-dispatch-db-runbook.md): the same settling, unconditional. Returns the number of tickets settled.
CREATE OR REPLACE FUNCTION lane_dispatch.ld_restore_reset() RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  PERFORM lane_dispatch.ld_entry();
  PERFORM 1 FROM lane_dispatch.cluster WHERE id = 1 FOR UPDATE;
  RETURN lane_dispatch.settle_restore((SELECT c.timeline_id FROM pg_control_checkpoint() c));
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_entry() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'lane_dispatch must run at READ COMMITTED, not %', current_setting('transaction_isolation') USING ERRCODE = '25000';
  END IF;
  PERFORM lane_dispatch.ld_timeline_ok();
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.me(p_kind text) RETURNS TABLE (host text, tags text[], allowed_repos text[], max_claims int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY SELECT p.host, p.tags, p.allowed_repos, p.max_claims FROM lane_dispatch.principals p WHERE p.login_role = session_user AND p.kind = p_kind;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no % principal for %', p_kind, session_user USING ERRCODE = '42501';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.fence_ok(p_epoch int, p_token uuid, p_host text, p_row_epoch int, p_row_token uuid, p_row_host text, p_row_state text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  RETURN coalesce(p_row_state IN ('claimed','running') AND p_row_epoch = p_epoch AND p_row_token = p_token AND p_row_host = p_host, false);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.eligible(p_requires_tags text[], p_repo_key text, p_tags text[], p_allowed_repos text[]) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  RETURN p_requires_tags <@ p_tags AND (p_repo_key = ANY (p_allowed_repos) OR '*' = ANY (p_allowed_repos));
END $$;

-- Agent principals that could ever run the ticket: the order key that lets a host take what few hosts can run first, and what ld_placeable reports.
CREATE OR REPLACE FUNCTION lane_dispatch.scarcity(p_requires_tags text[], p_repo_key text) RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  RETURN (SELECT count(*)::int FROM lane_dispatch.principals p
           WHERE p.kind = 'agent' AND p.max_claims > 0 AND lane_dispatch.eligible(coalesce(p_requires_tags, '{}'), p_repo_key, p.tags, p.allowed_repos));
END $$;

-- src/priority.js score(): min(ceiling, 2 * tierFactor + 2 * ageFactor), age capped at 20 min, a non-high held at 0.99 of the tier weight until it has waited 60 min.
-- The tier is the priority band: 7-9 high, 4-6 medium, 0-3 low. A ticket that does not age credits no wait.
-- Priority 9 (a release candidate) adds 0.01 on top, so it outranks every other high and every aged ticket that ties a high.
CREATE OR REPLACE FUNCTION lane_dispatch.eff_score(p_priority int, p_ages boolean, p_age_s double precision) RETURNS double precision
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_tier int := CASE WHEN p_priority >= 7 THEN 2 WHEN p_priority >= 4 THEN 1 ELSE 0 END; v_credit double precision := CASE WHEN p_ages THEN greatest(p_age_s, 0) ELSE 0 END;
BEGIN
  RETURN least(CASE WHEN v_tier = 2 OR v_credit >= 3600 THEN 2.0 ELSE 1.98 END, 2.0 * v_tier / 2 + 2.0 * least(1.0, v_credit / 1200))
         + CASE WHEN p_priority >= 9 THEN 0.01 ELSE 0 END;
END $$;

DROP FUNCTION IF EXISTS lane_dispatch.ticket_view(uuid);
CREATE OR REPLACE FUNCTION lane_dispatch.ticket_view(p_id uuid, p_owner text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  RETURN (SELECT jsonb_build_object('id', t.id, 'state', t.state, 'epoch', t.epoch, 'execution_id', t.id::text || '.e' || t.epoch::text,
                                    'host', t.host, 'lease_until', t.lease_until, 'cancel_requested', t.cancel_requested, 'urls', t.urls,
                                    'result', t.result, 'incarnation', t.incarnation, 'created_at', t.created_at,
                                    'started_at', t.started_at, 'finished_at', t.finished_at,
                                    'attempt_ref', CASE WHEN t.claim_token IS NULL THEN NULL
                                                        ELSE encode(sha256(convert_to('lane-attempt-ref:' || t.claim_token::text, 'UTF8')), 'hex') END)
            FROM lane_dispatch.tickets t WHERE t.id = p_id AND t.owner = p_owner);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.claim_view(p_id uuid, p_epoch int, p_token uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  IF p_id IS NULL THEN RETURN jsonb_build_object('status', 'none'); END IF;
  RETURN (SELECT jsonb_build_object('status', 'claimed', 'id', t.id, 'epoch', p_epoch, 'token', p_token,
                                    'execution_id', t.id::text || '.e' || p_epoch::text, 'repo_key', t.repo_key, 'lane', t.lane,
                                    'lane_key', t.lane_key, 'binding', t.binding, 'header', t.header, 'snapshot', t.snapshot,
                                    'lease_until', t.lease_until)
            FROM lane_dispatch.tickets t WHERE t.id = p_id);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.claim_replay(p_req uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_prev record;
BEGIN
  SELECT c.ticket, c.epoch, c.claim_token INTO v_prev FROM lane_dispatch.claim_requests c WHERE c.login_role = session_user AND c.req = p_req;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN lane_dispatch.claim_view(v_prev.ticket, v_prev.epoch, v_prev.claim_token);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_set_principal(p_login_role text, p_kind text, p_host text, p_tags text[], p_allowed_repos text[], p_max_claims int)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  PERFORM lane_dispatch.ld_entry();
  IF p_host IS NOT NULL AND p_host !~ '^[a-z0-9][a-z0-9-]{0,54}$' THEN
    RAISE EXCEPTION 'host % is not a valid channel name', p_host USING ERRCODE = '22023';
  END IF;
  INSERT INTO lane_dispatch.principals (login_role, kind, host, tags, allowed_repos, max_claims)
    VALUES (p_login_role, p_kind, p_host, coalesce(p_tags, '{}'), coalesce(p_allowed_repos, '{}'), coalesce(p_max_claims, 1))
    ON CONFLICT (login_role) DO UPDATE SET kind = EXCLUDED.kind, host = EXCLUDED.host, tags = EXCLUDED.tags,
                                           allowed_repos = EXCLUDED.allowed_repos, max_claims = EXCLUDED.max_claims;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_enqueue(p_id uuid, p_repo_key text, p_lane text, p_lane_key text, p_binding jsonb, p_header jsonb, p_snapshot jsonb,
                                                    p_requires_tags text[], p_cpu_cores real, p_memory_bytes bigint, p_priority int DEFAULT 0, p_min_protocol int DEFAULT 1,
                                                    p_ages boolean DEFAULT true, p_source text DEFAULT NULL, p_est_run_s real DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  INSERT INTO lane_dispatch.tickets (id, owner, repo_key, lane, lane_key, binding, header, snapshot, requires_tags, cpu_cores, memory_bytes, priority, incarnation,
                                     min_protocol, ages, source, est_run_s, created_at)
    VALUES (p_id, session_user, p_repo_key, p_lane, p_lane_key, p_binding, p_header, p_snapshot, coalesce(p_requires_tags, '{}'), p_cpu_cores, p_memory_bytes,
            coalesce(p_priority, 0), (SELECT c.incarnation FROM lane_dispatch.cluster c), coalesce(p_min_protocol, 1), coalesce(p_ages, true), p_source, p_est_run_s,
            lane_dispatch.now())
    ON CONFLICT (id) DO NOTHING;
  IF FOUND THEN PERFORM pg_notify('ld_work', ''); END IF;
  IF NOT EXISTS (SELECT 1 FROM lane_dispatch.tickets t WHERE t.id = p_id AND t.owner = session_user) THEN
    RAISE EXCEPTION 'ticket % already exists', p_id USING ERRCODE = '23505';
  END IF;
  RETURN lane_dispatch.ticket_view(p_id, session_user);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_get(p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  RETURN lane_dispatch.ticket_view(p_id, session_user);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_list_mine(p_states text[] DEFAULT NULL, p_limit int DEFAULT 100) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  RETURN coalesce((SELECT jsonb_agg(lane_dispatch.ticket_view(m.id, session_user) ORDER BY m.created_at DESC, m.id)
                     FROM (SELECT t.id, t.created_at FROM lane_dispatch.tickets t
                            WHERE t.owner = session_user AND (p_states IS NULL OR t.state = ANY (p_states))
                            ORDER BY t.created_at DESC, t.id LIMIT least(greatest(coalesce(p_limit, 100), 1), 500)) m), '[]'::jsonb);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_withdraw(p_id uuid) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_state text;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  SELECT t.state INTO v_state FROM lane_dispatch.tickets t WHERE t.id = p_id AND t.owner = session_user FOR UPDATE;
  IF NOT FOUND THEN RETURN 'missing'; END IF;
  IF v_state = 'queued' THEN
    UPDATE lane_dispatch.tickets SET state = 'withdrawn', finished_at = lane_dispatch.now() WHERE id = p_id;
    RETURN 'withdrawn';
  END IF;
  RETURN v_state;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_cancel(p_id uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_t record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  SELECT t.state, t.cancel_requested, t.claim_token, t.host, t.epoch, t.result INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id AND t.owner = session_user FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('state', 'missing', 'cancel_requested', false, 'confirmed', false); END IF;
  IF v_t.state = 'queued' THEN
    UPDATE lane_dispatch.tickets SET state = 'cancelled', cancel_requested = true, finished_at = lane_dispatch.now(),
           result = jsonb_build_object('kind', 'cancelled', 'cancelConfirmed', true, 'source', 'queued', 'epoch', v_t.epoch, 'author', 'queue')
     WHERE id = p_id;
    RETURN jsonb_build_object('state', 'cancelled', 'cancel_requested', true, 'confirmed', true);
  END IF;
  IF v_t.state IN ('claimed','running') THEN
    UPDATE lane_dispatch.tickets SET cancel_requested = true WHERE id = p_id;
    PERFORM pg_notify('ld_host_' || v_t.host, '');
    RETURN jsonb_build_object('state', v_t.state, 'cancel_requested', true, 'confirmed', false);
  END IF;
  RETURN jsonb_build_object('state', v_t.state, 'cancel_requested', v_t.cancel_requested, 'confirmed', coalesce(CASE WHEN jsonb_typeof(v_t.result->'cancelConfirmed') = 'boolean' THEN (v_t.result->>'cancelConfirmed')::boolean END, v_t.state = 'cancelled' AND v_t.result IS NULL));
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_attach_urls(p_id uuid, p_epoch int, p_urls jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_host text;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  UPDATE lane_dispatch.tickets SET urls = p_urls WHERE id = p_id AND owner = session_user AND epoch = p_epoch AND state IN ('claimed','running')
    RETURNING host INTO v_host;
  IF FOUND THEN PERFORM pg_notify('ld_host_' || v_host, ''); END IF;
  RETURN FOUND;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_register(p_version text, p_protocol int, p_schedule jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('agent');
  INSERT INTO lane_dispatch.runners (host, version, protocol, schedule, last_seen) VALUES (v_me.host, p_version, coalesce(p_protocol, 1), p_schedule, lane_dispatch.now())
    ON CONFLICT (host) DO UPDATE SET version = EXCLUDED.version, protocol = EXCLUDED.protocol, schedule = EXCLUDED.schedule, last_seen = EXCLUDED.last_seen;
  RETURN jsonb_build_object('host', v_me.host, 'max_claims', v_me.max_claims, 'protocol_min', lane_dispatch.protocol_min());
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_claim(p_req uuid, p_free_cpu real, p_free_mem bigint, p_held_keys text[] DEFAULT '{}') RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_out jsonb; v_now timestamptz; v_protocol int; v_held int; v_id uuid; v_epoch int; v_token uuid;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT p.host, p.tags, p.allowed_repos, p.max_claims INTO v_me FROM lane_dispatch.principals p WHERE p.login_role = session_user AND p.kind = 'agent' FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'no agent principal for %', session_user USING ERRCODE = '42501';
  END IF;
  v_out := lane_dispatch.claim_replay(p_req);
  IF v_out IS NOT NULL THEN RETURN v_out; END IF;
  SELECT r.protocol INTO v_protocol FROM lane_dispatch.runners r WHERE r.host = v_me.host;
  IF NOT FOUND OR v_protocol < lane_dispatch.protocol_min() THEN
    RETURN jsonb_build_object('status', 'upgrade', 'protocol_min', lane_dispatch.protocol_min());
  END IF;
  v_now := lane_dispatch.now();
  SELECT count(*) INTO v_held FROM lane_dispatch.tickets t WHERE t.host = v_me.host AND t.state IN ('claimed','running');
  IF v_held >= v_me.max_claims THEN RETURN jsonb_build_object('status', 'none'); END IF;
  SELECT t.id INTO v_id FROM lane_dispatch.tickets t
   WHERE t.state = 'queued' AND NOT t.cancel_requested AND t.min_protocol <= v_protocol AND t.not_before <= v_now
     AND lane_dispatch.eligible(t.requires_tags, t.repo_key, v_me.tags, v_me.allowed_repos)
     AND t.cpu_cores <= p_free_cpu AND t.memory_bytes <= p_free_mem AND NOT (t.lane_key = ANY (coalesce(p_held_keys, '{}')))
   ORDER BY lane_dispatch.eff_score(t.priority, t.ages, extract(epoch FROM v_now - t.created_at)) DESC,
            CASE WHEN lane_dispatch.eff_score(t.priority, t.ages, extract(epoch FROM v_now - t.created_at)) >= 2.0 THEN t.created_at END NULLS LAST,
            lane_dispatch.scarcity(t.requires_tags, t.repo_key), t.created_at, t.id LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 'none'); END IF;
  v_token := gen_random_uuid();
  UPDATE lane_dispatch.tickets SET state = 'claimed', epoch = epoch + 1, claim_token = v_token, host = v_me.host, lease_until = v_now + interval '120 seconds'
   WHERE id = v_id RETURNING epoch INTO v_epoch;
  INSERT INTO lane_dispatch.attempts (ticket, epoch, host, claim_token) VALUES (v_id, v_epoch, v_me.host, v_token);
  INSERT INTO lane_dispatch.claim_requests (login_role, req, ticket, epoch, claim_token) VALUES (session_user, p_req, v_id, v_epoch, v_token);
  RETURN lane_dispatch.claim_view(v_id, v_epoch, v_token);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_start(p_id uuid, p_epoch int, p_token uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_t record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('agent');
  SELECT * INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id FOR UPDATE;
  IF NOT lane_dispatch.fence_ok(p_epoch, p_token, v_me.host, v_t.epoch, v_t.claim_token, v_t.host, v_t.state) THEN RETURN false; END IF;
  IF v_t.state = 'running' THEN RETURN NOT v_t.cancel_requested; END IF;
  IF v_t.state <> 'claimed' THEN RETURN false; END IF;
  IF v_t.cancel_requested THEN RETURN false; END IF;
  IF v_t.lease_until <= lane_dispatch.now() THEN RETURN false; END IF;
  UPDATE lane_dispatch.tickets SET state = 'running', started_at = lane_dispatch.now(), lease_until = lane_dispatch.now() + interval '120 seconds' WHERE id = p_id;
  RETURN true;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_heartbeat(p_id uuid, p_epoch int, p_token uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_t record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('agent');
  SELECT * INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id FOR UPDATE;
  IF NOT lane_dispatch.fence_ok(p_epoch, p_token, v_me.host, v_t.epoch, v_t.claim_token, v_t.host, v_t.state) THEN
    RETURN jsonb_build_object('fenced', true, 'cancel', false);
  END IF;
  UPDATE lane_dispatch.tickets SET lease_until = lane_dispatch.now() + interval '120 seconds' WHERE id = p_id;
  RETURN jsonb_build_object('fenced', false, 'cancel', v_t.cancel_requested);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_finish(p_id uuid, p_epoch int, p_token uuid, p_result jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_t record; v_state text;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('agent');
  SELECT * INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id FOR UPDATE;
  IF lane_dispatch.fence_ok(p_epoch, p_token, v_me.host, v_t.epoch, v_t.claim_token, v_t.host, v_t.state) THEN
    v_state := CASE WHEN p_result->>'kind' = 'cancelled' THEN 'cancelled' ELSE 'finished' END;
    UPDATE lane_dispatch.tickets SET state = v_state, result = (p_result - 'author') || jsonb_build_object('epoch', p_epoch), finished_at = lane_dispatch.now() WHERE id = p_id;
    RETURN jsonb_build_object('fenced', false, 'state', v_state);
  END IF;
  IF v_t.state IN ('finished','cancelled') AND v_t.result IS NOT NULL AND v_t.epoch = p_epoch AND v_t.claim_token = p_token AND v_t.host = v_me.host THEN
    RETURN jsonb_build_object('fenced', false, 'state', v_t.state);
  END IF;
  RETURN jsonb_build_object('fenced', true, 'state', v_t.state);
END $$;

-- A claimed ticket that goes back to the queue (release or lease expiry), the one place the requeue cap and backoff live. The caller holds the row lock.
-- Returns the state it settled: cancelled when a cancel was requested, lost (release-cap) on the sixth return, otherwise queued with a not_before backoff.
CREATE OR REPLACE FUNCTION lane_dispatch.return_to_queue(p_id uuid, p_now timestamptz) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_t record; v_state text;
BEGIN
  SELECT t.cancel_requested, t.requeues, t.epoch INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id;
  v_state := CASE WHEN v_t.cancel_requested THEN 'cancelled' WHEN v_t.requeues >= 5 THEN 'lost' ELSE 'queued' END;
  UPDATE lane_dispatch.tickets SET state = v_state, claim_token = NULL, host = NULL, lease_until = NULL, started_at = NULL,
         requeues = requeues + CASE WHEN v_state = 'queued' THEN 1 ELSE 0 END,
         not_before = CASE WHEN v_state = 'queued' THEN p_now + least(interval '5 seconds' * 2 ^ requeues, interval '60 seconds') ELSE not_before END,
         finished_at = CASE WHEN v_state = 'queued' THEN NULL ELSE p_now END,
         result = CASE WHEN v_state = 'lost' THEN lane_dispatch.lost_result('release-cap', epoch) ELSE result END
   WHERE id = p_id;
  RETURN v_state;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_release(p_id uuid, p_epoch int, p_token uuid, p_proof text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_t record; v_state text;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('agent');
  IF p_proof IS NULL OR p_proof NOT IN ('never-started','cancel-confirmed') THEN
    RAISE EXCEPTION 'unknown release proof %', p_proof USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id FOR UPDATE;
  IF NOT lane_dispatch.fence_ok(p_epoch, p_token, v_me.host, v_t.epoch, v_t.claim_token, v_t.host, v_t.state) THEN
    RETURN jsonb_build_object('fenced', true, 'state', v_t.state);
  END IF;
  IF v_t.state = 'running' THEN
    IF NOT (v_t.cancel_requested AND p_proof = 'cancel-confirmed') THEN RETURN jsonb_build_object('refused', 'started'); END IF;
    UPDATE lane_dispatch.tickets SET state = 'cancelled', claim_token = NULL, host = NULL, lease_until = NULL, finished_at = lane_dispatch.now() WHERE id = p_id;
    RETURN jsonb_build_object('fenced', false, 'state', 'cancelled');
  END IF;
  v_state := lane_dispatch.return_to_queue(p_id, lane_dispatch.now());
  IF v_state = 'queued' THEN PERFORM pg_notify('ld_work', ''); END IF;
  RETURN jsonb_build_object('fenced', false, 'state', v_state);
END $$;

-- The timer. Never requeues a running row on expiry alone (invariant 2). Amnesty extends every lease after a gap between ticks longer than p_amnesty_after.
-- The cluster row is the one lock it takes, so two reapers (and a restore) serialise.
CREATE OR REPLACE FUNCTION lane_dispatch.ld_reap(p_lease_ttl interval DEFAULT interval '120 seconds', p_lost_after interval DEFAULT interval '2 hours',
                                                 p_amnesty_after interval DEFAULT interval '60 seconds') RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_now timestamptz; v_amnesty boolean; v_id uuid; v_state text; v_requeued int := 0; v_cancelled int := 0; v_lost int := 0; v_runner_lost int;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  PERFORM 1 FROM lane_dispatch.cluster WHERE id = 1 FOR UPDATE;
  v_now := lane_dispatch.now();
  v_amnesty := v_now - (SELECT c.last_tick FROM lane_dispatch.cluster c WHERE c.id = 1) > p_amnesty_after;
  IF v_amnesty THEN
    UPDATE lane_dispatch.tickets SET lease_until = greatest(lease_until, v_now + p_lease_ttl) WHERE state IN ('claimed','running');
  END IF;
  UPDATE lane_dispatch.cluster SET last_tick = v_now WHERE id = 1;
  FOR v_id IN SELECT t.id FROM lane_dispatch.tickets t WHERE t.state = 'claimed' AND t.lease_until < v_now ORDER BY t.id FOR UPDATE LOOP
    v_state := lane_dispatch.return_to_queue(v_id, v_now);
    v_requeued := v_requeued + (v_state = 'queued')::int;
    v_cancelled := v_cancelled + (v_state = 'cancelled')::int;
    v_lost := v_lost + (v_state = 'lost')::int;
  END LOOP;
  UPDATE lane_dispatch.tickets t SET state = 'lost', finished_at = v_now, result = lane_dispatch.lost_result('runner-lost', t.epoch)
   WHERE t.state = 'running' AND t.lease_until + (p_lost_after - p_lease_ttl) < v_now;
  GET DIAGNOSTICS v_runner_lost = ROW_COUNT;
  IF v_requeued > 0 THEN PERFORM pg_notify('ld_work', ''); END IF;
  DELETE FROM lane_dispatch.claim_requests WHERE ctid IN (
    SELECT c.ctid FROM lane_dispatch.claim_requests c JOIN lane_dispatch.tickets t ON t.id = c.ticket
     WHERE c.created_at < v_now - interval '24 hours' AND t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < v_now - interval '24 hours' LIMIT 1000);
  RETURN jsonb_build_object('amnesty', v_amnesty, 'requeued', v_requeued, 'cancelled', v_cancelled, 'lost', v_lost + v_runner_lost);
END $$;

-- BRAIN-1036 S3: the pull client's queue surface. Fenced enqueue, placement check and retention.
CREATE OR REPLACE FUNCTION lane_dispatch.ld_queue_ref() RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  RETURN (SELECT c.timeline::text || '.' || c.incarnation::text FROM lane_dispatch.cluster c WHERE c.id = 1);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_enqueue_fenced(p_expect_ref text, p_id uuid, p_repo_key text, p_lane text, p_lane_key text, p_binding jsonb,
                                                           p_header jsonb, p_snapshot jsonb, p_requires_tags text[], p_cpu_cores real, p_memory_bytes bigint, p_priority int DEFAULT 0, p_min_protocol int DEFAULT 1,
                                                           p_ages boolean DEFAULT true, p_source text DEFAULT NULL, p_est_run_s real DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_ref text;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  IF p_snapshot ? 'blobs' AND jsonb_typeof(p_snapshot->'blobs') <> 'array' THEN
    RAISE EXCEPTION 'snapshot.blobs must be an array' USING ERRCODE = '22023';
  END IF;
  SELECT c.timeline::text || '.' || c.incarnation::text INTO v_ref FROM lane_dispatch.cluster c WHERE c.id = 1;
  IF p_expect_ref IS DISTINCT FROM v_ref THEN
    RETURN jsonb_build_object('state', 'stale-queue-ref', 'queue_ref', v_ref);
  END IF;
  RETURN lane_dispatch.ld_enqueue(p_id, p_repo_key, p_lane, p_lane_key, p_binding, p_header, p_snapshot, p_requires_tags, p_cpu_cores, p_memory_bytes, p_priority,
                                  p_min_protocol, p_ages, p_source, p_est_run_s);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_placeable(p_repo_key text, p_requires_tags text[]) RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  RETURN lane_dispatch.scarcity(p_requires_tags, p_repo_key);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_prune_candidates(p_objects_after interval, p_rows_after interval, p_after_finished timestamptz DEFAULT NULL,
                                                             p_after_id uuid DEFAULT NULL, p_limit int DEFAULT 200) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_limit int := least(greatest(coalesce(p_limit, 200), 1), 500); v_items jsonb; v_count int; v_next jsonb;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  IF (p_after_finished IS NULL) <> (p_after_id IS NULL) THEN
    RAISE EXCEPTION 'cursor must be both null or both set' USING ERRCODE = '22023';
  END IF;
  SELECT coalesce(jsonb_agg(jsonb_build_object('id', m.id, 'finished_at', m.finished_at, 'row_due', m.finished_at < lane_dispatch.now() - p_rows_after) ORDER BY m.finished_at, m.id), '[]'::jsonb),
         count(*)::int INTO v_items, v_count
    FROM (SELECT t.id, t.finished_at FROM lane_dispatch.tickets t
           WHERE t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < lane_dispatch.now() - p_objects_after
             AND ((p_after_finished IS NULL AND p_after_id IS NULL)
                  OR (p_after_finished IS NOT NULL AND p_after_id IS NOT NULL AND (t.finished_at, t.id) > (p_after_finished, p_after_id)))
           ORDER BY t.finished_at, t.id LIMIT v_limit) m;
  IF v_count = v_limit THEN
    v_next := jsonb_build_object('finished_at', v_items->(v_count - 1)->'finished_at', 'id', v_items->(v_count - 1)->'id');
  END IF;
  RETURN jsonb_build_object('items', v_items, 'next', v_next);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_prune(p_ids uuid[], p_rows_after interval) RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_due uuid[]; v_deleted int;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  IF coalesce(cardinality(p_ids), 0) > 500 THEN
    RAISE EXCEPTION 'at most 500 ids per call' USING ERRCODE = '22023';
  END IF;
  SELECT coalesce(array_agg(t.id), '{}') INTO v_due FROM (
    SELECT t.id FROM lane_dispatch.tickets t
     WHERE t.id = ANY (p_ids) AND t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < lane_dispatch.now() - p_rows_after
       ORDER BY t.id FOR UPDATE) t;
  DELETE FROM lane_dispatch.log_chunks WHERE ticket = ANY (v_due);
  DELETE FROM lane_dispatch.claim_requests WHERE ticket = ANY (v_due);
  DELETE FROM lane_dispatch.tickets WHERE id = ANY (v_due);
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_prune_chunks(p_terminal_after interval DEFAULT interval '1 hour', p_limit int DEFAULT 10000) RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_deleted int;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  DELETE FROM lane_dispatch.log_chunks WHERE ctid IN (
    SELECT c.ctid FROM lane_dispatch.log_chunks c LEFT JOIN lane_dispatch.tickets t ON t.id = c.ticket
     WHERE t.id IS NULL OR (t.state IN ('finished','cancelled','withdrawn','lost') AND t.finished_at < lane_dispatch.now() - p_terminal_after)
     LIMIT least(greatest(coalesce(p_limit, 10000), 1), 100000));
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_blobs_live(p_refs jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
BEGIN
  PERFORM lane_dispatch.ld_entry();
  IF jsonb_typeof(p_refs) IS DISTINCT FROM 'array' OR jsonb_array_length(p_refs) > 500 THEN
    RAISE EXCEPTION 'refs must be a json array of at most 500' USING ERRCODE = '22023';
  END IF;
  RETURN coalesce((SELECT jsonb_agg(r) FROM jsonb_array_elements(p_refs) r WHERE EXISTS (
    SELECT 1 FROM lane_dispatch.tickets t, jsonb_array_elements(CASE WHEN jsonb_typeof(t.snapshot->'blobs') = 'array' THEN t.snapshot->'blobs' ELSE '[]'::jsonb END) b
     WHERE t.state NOT IN ('finished','cancelled','withdrawn','lost')
       AND b->>'sha256' = r->>'sha256' AND b->>'ns' = r->>'ns')), '[]'::jsonb);
END $$;

-- Live logs. Every call is idempotent on (ticket, epoch, seq): a call the guard killed is retried verbatim.
CREATE OR REPLACE FUNCTION lane_dispatch.ld_log(p_id uuid, p_epoch int, p_token uuid, p_seq int, p_bytes bytea) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_t record; v_logged bigint; v_len int; v_inserted int;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('agent');
  SELECT t.epoch, t.claim_token, t.host, t.state INTO v_t FROM lane_dispatch.tickets t WHERE t.id = p_id;
  IF NOT lane_dispatch.fence_ok(p_epoch, p_token, v_me.host, v_t.epoch, v_t.claim_token, v_t.host, v_t.state) THEN RETURN 'fenced'; END IF;
  v_len := octet_length(p_bytes);
  IF v_len > 262144 THEN RETURN 'capped'; END IF;
  SELECT a.log_bytes INTO v_logged FROM lane_dispatch.attempts a WHERE a.ticket = p_id AND a.epoch = p_epoch FOR UPDATE;
  IF NOT FOUND THEN RETURN 'fenced'; END IF;
  IF v_logged + v_len > 67108864 THEN RETURN 'capped'; END IF;
  IF (SELECT coalesce(sum(octet_length(c.bytes)), 0) FROM lane_dispatch.log_chunks c) + v_len > 1073741824 THEN RETURN 'capped'; END IF;
  INSERT INTO lane_dispatch.log_chunks (ticket, epoch, seq, bytes) VALUES (p_id, p_epoch, p_seq, p_bytes) ON CONFLICT (ticket, epoch, seq) DO NOTHING;
  GET DIAGNOSTICS v_inserted = ROW_COUNT;
  IF v_inserted = 1 THEN
    UPDATE lane_dispatch.attempts SET log_bytes = log_bytes + v_len WHERE ticket = p_id AND epoch = p_epoch;
  END IF;
  RETURN 'ok';
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_log_read(p_id uuid, p_epoch int, p_after_seq int) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  IF NOT EXISTS (SELECT 1 FROM lane_dispatch.tickets t WHERE t.id = p_id AND t.owner = session_user) THEN RETURN NULL; END IF;
  RETURN coalesce((SELECT jsonb_agg(jsonb_build_object('seq', m.seq, 'data', encode(m.bytes, 'base64')) ORDER BY m.seq)
                     FROM (SELECT c.seq, c.bytes FROM lane_dispatch.log_chunks c
                            WHERE c.ticket = p_id AND c.epoch = p_epoch AND c.seq > p_after_seq ORDER BY c.seq LIMIT 64) m), '[]'::jsonb);
END $$;

CREATE OR REPLACE FUNCTION lane_dispatch.ld_log_ack(p_id uuid, p_epoch int, p_upto_seq int) RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane_dispatch, pg_temp AS $$
DECLARE v_me record; v_deleted int;
BEGIN
  PERFORM lane_dispatch.ld_entry();
  SELECT * INTO v_me FROM lane_dispatch.me('submit');
  DELETE FROM lane_dispatch.log_chunks c USING lane_dispatch.tickets t
   WHERE t.id = p_id AND t.owner = session_user AND c.ticket = t.id AND c.epoch = p_epoch AND c.seq <= p_upto_seq;
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END $$;

-- The backend guard (design section 2 item 4). SECURITY INVOKER on purpose: it runs as the lane_admin login, whose cluster-level grants
-- (docs/lane-dispatch-db-runbook.md) let it see and signal other roles' backends. Transaction age, not query age, whatever the state.
CREATE OR REPLACE FUNCTION lane_dispatch.ld_guard(p_max_age interval DEFAULT interval '5 seconds') RETURNS int
LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $$
DECLARE v_killed int;
BEGIN
  SELECT count(*) INTO v_killed FROM (
    SELECT pg_terminate_backend(a.pid)
      FROM pg_stat_activity a JOIN pg_roles r ON r.rolname = a.usename
     WHERE (starts_with(a.usename, 'lane_agent_') OR starts_with(a.usename, 'lane_submit_'))
       AND NOT r.rolsuper AND a.pid <> pg_backend_pid() AND a.datname = current_database()
       AND clock_timestamp() - a.xact_start > p_max_age) k;
  RETURN v_killed;
END $$;

-- Ownership and grants for everything in the schema. Re-applied on every run so drift is corrected.
DO $$
DECLARE r record; s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['lane', 'lane_dispatch'] LOOP
    EXECUTE format('ALTER SCHEMA %I OWNER TO lane_definer', s);
    FOR r IN SELECT c.relname, c.relkind FROM pg_class c WHERE c.relnamespace = s::regnamespace AND c.relkind IN ('r','v','c') LOOP
      EXECUTE format('ALTER %s %I.%I OWNER TO lane_definer',
                     CASE r.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE 'TYPE' END, s, r.relname);
    END LOOP;
    FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p WHERE p.pronamespace = s::regnamespace LOOP
      EXECUTE format('ALTER FUNCTION %s OWNER TO lane_definer', r.sig);
      EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', r.sig);
    END LOOP;
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

REVOKE ALL ON SCHEMA lane_dispatch FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA lane_dispatch FROM PUBLIC, lane_agent, lane_submit, lane_admin, lane_reader;
GRANT USAGE ON SCHEMA lane_dispatch TO lane_agent, lane_submit, lane_admin;
GRANT EXECUTE ON FUNCTION lane_dispatch.ld_register(text, int, jsonb), lane_dispatch.ld_claim(uuid, real, bigint, text[]), lane_dispatch.ld_start(uuid, int, uuid),
  lane_dispatch.ld_heartbeat(uuid, int, uuid), lane_dispatch.ld_finish(uuid, int, uuid, jsonb), lane_dispatch.ld_release(uuid, int, uuid, text),
  lane_dispatch.ld_log(uuid, int, uuid, int, bytea) TO lane_agent;
GRANT EXECUTE ON FUNCTION lane_dispatch.ld_enqueue(uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real), lane_dispatch.ld_get(uuid),
  lane_dispatch.ld_list_mine(text[], int), lane_dispatch.ld_withdraw(uuid), lane_dispatch.ld_cancel(uuid), lane_dispatch.ld_attach_urls(uuid, int, jsonb),
  lane_dispatch.ld_log_read(uuid, int, int), lane_dispatch.ld_log_ack(uuid, int, int), lane_dispatch.ld_queue_ref(),
  lane_dispatch.ld_enqueue_fenced(text, uuid, text, text, text, jsonb, jsonb, jsonb, text[], real, bigint, int, int, boolean, text, real), lane_dispatch.ld_placeable(text, text[]) TO lane_submit;
GRANT EXECUTE ON FUNCTION lane_dispatch.ld_set_principal(text, text, text, text[], text[], int), lane_dispatch.ld_reap(interval, interval, interval),
  lane_dispatch.ld_restore_reset(), lane_dispatch.ld_guard(interval), lane_dispatch.ld_prune_candidates(interval, interval, timestamptz, uuid, int),
  lane_dispatch.ld_prune(uuid[], interval), lane_dispatch.ld_prune_chunks(interval, int), lane_dispatch.ld_blobs_live(jsonb) TO lane_admin;
