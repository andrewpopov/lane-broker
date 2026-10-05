-- BRAIN-400 task 1: claim path (spec rev6 section 5.5). Lock order: (1) lane_sched, (3) hosts, (4) groups,
-- (5) jobs, then outbox rows. Candidates are chosen with plain reads; the winner is locked group-then-job and
-- re-validated; a failed re-validation returns no claim. Every function derives the caller from session_user.

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

-- A stage opens once every job of an earlier stage in the group is terminal.
CREATE OR REPLACE FUNCTION lane.stage_open(p_group uuid, p_stage smallint) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT NOT EXISTS (SELECT 1 FROM lane.jobs e WHERE e.group_id = p_group AND e.stage < p_stage
                      AND e.state NOT IN ('succeeded','failed','cancelled','skipped','lost'))
$$;

-- Job-level eligibility on one host. The single definition used both to choose candidates and to re-validate.
CREATE OR REPLACE FUNCTION lane.job_eligible(j lane.jobs, h lane.hosts, p_room jsonb, p_mem bigint, p_held_keys text[], p_now timestamptz)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog, lane, pg_temp AS $$
  SELECT j.state = 'queued' AND j.not_before <= p_now AND lane.stage_open(j.group_id, j.stage)
     AND (j.host_pin IS NULL OR j.host_pin = h.host_id) AND j.host_tags_req <@ h.tags
     AND j.cpu_min <= coalesce((p_room ->> j.class)::real, 0) AND j.mem_bytes <= p_mem
     AND NOT (j.conflict_scope = 'host' AND j.conflict_keys && p_held_keys)
     AND NOT (j.conflict_scope = 'global' AND EXISTS (SELECT 1 FROM lane.exclusions x WHERE x.key = ANY (j.conflict_keys)))
     AND lane.template_ok(j.template_id, j.template_version)
$$;

-- Re-validation under the group and job locks. VOLATILE on purpose: each inner statement takes a fresh snapshot.
CREATE OR REPLACE FUNCTION lane.revalidate_claim(j lane.jobs, p_gid uuid, h lane.hosts, p_room jsonb, p_mem bigint, p_held_keys text[], p_now timestamptz)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE g lane.groups;
BEGIN
  SELECT * INTO g FROM lane.groups WHERE id = p_gid;
  RETURN FOUND AND j.group_id = g.id AND g.state = 'active' AND NOT g.cancel_requested
     AND lane.dest_ok(g.owner, h.host_id) AND lane.job_eligible(j, h, p_room, p_mem, p_held_keys, p_now);
END $$;

-- p_room: per-class CPU local admission accepts now, e.g. {"test":3.0,"sim":0}.
CREATE OR REPLACE FUNCTION lane.claim_next(p_gen bigint, p_room jsonb, p_mem bigint, p_held_keys text[], p_token uuid)
RETURNS SETOF lane.claim_result LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE
  v_me lane.principals; v_host lane.hosts; v_pick record; v_job lane.jobs; v_now timestamptz;
  v_grant real; v_seq bigint;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1) first
  v_now := clock_timestamp();                                                  -- the ONE clock, read after the lock
  v_me := lane.principal_of('agent');
  PERFORM lane.require_generation(p_gen);
  SELECT * INTO v_host FROM lane.hosts WHERE host_id = v_me.host_id FOR UPDATE;   -- (3)
  IF NOT FOUND OR NOT v_host.can_compute OR v_host.state <> 'active'
     OR coalesce(v_host.breaker_until, '-infinity') > v_now
     OR NOT (SELECT claims_enabled FROM lane.cluster) THEN RETURN; END IF;

  WITH pol AS (
    SELECT c.class, c.weight, coalesce(v.vtime, 0) AS vtime
      FROM lane.host_class_policy c
      LEFT JOIN lane.class_vtime v ON v.host_id = c.host_id AND v.class = c.class
     WHERE c.host_id = v_host.host_id AND c.enabled AND c.weight > 0
       AND coalesce((p_room ->> c.class)::real, 0) > 0),
  cand AS (                       -- one row per (group, class): that group's best eligible head job; no locks
    SELECT g.id AS gid, g.last_claim_at, pol.class, pol.weight, pol.vtime, j.id AS job_id, j.est_ref_ms, j.seq,
           coalesce(g.last_claim_at, g.activated_at) AS since,
           (v_now - coalesce(g.last_claim_at, g.activated_at) >= interval '20 minutes') AS overdue,
           least(2, g.tier + floor(extract(epoch FROM v_now - coalesce(g.last_claim_at, g.activated_at)) / 600))::int AS band
      FROM pol
      JOIN lane.groups g ON g.state = 'active' AND NOT g.cancel_requested AND lane.dest_ok(g.owner, v_host.host_id)
      JOIN LATERAL (SELECT j.* FROM lane.jobs j
                     WHERE j.group_id = g.id AND j.class = pol.class AND j.state = 'queued'
                       AND lane.job_eligible(j, v_host, p_room, p_mem, p_held_keys, v_now)
                     ORDER BY j.est_ref_ms DESC, j.seq LIMIT 1) j ON true)
  SELECT * INTO v_pick FROM cand
   ORDER BY overdue DESC,
            CASE WHEN overdue THEN since END ASC, CASE WHEN overdue THEN gid END ASC,   -- 1. overdue: total order (since, gid)
            band DESC,                                  -- 2. else the highest eligible band
            round(vtime::numeric, 9) ASC,               -- 3. class stride (rounded: float noise must not break true ties)
            last_claim_at NULLS FIRST,                  -- 4. group rotation
            est_ref_ms DESC, seq,                       -- 5. longest first, then arrival
            gid                                         -- 6. deterministic fallback LAST
   LIMIT 1;
  IF NOT FOUND THEN RETURN; END IF;

  PERFORM 1 FROM lane.groups WHERE id = v_pick.gid FOR UPDATE;                 -- (4)
  SELECT * INTO v_job FROM lane.jobs WHERE id = v_pick.job_id FOR UPDATE;      -- (5)
  IF NOT FOUND OR NOT lane.revalidate_claim(v_job, v_pick.gid, v_host, p_room, p_mem, p_held_keys, v_now) THEN
    RETURN;                                                                    -- no loop, no re-lock
  END IF;

  v_grant := least(v_job.cpu_req, (p_room ->> v_job.class)::real);
  UPDATE lane.jobs SET state = 'claimed', epoch = epoch + 1, claim_token = p_token, host = v_host.host_id,
         lease_until = v_now + interval '30 seconds', grant_cpu = v_grant
   WHERE id = v_job.id RETURNING * INTO v_job;
  IF v_job.conflict_scope = 'global' THEN
    INSERT INTO lane.exclusions (key, job_id, epoch, host)
      SELECT DISTINCT k, v_job.id, v_job.epoch, v_host.host_id FROM unnest(v_job.conflict_keys) AS k;
  END IF;
  UPDATE lane.groups SET last_claim_at = v_now WHERE id = v_pick.gid;
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

-- p_jobs: JSON array of {idem_key, stage, class, template_id, template_version, params, est_ref_ms, cpu_req,
-- cpu_min, mem_bytes, conflict_keys, conflict_scope, dup_safe, host_pin, host_tags_req, max_infra, max_work}.
-- No scheduling lock: the group is inserted `open` and only activate_group makes it visible.
CREATE OR REPLACE FUNCTION lane.submit_group(p_submit_uuid uuid, p_kind text, p_account text, p_tier smallint,
                                             p_snapshot jsonb, p_aggregator text, p_jobs jsonb)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_me lane.principals; v_gid uuid;
BEGIN
  v_me := lane.principal_of('submit');
  IF NOT (p_account = ANY (v_me.allowed_accounts)) THEN
    RAISE EXCEPTION 'account % not allowed for %', p_account, session_user USING ERRCODE = '42501';
  END IF;
  INSERT INTO lane.groups (id, submit_uuid, kind, account, owner, tier, snapshot, aggregator)
    VALUES (gen_random_uuid(), p_submit_uuid, p_kind, p_account, session_user, p_tier, p_snapshot, p_aggregator)
    ON CONFLICT (submit_uuid) DO NOTHING RETURNING id INTO v_gid;
  IF v_gid IS NULL THEN
    SELECT id INTO v_gid FROM lane.groups WHERE submit_uuid = p_submit_uuid AND owner = session_user;
    IF v_gid IS NULL THEN RAISE EXCEPTION 'submit_uuid belongs to another principal' USING ERRCODE = '42501'; END IF;
    RETURN v_gid;
  END IF;
  INSERT INTO lane.jobs (group_id, seq, idem_key, stage, class, template_id, template_version, params, est_ref_ms,
                         cpu_req, cpu_min, mem_bytes, conflict_keys, conflict_scope, dup_safe, host_pin,
                         host_tags_req, max_infra, max_work)
    SELECT v_gid, r.ord, r.idem_key, coalesce(r.stage, 0), r.class, r.template_id, r.template_version,
           coalesce(r.params, '{}'), r.est_ref_ms, r.cpu_req, coalesce(r.cpu_min, r.cpu_req), coalesce(r.mem_bytes, 0),
           coalesce(r.conflict_keys, '{}'), coalesce(r.conflict_scope, 'host'), coalesce(r.dup_safe, false), r.host_pin,
           coalesce(r.host_tags_req, '{}'), coalesce(r.max_infra, 3), coalesce(r.max_work, 2)
      FROM (SELECT x.*, row_number() OVER () AS ord
              FROM jsonb_to_recordset(p_jobs) AS x(idem_key text, stage smallint, class text, template_id text,
                   template_version int, params jsonb, est_ref_ms int, cpu_req real, cpu_min real, mem_bytes bigint,
                   conflict_keys text[], conflict_scope text, dup_safe boolean, host_pin text, host_tags_req text[],
                   max_infra smallint, max_work smallint)) r;
  RETURN v_gid;
END $$;

-- Serialized activation: the aging origin is the post-lock clock, so no earlier-committing transaction
-- can ever sort ahead of a group that is already active.
CREATE OR REPLACE FUNCTION lane.activate_group(p_group uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lane, pg_temp AS $$
DECLARE v_now timestamptz; v_owner text;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('lane_sched'));                       -- (1)
  v_now := clock_timestamp();
  SELECT owner INTO v_owner FROM lane.groups WHERE id = p_group;
  IF v_owner IS NULL THEN RETURN false; END IF;
  IF v_owner <> session_user AND NOT EXISTS (SELECT 1 FROM lane.principals WHERE login_role = session_user AND kind = 'admin') THEN
    RAISE EXCEPTION 'not the group owner' USING ERRCODE = '42501';
  END IF;
  UPDATE lane.groups SET state = 'active', activated_at = v_now WHERE id = p_group AND state = 'open';   -- (4)
  RETURN FOUND;
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
GRANT EXECUTE ON FUNCTION lane.claim_next(bigint, jsonb, bigint, text[], uuid) TO lane_agent, lane_admin;
GRANT EXECUTE ON FUNCTION lane.submit_group(uuid, text, text, smallint, jsonb, text, jsonb) TO lane_submit, lane_admin;
GRANT EXECUTE ON FUNCTION lane.activate_group(uuid) TO lane_submit, lane_admin;
