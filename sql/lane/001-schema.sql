-- BRAIN-400 task 1: the `lane` schema for the unified work queue (spec rev6 sections 3, 4.2, 5, 11.1).
-- Idempotent: every statement is safe to re-apply. Functions live in 002-claim.sql, which also hands
-- ownership to lane_definer and sets every grant, so a partial apply never leaves a role with table access.

-- Roles are cluster-level. lane_definer owns the schema objects and every SECURITY DEFINER function; the
-- four group roles carry grants, and per-host / per-identity login roles are made members of them.
-- The applier need not be a superuser: it must own the database, and have CREATEROLE when a role below does not
-- exist yet. Roles that already exist are never touched. Without CREATEROLE an administrator grants it
-- lane_definer once: GRANT lane_definer TO <applier> WITH SET TRUE. Needs PostgreSQL 16 or later.
DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['lane_definer', 'lane_admin', 'lane_agent', 'lane_submit', 'lane_reader'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', r);
    END IF;
  END LOOP;
  -- Handing ownership to lane_definer needs the applier to be able to SET ROLE to it (a superuser always can).
  IF NOT pg_has_role(session_user, 'lane_definer', 'SET') THEN
    EXECUTE format('GRANT lane_definer TO %I WITH INHERIT TRUE, SET TRUE', session_user);
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS lane;

CREATE TABLE IF NOT EXISTS lane.cluster (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  generation bigint NOT NULL,
  claims_enabled boolean NOT NULL);
INSERT INTO lane.cluster (singleton, generation, claims_enabled) VALUES (true, 1, false) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS lane.hosts (
  host_id text PRIMARY KEY,
  can_compute boolean NOT NULL DEFAULT true,
  state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','draining','disabled','fenced')),
  breaker_until timestamptz,
  tags text[] NOT NULL DEFAULT '{}');

-- login_role is the Postgres login (session_user); every function derives host/owner/account from it.
CREATE TABLE IF NOT EXISTS lane.principals (
  login_role text PRIMARY KEY,
  host_id text REFERENCES lane.hosts(host_id),
  kind text NOT NULL CHECK (kind IN ('agent','submit','reader','admin')),
  allowed_accounts text[] NOT NULL DEFAULT '{}',
  allowed_repos text[] NOT NULL DEFAULT '{}',
  allowed_dest_hosts text[] NOT NULL DEFAULT '{}');

CREATE TABLE IF NOT EXISTS lane.host_class_policy (
  host_id text NOT NULL REFERENCES lane.hosts(host_id), class text NOT NULL,
  weight real NOT NULL DEFAULT 1 CHECK (weight >= 0),
  floor_cpu_pct real, cap_cpu_pct real,
  claim_when_idle_s int NOT NULL DEFAULT 0,
  enabled boolean NOT NULL DEFAULT true,
  PRIMARY KEY (host_id, class));

-- active: the class had eligible work at the last claim on this host. A class that returns from inactive is
-- caught up to the minimum vtime of the active classes (spec 5.2) so an idle class cannot bank a burst.
CREATE TABLE IF NOT EXISTS lane.class_vtime (
  host_id text NOT NULL REFERENCES lane.hosts(host_id), class text NOT NULL,
  vtime double precision NOT NULL DEFAULT 0,
  active boolean NOT NULL DEFAULT false,
  PRIMARY KEY (host_id, class));
ALTER TABLE lane.class_vtime ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS lane.command_templates (
  template_id text NOT NULL, version int NOT NULL,
  revoked boolean NOT NULL DEFAULT false,
  PRIMARY KEY (template_id, version));

-- activated_at is the aging origin: assigned by activate_group under the lane_sched lock from the
-- post-lock clock, never from created_at (a late-committing earlier transaction would sort ahead).
CREATE TABLE IF NOT EXISTS lane.groups (
  id uuid PRIMARY KEY, submit_uuid uuid NOT NULL UNIQUE,
  kind text NOT NULL CHECK (kind IN ('gate','experiment','single')), account text NOT NULL, owner text NOT NULL,
  tier smallint NOT NULL DEFAULT 1 CHECK (tier BETWEEN 0 AND 2),
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz, last_claim_at timestamptz,
  cancel_requested boolean NOT NULL DEFAULT false,
  snapshot jsonb NOT NULL, aggregator text NOT NULL, expected_jobs int, shard_digest text,
  on_stage_failure text NOT NULL DEFAULT 'skip_later' CHECK (on_stage_failure IN ('skip_later','continue')),
  spec jsonb NOT NULL DEFAULT '{}',
  state text NOT NULL DEFAULT 'open'
    CHECK (state IN ('open','active','aggregating','done','failed','cancelled','held')),
  result jsonb, deadline_at timestamptz,
  CHECK (state <> 'active' OR activated_at IS NOT NULL));

CREATE TABLE IF NOT EXISTS lane.jobs (
  id bigserial PRIMARY KEY, group_id uuid NOT NULL REFERENCES lane.groups(id), seq bigint NOT NULL,
  idem_key text NOT NULL, UNIQUE (group_id, idem_key), stage smallint NOT NULL DEFAULT 0,
  class text NOT NULL, template_id text NOT NULL, template_version int NOT NULL,
  FOREIGN KEY (template_id, template_version) REFERENCES lane.command_templates(template_id, version),
  params jsonb NOT NULL,
  est_ref_ms int NOT NULL, cpu_req real NOT NULL, cpu_min real NOT NULL, mem_bytes bigint NOT NULL,
  conflict_keys text[] NOT NULL DEFAULT '{}', conflict_scope text NOT NULL DEFAULT 'host' CHECK (conflict_scope IN ('host','global')),
  dup_safe boolean NOT NULL, host_pin text, host_tags_req text[] NOT NULL DEFAULT '{}',
  state text NOT NULL DEFAULT 'queued' CHECK (state IN
     ('queued','claimed','preparing','running','succeeded','failed','cancelled','skipped','lost','held')),
  epoch int NOT NULL DEFAULT 0, host text, claim_token uuid, lease_until timestamptz,
  eligible_since timestamptz NOT NULL DEFAULT now(), not_before timestamptz NOT NULL DEFAULT now(),
  retries_infra smallint NOT NULL DEFAULT 0, max_infra smallint NOT NULL,
  retries_work smallint NOT NULL DEFAULT 0, max_work smallint NOT NULL,
  grant_cpu real, started_at timestamptz, finished_at timestamptz, exit_code int, permanent boolean, wedged boolean,
  result jsonb, log_ref text, log_tail text);
CREATE INDEX IF NOT EXISTS jobs_queued_head ON lane.jobs (group_id, class, est_ref_ms DESC, seq) WHERE state = 'queued';
-- lane.stage_open asks "is any earlier-stage job of this group still unfinished?" once per candidate job. Without this
-- partial index it scanned every job of the group (about 1,400 buffers, 5 ms per group and class at 60k queued jobs).
CREATE INDEX IF NOT EXISTS jobs_unfinished_stage ON lane.jobs (group_id, stage) WHERE state NOT IN ('succeeded','failed','cancelled','skipped','lost');
-- claim_next's per-group lookup has two indexes that can serve it (this one and jobs_queued_head); only the second
-- avoids evaluating eligibility on every queued job of the group, and the planner picks it only from fresh statistics.
-- PostgreSQL 17 chose the wrong one (about 200 ms per claim) on a 400-job table nobody had analysed yet. Keep the
-- statistics fresh on this hot table instead of hoping.
ALTER TABLE lane.jobs SET (autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 200, autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX IF NOT EXISTS groups_active ON lane.groups (id) WHERE state = 'active';

CREATE TABLE IF NOT EXISTS lane.exclusions (
  key text NOT NULL, job_id bigint NOT NULL, epoch int NOT NULL, host text NOT NULL,
  PRIMARY KEY (key, job_id, epoch));

CREATE TABLE IF NOT EXISTS lane.job_events (
  id bigserial PRIMARY KEY, job_id bigint NOT NULL, epoch int NOT NULL, kind text NOT NULL,
  host text, at timestamptz NOT NULL, detail jsonb NOT NULL DEFAULT '{}');

-- Outbox tables (spec 11.1). Producers insert as their LAST statement; consumers ack as theirs.
CREATE TABLE IF NOT EXISTS lane.transition_events (
  event_id bigserial PRIMARY KEY, job_id bigint NOT NULL REFERENCES lane.jobs(id), epoch int NOT NULL,
  seq bigint NOT NULL,
  kind text NOT NULL CHECK (kind IN ('claimed','started','succeeded','failed_final','lost','cancelled','released')),
  payload jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (job_id, seq));
CREATE TABLE IF NOT EXISTS lane.hook_outbox (
  event_id bigint NOT NULL REFERENCES lane.transition_events(event_id), consumer text NOT NULL,
  applied_at timestamptz, outcome text,
  PRIMARY KEY (event_id, consumer));
CREATE TABLE IF NOT EXISTS lane.report_outbox (
  event_id bigserial PRIMARY KEY, experiment_id text NOT NULL, generation int NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now(),
  applied_at timestamptz, outcome text,
  UNIQUE (experiment_id, generation));

CREATE OR REPLACE FUNCTION lane.reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% on lane.% is not allowed', TG_OP, TG_TABLE_NAME USING ERRCODE = 'LN002';
END $$;
DROP TRIGGER IF EXISTS transition_events_immutable ON lane.transition_events;
CREATE TRIGGER transition_events_immutable BEFORE UPDATE OR DELETE ON lane.transition_events
  FOR EACH ROW EXECUTE FUNCTION lane.reject_mutation();

CREATE OR REPLACE FUNCTION lane.reject_params_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.params IS DISTINCT FROM OLD.params THEN
    RAISE EXCEPTION 'lane.jobs.params is immutable' USING ERRCODE = 'LN002';
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS jobs_params_immutable ON lane.jobs;
CREATE TRIGGER jobs_params_immutable BEFORE UPDATE OF params ON lane.jobs
  FOR EACH ROW EXECUTE FUNCTION lane.reject_params_change();

-- Read-only summary for lane_reader (a view runs as its owner, so the reader needs no table grant).
CREATE OR REPLACE VIEW lane.job_state_counts AS
  SELECT class, state, count(*) AS jobs FROM lane.jobs GROUP BY class, state;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typnamespace = 'lane'::regnamespace AND typname = 'claim_result') THEN
    CREATE TYPE lane.claim_result AS (
      job_id bigint, group_id uuid, epoch int, claim_token uuid, lease_until timestamptz,
      grant_cpu real, class text, template_id text, template_version int, params jsonb);
  END IF;
END $$;
