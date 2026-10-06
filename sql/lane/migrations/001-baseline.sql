-- BRAIN-400: the `lane` schema for the unified work queue, baseline v1 (spec rev11 sections 3, 4.2, 5, 11.1).
-- A versioned migration: applyMigrations (src/lane-db.js) runs it once, in its own transaction, and records it in
-- lane.schema_version. A schema change is a NEW numbered file in this directory, never an edit to an applied one.
-- Functions live in ../functions.sql, which is re-applied after every run and also hands ownership to lane_definer
-- and sets every grant, so a partial apply never leaves a role with table access.

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

CREATE TABLE lane.schema_version (version int PRIMARY KEY CHECK (version > 0), applied_at timestamptz NOT NULL DEFAULT now());

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
  budget real NOT NULL DEFAULT 8 CHECK (budget > 0),          -- B: the CPU the host offers the queue; floors and caps are fractions of it
  tags text[] NOT NULL DEFAULT '{}');

-- Five strict bands (spec 5.1): band = rank, and nothing but the overdue rule crosses a class boundary.
-- requires_capability names a boolean column of lane.principals that a submitter must hold for the class.
CREATE TABLE IF NOT EXISTS lane.priority_classes (
  name text PRIMARY KEY, rank smallint NOT NULL UNIQUE, cod_per_min real NOT NULL CHECK (cod_per_min >= 0), requires_capability text);
INSERT INTO lane.priority_classes (name, rank, cod_per_min, requires_capability) VALUES
  ('interactive', 4, 10, 'can_interactive'), ('gate', 3, 4, NULL), ('normal', 2, 2, NULL),
  ('batch', 1, 0.5, 'can_batch'), ('scavenger', 0, 0.05, 'can_scavenge')
  ON CONFLICT DO NOTHING;

-- Tunables and the two flags that stay off until the ETA gate passes (spec 5.5, 13). interactive_max_cores NULL means
-- half the fleet's admission budget (5.3).
CREATE TABLE IF NOT EXISTS lane.config (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  wsjf_enabled boolean NOT NULL DEFAULT false, deadlines_enabled boolean NOT NULL DEFAULT false,
  interactive_max_cores real);
INSERT INTO lane.config (singleton) VALUES (true) ON CONFLICT DO NOTHING;

-- login_role is the Postgres login (session_user); every function derives host/owner/account from it.
CREATE TABLE IF NOT EXISTS lane.principals (
  login_role text PRIMARY KEY,
  host_id text REFERENCES lane.hosts(host_id),
  kind text NOT NULL CHECK (kind IN ('agent','submit','reader','admin')),
  allowed_accounts text[] NOT NULL DEFAULT '{}',
  allowed_repos text[] NOT NULL DEFAULT '{}',
  allowed_dest_hosts text[] NOT NULL DEFAULT '{}',
  max_class text NOT NULL DEFAULT 'gate' REFERENCES lane.priority_classes(name),       -- agents get at most `gate` by default (5.1)
  can_interactive boolean NOT NULL DEFAULT false, can_batch boolean NOT NULL DEFAULT true,
  can_scavenge boolean NOT NULL DEFAULT false, can_deadline boolean NOT NULL DEFAULT false);

-- Speed factors, time multipliers (>1 slower), by host, work class and ref-s bucket (short < 120 s, mid <= 600 s, long); 1 when absent.
CREATE TABLE IF NOT EXISTS lane.host_factors (
  host_id text NOT NULL REFERENCES lane.hosts(host_id), class text NOT NULL, bucket text NOT NULL CHECK (bucket IN ('short','mid','long')),
  factor real NOT NULL CHECK (factor > 0), PRIMARY KEY (host_id, class, bucket));

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

CREATE TABLE IF NOT EXISTS lane.command_templates (
  template_id text NOT NULL, version int NOT NULL,
  revoked boolean NOT NULL DEFAULT false,
  PRIMARY KEY (template_id, version));

-- One scheduling parent per (account, class), derived server-side from the principal's account and the group's class
-- (spec 5.3). Aging, fair share, decayed usage and remaining work live here, so splitting a submission buys nothing.
-- aging_anchor is set by activate_group under lane_sched from the post-lock clock. demoted is the interactive
-- over-limit hysteresis state, rewritten under lane_sched before each selection.
CREATE TABLE IF NOT EXISTS lane.sched_parents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), account text NOT NULL,
  prio_class text NOT NULL REFERENCES lane.priority_classes(name), UNIQUE (account, prio_class),
  aging_anchor timestamptz, last_claim_at timestamptz,
  running_cpu real NOT NULL DEFAULT 0, usage_decayed double precision NOT NULL DEFAULT 0, usage_at timestamptz,   -- core-seconds, half-life 30 min
  demoted boolean NOT NULL DEFAULT false,
  rem_ref jsonb);                                -- remaining ref-s by work class and bucket: {"sim": {"short": 120, "long": 4000}}

-- aging_anchor is the group's own service origin, used only to order siblings inside one parent; it is assigned by
-- activate_group under the lane_sched lock, never from created_at (a late-committing earlier transaction would sort ahead).
CREATE TABLE IF NOT EXISTS lane.groups (
  id uuid PRIMARY KEY, submit_uuid uuid NOT NULL UNIQUE,
  kind text NOT NULL CHECK (kind IN ('gate','experiment','single')), account text NOT NULL, owner text NOT NULL,
  parent_id uuid NOT NULL REFERENCES lane.sched_parents(id),
  prio_class text NOT NULL DEFAULT 'normal' REFERENCES lane.priority_classes(name),
  created_at timestamptz NOT NULL DEFAULT now(),
  aging_anchor timestamptz, last_claim_at timestamptz,
  cancel_requested boolean NOT NULL DEFAULT false,
  snapshot jsonb NOT NULL, aggregator text NOT NULL, expected_jobs int, shard_digest text,
  on_stage_failure text NOT NULL DEFAULT 'skip_later' CHECK (on_stage_failure IN ('skip_later','continue')),
  spec jsonb NOT NULL DEFAULT '{}',
  state text NOT NULL DEFAULT 'open'
    CHECK (state IN ('open','active','aggregating','done','failed','cancelled','held')),
  result jsonb,
  deadline_at timestamptz,                       -- as requested; the scheduler reads only lane.deadline_eff (5.8)
  deadline_valid boolean NOT NULL DEFAULT false, -- judged once at submit and on reclass
  CHECK (state <> 'active' OR aging_anchor IS NOT NULL));

-- The latest frozen forecast per group (spec 5.7); urgency reads only this, never its own output.
CREATE TABLE IF NOT EXISTS lane.group_eta (
  group_id uuid PRIMARY KEY REFERENCES lane.groups(id), eta_p90_s real NOT NULL, low_confidence boolean NOT NULL DEFAULT false);

CREATE TABLE IF NOT EXISTS lane.jobs (
  id bigserial PRIMARY KEY, group_id uuid NOT NULL REFERENCES lane.groups(id), seq bigint NOT NULL,
  idem_key text NOT NULL, UNIQUE (group_id, idem_key), stage smallint NOT NULL DEFAULT 0,
  class text NOT NULL, template_id text NOT NULL, template_version int NOT NULL,
  FOREIGN KEY (template_id, template_version) REFERENCES lane.command_templates(template_id, version),
  params jsonb NOT NULL,
  est_p50_s real NOT NULL, est_p90_s real NOT NULL, est_source text NOT NULL DEFAULT 'default',
  exclusive boolean NOT NULL DEFAULT false,                 -- calibration (5.9): runs alone on its host behind a reservation
  cpu_req real NOT NULL, cpu_min real NOT NULL, mem_bytes bigint NOT NULL,
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
-- Two indexes with DISJOINT predicates, so no lookup has a choice between them and the plan cannot depend on table statistics
-- (PostgreSQL 17 once picked a 200 ms plan on a never-analysed table when two indexes could serve the same lookup).
-- jobs_queued_head serves the head lookup (equality on group, open stage, work class; longest-first order is the index order, so the
-- longest fitting job is the first row that passes the filter) AND the queued half of lane.open_stage (the group's smallest queued stage).
CREATE INDEX IF NOT EXISTS jobs_queued_head ON lane.jobs (group_id, stage, class, est_p50_s DESC, seq) WHERE state = 'queued';
-- jobs_inflight_stage serves the other half of lane.open_stage: the smallest stage among a group's jobs that are unfinished but not queued.
CREATE INDEX IF NOT EXISTS jobs_inflight_stage ON lane.jobs (group_id, stage) WHERE state IN ('claimed','preparing','running','held');
CREATE INDEX IF NOT EXISTS groups_active ON lane.groups (id) WHERE state = 'active';

-- The host exclusion fence (spec 5.9): while a row exists every claim on the host admits only owner_job_id.
-- Release (terminal event of the owner, crash expiry, drain timeout) belongs to the dispatcher and reaper, which are not built yet.
CREATE TABLE IF NOT EXISTS lane.host_reservations (
  host_id text PRIMARY KEY REFERENCES lane.hosts(host_id), owner_job_id bigint NOT NULL REFERENCES lane.jobs(id),
  state text NOT NULL CHECK (state IN ('draining','active')), window_id text NOT NULL, created_at timestamptz NOT NULL,
  drain_deadline timestamptz NOT NULL, active_deadline timestamptz NOT NULL);

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
