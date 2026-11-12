-- BRAIN-930 S1: schema lane_dispatch (knowledge/lane-pull-dispatch.md section 2). Callers hold no table privilege; ownership and grants are in ../functions.sql.

CREATE SCHEMA IF NOT EXISTS lane_dispatch;

-- tags, allowed_repos and max_claims are admin-owned (ld_set_principal), never runner-asserted.
CREATE TABLE lane_dispatch.principals (
  login_role text PRIMARY KEY,
  host text,
  kind text NOT NULL CHECK (kind IN ('agent','submit','admin')),
  tags text[] NOT NULL DEFAULT '{}',
  allowed_repos text[] NOT NULL DEFAULT '{}',
  max_claims int NOT NULL DEFAULT 1 CHECK (max_claims >= 0),
  CONSTRAINT principals_agent_has_host CHECK ((kind = 'agent') = (host IS NOT NULL)));
CREATE UNIQUE INDEX principals_agent_host ON lane_dispatch.principals (host) WHERE kind = 'agent';

CREATE TABLE lane_dispatch.claim_lock (id int PRIMARY KEY CHECK (id = 1));
INSERT INTO lane_dispatch.claim_lock (id) VALUES (1);

CREATE TABLE lane_dispatch.cluster (
  id int PRIMARY KEY CHECK (id = 1),
  timeline int NOT NULL,
  incarnation int NOT NULL DEFAULT 0,
  last_tick timestamptz NOT NULL DEFAULT now());
INSERT INTO lane_dispatch.cluster (id, timeline) VALUES (1, (SELECT timeline_id FROM pg_control_checkpoint()));

CREATE TABLE lane_dispatch.tickets (
  id uuid PRIMARY KEY,
  owner text NOT NULL,
  repo_key text NOT NULL,
  lane text NOT NULL,
  lane_key text NOT NULL,
  binding jsonb NOT NULL,
  header jsonb NOT NULL,
  snapshot jsonb NOT NULL,
  requires_tags text[] NOT NULL DEFAULT '{}',
  cpu_cores real NOT NULL CHECK (cpu_cores >= 0),
  memory_bytes bigint NOT NULL CHECK (memory_bytes >= 0),
  priority smallint NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 9),
  state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','claimed','running','quarantined','finished','cancelled','withdrawn','lost')),
  incarnation int NOT NULL,
  epoch int NOT NULL DEFAULT 0,
  claim_token uuid,
  host text,
  lease_until timestamptz,
  cancel_requested boolean NOT NULL DEFAULT false,
  urls jsonb,
  result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz,
  CONSTRAINT tickets_held_has_owner CHECK (state NOT IN ('claimed','running') OR (claim_token IS NOT NULL AND host IS NOT NULL AND lease_until IS NOT NULL)));
CREATE INDEX tickets_queued ON lane_dispatch.tickets (priority DESC, created_at, id) WHERE state = 'queued';
CREATE INDEX tickets_held ON lane_dispatch.tickets (host) WHERE state IN ('claimed','running');
CREATE INDEX tickets_lease ON lane_dispatch.tickets (lease_until) WHERE state IN ('claimed','running');
CREATE INDEX tickets_owner ON lane_dispatch.tickets (owner, created_at DESC);

CREATE TABLE lane_dispatch.attempts (
  ticket uuid NOT NULL REFERENCES lane_dispatch.tickets (id) ON DELETE CASCADE,
  epoch int NOT NULL,
  host text NOT NULL,
  claim_token uuid NOT NULL,
  log_bytes bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (ticket, epoch));

-- ticket is NULL for a claim that found nothing, so a replayed req gets the same "no claim".
CREATE TABLE lane_dispatch.claim_requests (
  login_role text NOT NULL,
  req uuid NOT NULL,
  ticket uuid,
  epoch int,
  claim_token uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (login_role, req));

CREATE TABLE lane_dispatch.runners (
  host text PRIMARY KEY,
  version text,
  schedule jsonb,
  last_seen timestamptz NOT NULL DEFAULT now());

CREATE UNLOGGED TABLE lane_dispatch.log_chunks (
  ticket uuid NOT NULL,
  epoch int NOT NULL,
  seq int NOT NULL,
  bytes bytea NOT NULL,
  PRIMARY KEY (ticket, epoch, seq));
