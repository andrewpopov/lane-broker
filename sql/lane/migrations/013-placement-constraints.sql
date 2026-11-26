-- PKG-439: per-ticket placement constraints, enforced at claim time. A runner's platform is admin-set (lane_dispatch.ld_set_platform), never runner-reported; a ticket may name a capability
-- class (`<platform>[+tag...]`, its tags are also in requires_tags), hosts it must never run on (avoid_hosts) and the one host it must run on (target_host). Host ids are canonical
-- (lower-case runner ids), resolved by the client before the call. No backfill: every existing row is unconstrained, and a principal with no platform simply never claims a ticket that names one.
-- No roles are touched, so a NOCREATEROLE migrator applies this.
ALTER TABLE lane_dispatch.principals
  ADD COLUMN platform text CHECK (platform IS NULL OR platform IN ('darwin', 'linux', 'win32'));

ALTER TABLE lane_dispatch.tickets
  ADD COLUMN capability_class text CHECK (capability_class IS NULL OR capability_class ~ '^(darwin|linux|win32)(\+[a-z0-9][a-z0-9-]{0,62})*$'),
  ADD COLUMN class_platform text CHECK (class_platform IS NULL OR class_platform IN ('darwin', 'linux', 'win32')),
  ADD COLUMN avoid_hosts text[] NOT NULL DEFAULT '{}',
  ADD COLUMN target_host text;
