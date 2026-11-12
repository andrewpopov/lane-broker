# lane_dispatch database setup (cluster level)

`sql/lane/migrations/005-lane-dispatch.sql` and `sql/lane/functions.sql` create the `lane_dispatch` schema, its functions and every
grant that lives inside the database. `applyMigrations` also calls `applyDatabaseAcls` (`src/lane-db.js`), which revokes
CONNECT, CREATE and TEMP on the current database from PUBLIC and grants CONNECT to the four lane group roles. Design:
`agent_brain/knowledge/lane-pull-dispatch.md` section 2.

Cluster-level privileges need a superuser and stay out of those files, because the applier is a CREATEROLE non-superuser
(`tests/lane-owner-apply.test.js`). Run this once per cluster as a superuser, after the first apply has created the group roles.
Each runner or submitter login is one role per host, named `lane_agent_<host>` or `lane_submit_<name>` (the guard terminates by that prefix).

```sql
GRANT pg_signal_backend, pg_read_all_stats TO lane_admin;

CREATE ROLE lane_agent_<host> LOGIN PASSWORD '<from the secret store>' IN ROLE lane_agent CONNECTION LIMIT 3;
ALTER ROLE lane_agent_<host> SET statement_timeout = '2s';
ALTER ROLE lane_agent_<host> SET idle_in_transaction_session_timeout = '5s';
-- the same three statements for each lane_submit_<name>, IN ROLE lane_submit
```

Then, as a member of `lane_admin`, register each login; tags, repos and `max_claims` are admin-owned and never runner-asserted:

```sql
SELECT lane_dispatch.ld_set_principal('lane_agent_<host>', 'agent', '<host>', '{linux}', '{repoA}', 2);
SELECT lane_dispatch.ld_set_principal('lane_submit_<name>', 'submit', NULL, '{}', '{}', 1);
```

`pg_hba` pins each login to its host: `hostssl lane_queue lane_agent_<host> <host tailnet IP>/32 scram-sha-256`. A login with no
`lane_dispatch.principals` row (for example the legacy `lane_agent_h1`) is refused by every `ld_*` function.

## Timers on the database host

Two systemd timers on pitelite run as a `lane_admin` login (a member of `lane_admin`, so it holds the `pg_signal_backend` and `pg_read_all_stats` grants above). Both are required: the guard is the enforcement behind the per-role `statement_timeout`, which a caller can override.

| Timer | Every | Runs |
|---|---|---|
| `lane-dispatch-reap.timer` | 2 s | `SELECT lane_dispatch.ld_reap();` Defaults: interval 2 s, lease TTL 120 s, `lost_after` 30 min. A gap since the last tick of more than twice the interval extends every lease first (outage amnesty), so an outage settles nothing. |
| `lane-dispatch-guard.timer` | 2 s | `SELECT lane_dispatch.ld_guard();` It terminates every `lane_agent_*` and `lane_submit_*` backend whose transaction is older than 5 s, whatever its state, and skips superusers. A caller that pins `claim_lock` with `BEGIN; SELECT ld_claim(...)` therefore blocks claims for at most one tick plus 5 s. |

If the reap interval changes, pass the new value as the first argument of `ld_reap` (`SELECT lane_dispatch.ld_reap('5 seconds');`), or the amnesty threshold stays at 4 s.

`ld_guard` is the only `SECURITY INVOKER` function in the schema, because it must run with the admin login's own cluster privileges. `ld_log` accepts at most 256 KiB per call, 64 MiB per attempt and 1 GiB across the table; past a cap it answers `capped`.

After a restore from backup the first `ld_*` call quarantines every non-terminal ticket (`state = 'quarantined'`). A quarantined ticket leaves that state only when its runner reasserts it, through a terminal `ld_cancel` by its owner, or through `SELECT lane_dispatch.ld_cancel_quarantined('<ticket id>');` by an operator.
