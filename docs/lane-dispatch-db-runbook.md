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
