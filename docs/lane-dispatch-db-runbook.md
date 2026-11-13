# lane_dispatch database setup (cluster level)

`sql/lane/migrations/005-lane-dispatch.sql` and `sql/lane/functions.sql` create the `lane_dispatch` schema, its functions and every
grant that lives inside the database. `applyDatabaseAcls` (`src/lane-db.js`) revokes CONNECT, CREATE and TEMP on the current
database from PUBLIC and grants CONNECT to the four lane group roles. It is opt-in (`applyMigrations(client, { databaseAcls: true })`
or a direct call), and it throws before revoking anything when the database holds a schema other than `lane`, `lane_dispatch`
and `public`, or any object in `public`, so it refuses a shared application database. Design:
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

A submit login that runs the pull client (`lane_submit_train`, the shared supervisor identity) is created with `CONNECTION LIMIT 24` instead of 3, because every concurrent `lane` supervisor on the host holds its own connection (approved 2026-11-12). The other submit logins keep 3.

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

After a restore from backup the first `ld_*` call quarantines every non-terminal ticket (`state = 'quarantined'`). A quarantined ticket leaves that state only when its runner reasserts it as `running` with the same host, epoch and token, through `ld_cancel` by its owner, or through `SELECT lane_dispatch.ld_cancel_quarantined('<ticket id>');` by an operator. A heartbeat and `ld_finish` never lift quarantine: each answers `reassert: true` (finish adds `state: 'quarantined'`) and changes nothing, so the runner reasserts, and only a `running` reassert lifts it. A reassert of a quarantined claim as `pre-start`, `ld_start` and `ld_release` are fenced or refused, and a reassert from another host, epoch or token is always fenced (the one exception is an ownerless rolled-back row, which an eligible runner may adopt as `running` at a higher epoch).

Every `ld_*` call must run at READ COMMITTED; any other isolation level is refused (SQLSTATE 25000).

A reassert of a row that is not quarantined never changes its state: it extends the lease and answers `{result: 'ok', state}` with the row's current state, so a `pre-start` reassert of a `running` row leaves it `running`.

## Notification channels

Each hint carries an empty payload, so no ticket id reaches every listener, and a listener treats it as "look now", never as data. `ld_work` means a ticket may have become claimable: `ld_enqueue` on insert, `ld_release` when the row returns to `queued`, and `ld_reap` when it requeued at least one row. `ld_host_<host>` goes to one agent: `ld_cancel` of a claimed or running row, and `ld_attach_urls` when it found its row. `ld_set_principal` refuses a host that does not match `^[a-z0-9][a-z0-9-]{0,54}$` (the runner-name shape, short enough that `ld_host_` plus the host fits Postgres's 63-byte limit), so the channel name is always safe. Hosts may contain hyphens, so the agent must `LISTEN` with a quoted identifier (`client.escapeIdentifier('ld_host_' + host)`); `pg_notify` takes the name as text and needs no quoting.

An agent login holds at most two connections (`CONNECTION LIMIT 3` leaves one spare): one for calls and one that only LISTENs. A lost NOTIFY costs latency, not correctness, because the agent still polls.

## Pruning

Each `ld_reap` tick deletes at most 1000 `claim_requests` rows in total. Rows with no ticket (`ticket IS NULL`) older than 10 minutes go first; the remaining budget removes claimed mappings created more than 24 hours ago (which keeps the delete from scanning `tickets`) whose ticket is `finished`, `cancelled`, `withdrawn` or `lost` and whose `finished_at` is more than 24 hours old. A mapping whose ticket is not terminal is never pruned, whatever its age. `tickets`, `attempts` and `log_chunks` are not pruned here.

## Pull client surface and retention

Submit logins (the pull client) also hold `ld_queue_ref()` (the queue's `timeline.incarnation`, which moves after a restore), `ld_enqueue_fenced(expected_ref, ...)` (the `ld_enqueue` arguments after a leading ref; a stale ref returns `{state: 'stale-queue-ref', queue_ref}` and inserts nothing, otherwise it calls `ld_enqueue` unchanged) and `ld_placeable(repo_key, requires_tags)` (the number of agent principals with `max_claims > 0` that are eligible for the tags and repo). `ticket_view` carries `attempt_ref`, `sha256('lane-attempt-ref:' || claim_token)`, NULL while the row holds no token; `claim_token` itself never reaches a submitter. `ld_heartbeat` returns `urls` only when `urls->>'epoch'` equals the row's epoch, so a reclaimed agent sees `urls: null` until the owner attaches URLs for the new epoch.

`ld_cancel` and `ld_cancel_quarantined` store a server-authored result `{kind: 'cancelled', cancelConfirmed, source, epoch}`. `source` is `queued` (confirmed), `quarantine` (owner cancel of a quarantined row, unconfirmed) or `operator` (unconfirmed). A replayed `ld_cancel` reports `confirmed` from the stored `cancelConfirmed`, so it never upgrades an unproven cancel.

Retention is admin-only (`lane_admin` login, run by `lane pull gc`; none of these is granted to agent or submit logins):

- `ld_prune_candidates(objects_after, rows_after, after_finished, after_id, limit)` pages terminal tickets older than `objects_after` in ascending `(finished_at, id)`. The first page passes both cursor arguments NULL; a half-null cursor raises 22023. `limit` is clamped to 1..500. It returns `{items: [{id, finished_at, row_due}], next}`; `next` is the cursor for the following page, or null on a short page.
- `ld_prune(ids, rows_after)` deletes the log chunks, attempts and ticket rows of the listed ids that are terminal and older than `rows_after` (re-checked inside the call); more than 500 ids raises 22023. It returns the number of ticket rows deleted.
- `ld_prune_chunks(terminal_after, limit)` deletes `log_chunks` with no ticket row or whose ticket has been terminal longer than `terminal_after` (default 1 hour), at most `limit` per call (default 10000).
- `ld_blobs_live(refs)` takes a JSON array of at most 500 `{ns, sha256}` and returns the subset referenced by a non-terminal ticket's `snapshot.blobs`. A blob is deleted from object storage only when it is old enough and absent from this set.
