# lane_dispatch database setup (cluster level)

`sql/lane/migrations/005-lane-dispatch.sql`, `006-pull-simplify.sql` and `sql/lane/functions.sql` create the `lane_dispatch` schema, its functions and every
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

Two systemd units on pitelite run as a `lane_admin` login (a member of `lane_admin`, so it holds the `pg_signal_backend` and `pg_read_all_stats` grants above). Both are required: the guard is the enforcement behind the per-role `statement_timeout`, which a caller can override.

One `lane-dispatch-reap.timer` fires every 15 s (`AccuracySec=1s`) and runs two separate transactions, one SET per parameter, and the guard runs whether or not the reap succeeded:

```sql
BEGIN; SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '10s'; SELECT lane_dispatch.ld_reap(); COMMIT;
SELECT lane_dispatch.ld_guard('30 seconds');
```

On a reap error the unit rolls back, runs the guard in its own transaction, then exits non-zero.

- `ld_reap(lease_ttl '120 seconds', lost_after '2 hours', amnesty_after '60 seconds')` takes the `cluster` row lock, so two reapers and a restore serialise. A claimed row past its lease returns to `queued` (or settles `cancelled` when a cancel was requested). A running row never returns to the queue on expiry alone: it settles `lost` (reason `runner-lost`) only `lost_after` past its lease. A gap since the last tick longer than `amnesty_after` extends every lease first, so an outage settles nothing. It prunes no `none` rows, because `ld_claim` never writes one.
- Every return to `queued`, by release or lease expiry, increments `requeues` and sets `not_before` to now plus `least(5 s * 2^requeues, 60 s)` (5, 10, 20, 40, 60 s). The sixth return settles the row `lost` with reason `release-cap`.
- `ld_guard('30 seconds')` terminates every `lane_agent_*` and `lane_submit_*` backend whose transaction is older than 30 s, whatever its state, and skips superusers. No claim holds a global lock any more (`ld_claim` locks only its own principal row and the ticket it takes), so a pinned claim delays only its own login.

`ld_guard` is the only `SECURITY INVOKER` function in the schema, because it must run with the admin login's own cluster privileges. `ld_log` accepts at most 256 KiB per call, 64 MiB per attempt and 1 GiB across the table; past a cap it answers `capped`.

Every terminal transition the queue makes itself (restore marking, `ld_restore_reset`, the release cap and the reaper's `lost`) sets `finished_at` and the result `{kind: 'lost', reason, author: 'queue', epoch}`, so retention (`ld_prune_candidates`) selects each of them. The reasons are `queue-restored`, `release-cap` and `runner-lost`.

### Restores

A restore settles every non-terminal ticket `lost` (reason `queue-restored`) and deletes every `claim_requests` row. Runners learn on their next heartbeat (`fenced: true`) and kill their work; the caller resubmits.

- A restore onto a new timeline does this automatically: the first `ld_*` call after it locks the `cluster` row, records the new timeline, bumps the incarnation (so `ld_queue_ref()` moves and fenced enqueues against the old ref are refused) and settles the queue once.
- A restore that keeps the timeline settles nothing by itself. Run `SELECT lane_dispatch.ld_restore_reset();` as `lane_admin`; it does the same settling unconditionally and returns the number of tickets settled.
- Seal before restoring. Seal the node (a confirmed full seal with drain passes, not only a pg_hba reload) before the restore, and keep it sealed until the first `ld_*` call on the new timeline or `ld_restore_reset()` has committed. Only then unseal. Otherwise a runner can reconnect to the restored database before the queue has settled it and heartbeat a row that no longer exists.

Every `ld_*` call must run at READ COMMITTED; any other isolation level is refused (SQLSTATE 25000).

### Runner protocol

`ld_register(version, protocol, schedule)` stores `runners.protocol` and answers `{host, max_claims, protocol_min}`. `ld_claim` answers `{status: 'upgrade', protocol_min}` when the host has no `runners` row or its protocol is below `protocol_min()` (a constant, 1), and writes no `claim_requests` row for it. A ticket's `min_protocol` (an `ld_enqueue` argument) hides it from older runners.

`ld_release` takes the proofs `never-started` and `cancel-confirmed` only. A `claimed` row returns to `queued` (or `cancelled` when a cancel was requested); a `running` row with a cancel requested settles `cancelled`; any other `running` row is left unchanged and the answer is `{refused: 'started'}`. `ld_start` on a running row whose cancel was requested answers false, and a heartbeat answers `cancel: true`.

### Claim order

`ld_claim` takes the ticket with the highest `eff_score(priority, ages, age)`, then the fewest agent principals able to run it (`scarcity`, the count `ld_placeable` reports), then the oldest, then the lowest id (priority is no tie-break: after 60 minutes an aged ticket that ties a high wins on age). `eff_score` ports `src/priority.js` `score()`: the tier is the priority band (7-9 high, 4-6 medium, 0-3 low), weights 2 and 2, age saturating at 20 minutes, and a non-high held at 0.99 of the tier weight until it has waited 60 minutes. Priority 9 (a release candidate) adds 0.01, so it beats every other high and every aged ticket that ties one. A ticket enqueued with `ages = false` credits no wait. `lane_dispatch.now()` is the queue's clock; the GUC `lane.test_now` moves it for tests only, and only when its value is the one stored by `ALTER DATABASE` or `ALTER ROLE` (a session `SET` to any other value is ignored).

## Notification channels

Each hint carries an empty payload, so no ticket id reaches every listener, and a listener treats it as "look now", never as data. `ld_work` means a ticket may have become claimable: `ld_enqueue` on insert, `ld_release` when the row returns to `queued`, and `ld_reap` when it requeued at least one row (a row backing off on `not_before` becomes claimable without a hint, so an agent still polls). `ld_host_<host>` goes to one agent: `ld_cancel` of a claimed or running row, and `ld_attach_urls` when it found its row. `ld_set_principal` refuses a host that does not match `^[a-z0-9][a-z0-9-]{0,54}$` (the runner-name shape, short enough that `ld_host_` plus the host fits Postgres's 63-byte limit), so the channel name is always safe. Hosts may contain hyphens, so the agent must `LISTEN` with a quoted identifier (`client.escapeIdentifier('ld_host_' + host)`); `pg_notify` takes the name as text and needs no quoting.

An agent login holds at most two connections (`CONNECTION LIMIT 3` leaves one spare): one for calls and one that only LISTENs. A lost NOTIFY costs latency, not correctness, because the agent still polls.

## Pruning

Each `ld_reap` tick deletes at most 1000 `claim_requests` rows: claimed mappings created more than 24 hours ago (which keeps the delete from scanning `tickets`) whose ticket is `finished`, `cancelled`, `withdrawn` or `lost` and whose `finished_at` is more than 24 hours old. A mapping whose ticket is not terminal is never pruned, whatever its age. `tickets`, `attempts` and `log_chunks` are not pruned here.

## Pull client surface and retention

Submit logins (the pull client) also hold `ld_queue_ref()` (the queue's `timeline.incarnation`, which moves after a restore), `ld_enqueue_fenced(expected_ref, ...)` (the `ld_enqueue` arguments after a leading ref; a stale ref returns `{state: 'stale-queue-ref', queue_ref}` and inserts nothing, otherwise it calls `ld_enqueue` unchanged) and `ld_placeable(repo_key, requires_tags)` (the number of agent principals with `max_claims > 0` that are eligible for the tags and repo). `ticket_view` carries `attempt_ref`, `sha256('lane-attempt-ref:' || claim_token)`, NULL while the row holds no token; `claim_token` itself never reaches a submitter. `ld_heartbeat` answers only `{fenced, cancel}`.

`ld_cancel` of a queued row stores a server-authored result `{kind: 'cancelled', cancelConfirmed: true, source: 'queued', epoch}`; a cancel of a claimed or running row only sets `cancel_requested`. A replayed `ld_cancel` reports `confirmed` from the stored `cancelConfirmed` (a non-boolean value reads as false), so it never upgrades an unproven cancel. A cancelled row with no stored result (released with `cancel-confirmed`, or a claimed row reaped after a cancel request) reads as confirmed.

Retention is admin-only (`lane_admin` login, run by `lane pull gc`; none of these is granted to agent or submit logins):

- `ld_prune_candidates(objects_after, rows_after, after_finished, after_id, limit)` pages terminal tickets older than `objects_after` in ascending `(finished_at, id)`. The first page passes both cursor arguments NULL; a half-null cursor raises 22023. `limit` is clamped to 1..500. It returns `{items: [{id, finished_at, row_due}], next}`; `next` is the cursor for the following page, or null on a short page.
- `ld_prune(ids, rows_after)` deletes the log chunks, claim_requests mappings, attempts and ticket rows of the listed ids that are terminal and older than `rows_after` (re-checked inside the call); more than 500 ids raises 22023. It returns the number of ticket rows deleted.
- `ld_prune_chunks(terminal_after, limit)` deletes `log_chunks` with no ticket row or whose ticket has been terminal longer than `terminal_after` (default 1 hour), at most `limit` per call (default 10000).
- `ld_enqueue_fenced` refuses (22023) a snapshot whose `blobs` is present but not an array, and `ld_blobs_live` treats a non-array `blobs` as empty. `ld_blobs_live(refs)` takes a JSON array of at most 500 `{ns, sha256}` and returns the subset referenced by a non-terminal ticket's `snapshot.blobs`. A blob is deleted from object storage only when it is old enough and absent from this set.
