# lane_dispatch database setup (cluster level)

`sql/lane/migrations/005-lane-dispatch.sql`, `006-pull-simplify.sql`, `007-avoid-tags.sql`, `008-requires.sql`, `009-elastic-head-reserve.sql`, `010-claim-weight.sql`, `011-ticket-status.sql` and `sql/lane/functions.sql` create the `lane_dispatch` schema, its functions and every
grant that lives inside the database. `applyDatabaseAcls` (`src/lane-db.js`) revokes CONNECT, CREATE and TEMP on the current
database from PUBLIC and grants CONNECT to the lane group roles (`lane_admin`, `lane_agent`, `lane_submit`, `lane_reader`, `train_reader`). It is opt-in (`applyMigrations(client, { databaseAcls: true })`
or a direct call), and it throws before revoking anything when the database holds a schema other than `lane`, `lane_dispatch`
and `public`, or any object in `public`, so it refuses a shared application database. Design:
`agent_brain/knowledge/lane-pull-dispatch.md` section 2.

## Applying migrations

On pitelite the migrations are applied by `lane_migrate`, never by `postgres` or `zirk` (BRAIN-1146). It owns nothing: it is
`LOGIN NOSUPERUSER NOCREATEROLE`, a member of `lane_definer` with INHERIT and SET, and holds CONNECT and CREATE on `lane_queue_db`.
CREATE is required because `functions.sql`'s `CREATE SCHEMA IF NOT EXISTS lane_dispatch` checks it even when the schema exists
(`tests/lane-owner-apply.test.js`, the BRAIN-1146 case). Created once by a superuser:

```sql
CREATE ROLE lane_migrate LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2;
GRANT lane_definer TO lane_migrate WITH INHERIT TRUE, SET TRUE;
GRANT CONNECT, CREATE ON DATABASE lane_queue_db TO lane_migrate;
```

It has no password. It logs in only as OS `postgres` over pitelite's socket: `local lane_queue_db lane_migrate peer map=lanequeue`,
rendered from zirkbot `ops/pitelite/failover/cluster.toml` (consumer `lane-migrate`) and `fence.py` `PG_IDENT_ROWS`. Apply from a
lane-broker checkout of the version being deployed, with its dependencies installed:

```sh
sudo -u postgres node --input-type=module -e "
import { connect, applyMigrations } from './src/lane-db.js';
const c = await connect({ host: '/var/run/postgresql', user: 'lane_migrate', database: 'lane_queue_db', options: '-c lock_timeout=5s' });
try { console.log(JSON.stringify(await applyMigrations(c))); } finally { await c.end(); }"
```

It prints `{"version":N,"applied":[...]}`. A run with nothing new prints `"applied":[]` and only re-applies `functions.sql`.

## Cluster-level privileges

Cluster-level privileges need a superuser and stay out of those files, because the applier is not a superuser
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

`pg_hba` pins each login to its host: `hostssl lane_queue_db lane_agent_<host> <host tailnet IP>/32 scram-sha-256`. A login with no
`lane_dispatch.principals` row (for example the legacy `lane_agent_h1`) is refused by every `ld_*` function.

## Adding and removing a pull runner

`ops/runner/lane-runner-admin.js` (logic in `ops/runner/runner-admin-lib.js`) registers a pull runner on pitelite from the admin's Mac. It reaches pitelite with `ssh pitelite sudo -n` and runs `mc` and `op-bot` locally. The default is a dry run that prints every action (the SQL with the password as `'<redacted>'`, a unified diff of each file it would edit); `--apply` performs the steps in order and stops at the first failure, printing what succeeded and the command to resume. Every step converges, so re-running `add` after a partial failure finishes the job.

```sh
node ops/runner/lane-runner-admin.js add <host> --ip <tailnet IPv4> [--tags a,b] [--repos '*'|a,b] [--max-claims N] [--platform darwin|linux|win32] [--out <dir>] [--note <text>] [--apply]
node ops/runner/lane-runner-admin.js set-platform <host> darwin|linux|win32|none [--apply]
node ops/runner/lane-runner-admin.js remove <host> [--force] [--apply]
node ops/runner/lane-runner-admin.js status <host>
```

`<host>` matches `^[a-z][a-z0-9-]{0,30}$` and `--ip` is inside 100.64.0.0/10. The Postgres role is `lane_agent_<host with - as _>`; the RustFS policy is `lane-runner-<host>`. Run the dry run first, read it, then add `--apply`. **Only one admin should run it at a time.** An `--apply` run holds a per-host lockfile `~/.cache/lane-runner-admin/<host>.lock` (pid and start time; reclaimed only when that pid is dead), which serializes runs on one machine but not across machines. So the `hba`, `rustfs` and `train` steps of `add` re-check that the role and principal row still exist ("the role was removed while this add ran"), and the 1Password step re-lists after creating and fails naming both ids if two items share the title. The resume command it prints is `node ops/runner/lane-runner-admin.js …`.

`add` changes, in order:

1. **1Password.** Item `lane-agent <host>` in vault `BotAccess` (via `op-bot`): the role password (32 random bytes, base64url) and the RustFS `rustfs_access_key` / `rustfs_secret_key`. An existing item is read back, never regenerated. Secrets reach `op-bot` through a template file in a 0700 temp dir, removed afterwards.
2. **Postgres.** `CREATE ROLE ... IN ROLE lane_agent CONNECTION LIMIT 3` (an existing role gets `ALTER ROLE ... LOGIN PASSWORD ... CONNECTION LIMIT 3` instead, which also undoes the `NOLOGIN` an interrupted `remove` leaves), the two per-role timeouts, and `ld_set_principal`. The SQL, password included, goes to `psql` on stdin as one `BEGIN … COMMIT`; a failure reports only the exit code and SQLSTATE, never psql's output, which would echo the password.
3. **`pg_hba.conf`.** A `# BEGIN/END lane-runner-admin <host>` block with the `hostssl ... <ip>/32 scram-sha-256` line goes before `local lane_queue_db all reject`, and the role joins the two catch-all reject lists. Each file edit is one script run on pitelite under `flock /run/lock/lane-runner-admin.lock`: it refuses (exit 3) if the file's sha256 is no longer the one the plan was computed from, stages the new content beside the file with the live owner and mode, copies a backup to `<file>.bak-lane-runner-admin-<UTC stamp>`, moves the stage over the file, asks Postgres's own parse of the files on disk (`pg_hba_file_rules`, so includes, quoting and continuations are resolved at commit time, under the lock) whether the result is safe, restores the backup on any failure, and only then reloads, requiring `SELECT pg_reload_conf()` to answer exactly `t`: otherwise the validated file stays, the script exits non-zero ("reload failed: the file is valid but not live; re-run to reload") and a re-run reloads. An unchanged hba is still checked and reloaded, so a run that died before the reload converges. `--note` is limited to `[A-Za-z0-9 .,:;()~#+_/-]` (120 characters) because a backslash would continue the pg_hba comment over the rule, and `add` refuses when a rule outside its block may already let the role in: a non-reject rule whose database field has `lane_queue_db`, `all`, `sameuser`, `samerole`, a `/regex` or an `@file`, and whose user field has the role, `all`, any `+group`, a `/regex` or an `@file` (regexes, files and groups are refused, not evaluated). That text check, which also refuses a line ending in a backslash and any non-comment line with a double quote, is only the dry-run early warning; the authoritative check is the invariant the edit script runs against `pg_hba_file_rules` after the move. First it refuses any non-comment line containing a double quote in every file the view read (the view cannot tell a quoted keyword from the keyword). Then, for role R and the TCP and `local` rule families separately, it walks the rules that may match R (user R, `all`, any `+group` or `/regex`, in any database) in order, up to the first definite barrier: a `reject` whose database list has `all` and whose user list has R or `all` (for TCP also `host` with address `all`). There must be a barrier. Before it every rule must be a `reject` or, for TCP, R's own `hostssl <ip>/32 scram-sha-256` line, which must appear exactly once and with no `reject` before it whose database may match `lane_queue_db` (`lane_queue_db`, `all`, `sameuser`, `samerole`, `samegroup`, `/regex`). Any other rule fails, naming `file:line`. `remove` checks only that no rule has an error and no non-reject rule names the role literally (plus the quote check).
4. **RustFS.** `mc admin user add`, a policy `lane-runner-<host>` (read `lane-snap/*`, write `lane-out/<host>/*`, read `lane-out/<host>/.probe`) and its attachment. The root credentials come from pitelite's `rustfs.env` into the `mc` child's `MC_HOST_pl` only. `mc admin user add` has no stdin form, so the runner's secret key is briefly in `mc`'s argv on the admin's Mac.
5. **Train submitter config.** `{"name":"<host>","transport":"pull"}` is added to `.runners` in `/data/train/config/lane-broker/config.json` (backed up, written as user `train`, parse-checked). **`agent_brain/ops/pitelite-controller/provision.sh` writes this file**, so a re-provision drops the runner unless it is added there too.
6. **Worker files** (with `--out <dir>`, mode 0600 in a 0700 dir): `config.json`, `password`, `rustfs.env` and `ca.pem`. Copy them to `~/.config/lane-agent/` on the worker (see *Config* under `lane agent` in the README).

`remove` first refuses while `lane_dispatch.tickets` has `claimed` or `running` rows for the host (it prints their ids and lanes; `--force` goes on). The guarantee is one transaction: it locks the principal row `FOR UPDATE` (the lock `ld_claim` takes, so no claim lands meanwhile), re-checks for held tickets, sets `NOLOGIN` and deletes the `principals` and `runners` rows (no foreign key links them). Then it terminates the role's sessions and drops the role; removes the `pg_hba` block and reject-list entries with the same backup, check and reload; detaches and removes the RustFS user and policy (the user is found through the access key in the 1Password item; "not found" is tolerated); and removes the train entry. The 1Password item stays: archive it with `op-bot item delete "lane-agent <host>" --vault BotAccess --archive`.

`status` is read-only and reports the role, principal row, runners row (version, `last_seen`), the `pg_hba` line and reject-list membership, the RustFS user and policy, the train entry, the 1Password item and the host's claimed and running tickets.

## Reading the queue as lane_reader

`lane_reader` has no USAGE on `lane_dispatch` and no grant on its tables. Monitoring reads three views in schema `lane`, owned by
`lane_definer` and granted SELECT in `sql/lane/functions.sql`, so every apply keeps them: `lane.pull_tickets` (no `claim_token`;
the header only as `argv_md5`), `lane.pull_principals` (agent principals' `host, tags, allowed_repos, max_claims`, no
`login_role`) and `lane.pull_runners` (`host, last_seen`). `last_seen` is liveness: `ld_register`, `ld_claim` and `ld_heartbeat`
all refresh it, at most once per 30 s per host, so a live agent's row is under about 45 s old (BRAIN-1301). A reader that needs another column gets it added to a view; never grant
`lane_reader` anything in `lane_dispatch` by hand, because the next apply's REVOKE removes it (BRAIN-1154).

## Timers on the database host

Two systemd units on pitelite run as a `lane_admin` login (a member of `lane_admin`, so it holds the `pg_signal_backend` and `pg_read_all_stats` grants above). Both are required: the guard is the enforcement behind the per-role `statement_timeout`, which a caller can override.

One `lane-dispatch-reap.timer` (templates `ops/pull/lane-dispatch-reap.{sh,service,timer}`; the script installs at `/opt/lane-dispatch-reap/`) fires every 15 s (`AccuracySec=1s`) and runs two separate transactions, one SET per parameter, and the guard runs whether or not the reap succeeded:

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

`ld_claim(req, free_cpu, free_mem, held_keys, has_tools)` takes a ticket only when its `requires` (the lane's tools, the trailing `ld_enqueue` argument since migration 008) is a subset of `has_tools`. The agent builds `has_tools` from `ld_needed_tools()`, the sorted, distinct tools of the queued tickets that agent is eligible for, by checking each one on the PATH it spawns lanes with. A 4-argument claim (an agent older than BRAIN-1135) passes no tools, so it never takes a ticket that requires one. `claim_view` returns `requires`, so the agent can check again before `ld_start`.

`ld_release` takes the proofs `never-started` and `cancel-confirmed` only. A `claimed` row returns to `queued` (or `cancelled` when a cancel was requested); a `running` row with a cancel requested and proof `cancel-confirmed` settles `cancelled`; any other `running` row is left unchanged and the answer is `{refused: 'started'}`. `ld_start` on a running row whose cancel was requested answers false, and a heartbeat answers `cancel: true`.

### Claim order

`ld_claim` takes the ticket with the highest `eff_score(priority, ages, age)`, then, among tickets that have waited past the 60 minute horizon and score at the high level, the oldest first (fresh highs skip this step and go by scarcity), then the fewest agent principals able to run it (`scarcity`, the count `ld_placeable` reports), then the oldest, then the lowest id (priority is no tie-break: after 60 minutes an aged ticket that ties a high wins on age). `eff_score` ports `src/priority.js` `score()`: the tier is the priority band (7-9 high, 4-6 medium, 0-3 low), weights 2 and 2, age saturating at 20 minutes, and a non-high held at 0.99 of the tier weight until it has waited 60 minutes. Priority 9 (a release candidate) adds 0.01, so it beats every other high and every aged ticket that ties one. A ticket enqueued with `ages = false` credits no wait. `lane_dispatch.now()` is the queue's clock; the GUC `lane.test_now` moves it for tests only, and only when its value is the one stored by `ALTER DATABASE` or `ALTER ROLE` (a session `SET` to any other value is ignored).

## Notification channels

Each hint carries an empty payload, so no ticket id reaches every listener, and a listener treats it as "look now", never as data. `ld_work` means a ticket may have become claimable: `ld_enqueue` on insert, `ld_release` when the row returns to `queued`, and `ld_reap` when it requeued at least one row (a row backing off on `not_before` becomes claimable without a hint, so an agent still polls). `ld_host_<host>` goes to one agent: `ld_cancel` of a claimed or running row, and `ld_attach_urls` when it found its row. `ld_set_principal` refuses a host that does not match `^[a-z0-9][a-z0-9-]{0,54}$` (the runner-name shape, short enough that `ld_host_` plus the host fits Postgres's 63-byte limit), so the channel name is always safe. Hosts may contain hyphens, so the agent must `LISTEN` with a quoted identifier (`client.escapeIdentifier('ld_host_' + host)`); `pg_notify` takes the name as text and needs no quoting.

An agent login holds at most two connections (`CONNECTION LIMIT 3` leaves one spare): one for calls and one that only LISTENs. A lost NOTIFY costs latency, not correctness, because the agent still polls.

## Pruning

Each `ld_reap` tick deletes at most 1000 `claim_requests` rows: claimed mappings created more than 24 hours ago (which keeps the delete from scanning `tickets`) whose ticket is `finished`, `cancelled`, `withdrawn` or `lost` and whose `finished_at` is more than 24 hours old. A mapping whose ticket is not terminal is never pruned, whatever its age. `tickets`, `attempts` and `log_chunks` are not pruned here.

## Pull client surface and retention

Submit logins (the pull client) also hold `ld_queue_ref()` (the queue's `timeline.incarnation`, which moves after a restore), `ld_enqueue_fenced(expected_ref, ...)` (the `ld_enqueue` arguments after a leading ref; a stale ref returns `{state: 'stale-queue-ref', queue_ref}` and inserts nothing, otherwise it calls `ld_enqueue` unchanged, including the trailing `avoid_tags`, `requires` and `min_cpu_cores`) and `ld_placeable(repo_key, requires_tags)` (the number of agent principals with `max_claims > 0` that are eligible for the tags and repo). `ticket_view` carries `attempt_ref`, `sha256('lane-attempt-ref:' || claim_token)`, NULL while the row holds no token; `claim_token` itself never reaches a submitter. `ld_heartbeat` answers only `{fenced, cancel}`.

`ld_cancel` of a queued row stores a server-authored result `{kind: 'cancelled', cancelConfirmed: true, source: 'queued', epoch}`; a cancel of a claimed or running row only sets `cancel_requested`. A replayed `ld_cancel` reports `confirmed` from the stored `cancelConfirmed` (a non-boolean value reads as false), so it never upgrades an unproven cancel. A cancelled row with no stored result (released with `cancel-confirmed`, or a claimed row reaped after a cancel request) reads as confirmed.

Retention is admin-only (`lane_admin` login, run by `lane pull gc`; none of these is granted to agent or submit logins):

- `ld_prune_candidates(objects_after, rows_after, after_finished, after_id, limit)` pages terminal tickets older than `objects_after` in ascending `(finished_at, id)`. The first page passes both cursor arguments NULL; a half-null cursor raises 22023. `limit` is clamped to 1..500. It returns `{items: [{id, finished_at, row_due}], next}`; `next` is the cursor for the following page, or null on a short page.
- `ld_prune(ids, rows_after)` takes the `cluster` row lock first (as `ld_reap` does, so the two serialise instead of deadlocking on `claim_requests`), locks the listed terminal rows in id order and then deletes the log chunks, claim_requests mappings, attempts and ticket rows of the listed ids that are terminal and older than `rows_after` (re-checked inside the call); more than 500 ids raises 22023. It returns the number of ticket rows deleted.
- `ld_prune_chunks(terminal_after, limit)` deletes `log_chunks` with no ticket row or whose ticket has been terminal longer than `terminal_after` (default 1 hour), at most `limit` per call (default 10000). Nothing calls it any more (`lane pull gc` dropped the pass with the log stream); it goes with `log_chunks` in a later migration.
- `ld_enqueue_fenced` refuses (22023) a snapshot whose `blobs` is present but not an array, and `ld_blobs_live` treats a non-array `blobs` as empty. `ld_blobs_live(refs)` takes a JSON array of at most 500 `{ns, sha256}` and returns the subset referenced by a non-terminal ticket's `snapshot.blobs`. A blob is deleted from object storage only when it is old enough and absent from this set.

When this database is unreachable from a host that sets `lanePull.localFallbackWhenUnreachable`, that host runs its pull-eligible lanes locally instead of waiting out `queueUnreachableAfterMs` (README, pull dispatch: *Local fallback while the queue is unreachable*). Its marker is `<state>/pull/queue-health.json`; `lane status` shows `pull queue: unreachable since <time>` while it is set. The cancel a moved ticket owes its row is delivered by the same sweep as the `queue-unreachable` one, so a queue that comes back cancels the stale row (a row a runner already claimed is told to stop).

One unit on pitelite runs the client side (templates in `ops/pull/`): `lane-pull-gc.timer` hourly (`lane-pull-gc.service`, an admin login whose password `/etc/lane-pull/gc-admin.pass` is root:root 0600 and reaches only that unit through `LoadCredential=`, with the submitter password `/etc/lane-pull/submit.pass` hidden from it by `InaccessiblePaths=`). There is no tender unit: a dead supervisor's ticket is settled by the reconcile sweep, `lane wait` or `lane cancel`. **`train-lane-runner-watch.timer` (every 60 s, `lane runner watch`, which calls `sweepOrphans`) is required for pull**: it settles those tickets, sends the cancel a ticket owes its row after a queue outage (`<state>/pull/<id>/cancel-owed.json`) and is the only thing that does so unattended; keep it enabled on every host that runs `lane run` for pull. `lane pull gc` passes `rows_after = lanePull.rowRetentionMs` (default 7 days) and refuses a smaller `--rows-after`, because the client's enqueue horizon assumes a row survives that long. Paging state is `<state>/pull/gc-state.json`. gc also expires local state: a ticket's ledger entry and `pull/<id>/` (envelope, staged copies) after `lanePull.ledgerRetentionMs` (default 45 days; the config refuses below 35 days, the queue's backup retention), never while an attempt record exists.

## Ticket status for merge-train (migration 011, PKG-437)

`011-ticket-status.sql` adds four `lane_dispatch.tickets` columns: `late_result jsonb`, `late_result_at timestamptz`, `log_state text` (null or `missing`) and `status_changed_at timestamptz` (stamped at every transition). `train_reader` is a NOLOGIN group role holding USAGE on schema `lane` and SELECT on `lane.ticket_status_v1`, nothing else: no table, no `ld_*` function, no USAGE on `lane_dispatch` (grants in `functions.sql`, so every apply keeps them, and only once the role exists). The production login that gets `train_reader` is cluster-level and out of this repo's scope (zirkbot `cluster.toml`); until it is mapped, nothing logs in as the role.

**Provisioning `train_reader` (once per cluster).** The migration creates the role only when the applier is a superuser or has CREATEROLE; `lane_migrate` has neither, so on pitelite it skips the role with a NOTICE and still applies the columns. Then a superuser (or a CREATEROLE role) runs once, and the database owner or a superuser grants CONNECT, because `applyMigrations` leaves database ACLs alone (`databaseAcls` is false) and PUBLIC has no CONNECT:

```sql
CREATE ROLE train_reader NOLOGIN;
GRANT CONNECT ON DATABASE lane_queue_db TO train_reader;
```

Then re-apply `functions.sql` as `lane_migrate` (the command under *Applying migrations*; a run with nothing new prints `"applied":[]` and re-applies it), which grants the role. An explicit `applyDatabaseAcls` by an authorized role also grants CONNECT, to every lane group role that exists. A login whose only membership is `train_reader` can then connect and select the view (`tests/lane-dispatch-ticket-status.test.js`, the provisioning case).

`lane_admin` holds SELECT only on `lane.ticket_status_v1`, `lane.pull_tickets`, `lane.pull_principals` and `lane.pull_runners`: they are single-table views, so the blanket `GRANT ALL` on schema `lane` would let it write ticket state through them past the fence; `functions.sql` revokes that after the blanket grant.

`lane.ticket_status_v1` has these columns, in this order, and the list is final for `_v1` (a change is a `_v2`): `dispatch_key` (the ticket's dispatch key since migration 012, below; NULL for a ticket submitted without one and on a queue not yet at 012), `ticket_id`, `repo_key`, `lane`, `state`, `claim_generation` (the `epoch`), `host`, `lease_until`, `result` (as stored), `log_ready`, `log_sha256`, `log_state`, `late_result_at`, `updated_at`. It never carries `claim_token`, the header or the snapshot. `log_ready` is `result->>'logSha256' IS NOT NULL`: the agent uploads the log and puts the hash in the result before `ld_finish`. `updated_at` is `greatest(status_changed_at, late_result_at)`: `status_changed_at` is stamped at every transition (enqueue, claim, start, finish, requeue, cancel, withdraw, lost, restore) and at the log-missing mark, so it never moves backwards and a reader may use it as a change cursor. A held `ld_cancel` and a heartbeat change nothing in the view and do not move it.

**Change events.** Each transition sends `pg_notify('lane_ticket_events', json)` from `lane_dispatch.notify_ticket(id, state, epoch, reason)`, called explicitly in the transaction that makes the change (a rollback sends nothing). The payload is `{"ticket": id, "state": ..., "epoch": n, "reason": ... | null, "repo": ..., "lane": ...}`, `repo` and `lane` cut to 200 characters. This is a new contract and is distinct from the empty-payload hint channels above (`ld_work`, `ld_host_*`), which stay empty. `state` is one of:

- `claimed` (`ld_claim`) and `running` (`ld_start`), both without a reason;
- `finished`, or `cancelled` when the result's kind is `cancelled` (`ld_finish`; the reason is the result kind), and `cancelled` with reason `cancel-confirmed` (`ld_release` of a running row);
- `queued` (a claim returned to the queue, with reason `released-never-started`, `released-cancel-confirmed`, `lease-expired` or `supervisor-died-while-queued`; `epoch` is the attempt that ended, the next `claimed` carries epoch + 1), and `cancelled` or `lost` (`release-cap`) from the same return path;
- `cancelled` with reason `cancelled-while-queued` (`ld_cancel` of an unheld row) and `withdrawn` (`ld_withdraw`);
- `lost` with reason `runner-lost` (`ld_reap`) or `queue-restored` (a restore);
- `log-missing` (`ld_reap`, reason `no-log-sha256`).

`ld_cancel` of a held row only sets `cancel_requested` and sends no event; `cancelled` follows at the real transition. `ld_enqueue`'s insert and `ld_prune` send none. The events are untrusted hints: any connected queue login can LISTEN on the channel (and so see other submitters' ticket ids, repos, lanes and outcomes) and any connected login can NOTIFY on it, so a reader must confirm the state in the view before acting, and the payload carries only ids, repo, lane, state, epoch and reason. NOTIFY is lossy for a listener that is down, so a reader must also poll the view, and a pruned row vanishes: a reader treats a ticket missing from the view as terminal and pruned.

**Late results.** `ld_finish` keeps its answer and its fence. When the ticket is already terminal (`finished`, `cancelled`, `withdrawn`, `lost`), the call is not an idempotent replay, and the caller's host, epoch and token match an `attempts` row, the result (minus `author` when it is a JSON object; a scalar or null is stored as is) is stored in `late_result` with `late_result_at`, latest wins. The state and `result` never change: the first terminal wins.

**Log missing.** Each `ld_reap` marks `log_state = 'missing'`, once, on a `finished` ticket with no `result->>'logSha256'` that finished more than 10 minutes and less than 24 hours ago, and sends `log-missing`. It runs under the `cluster` row lock like the rest of the reaper. A ticket that finished more than 24 hours ago is never marked, so the first reap after migration 011 sends no burst of old events.

## Dispatch keys (migration 012, PKG-438)

A caller that loses the ticket id (a `lane run` killed or timed out) submits with a dispatch key and finds the run again by it. `012-dispatch-key.sql` adds `lane_dispatch.tickets.dispatch_key text` (null, or 1-200 characters of `[A-Za-z0-9:._/-]`, enforced by a CHECK) and the unique partial index `tickets_dispatch_key ON tickets (dispatch_key) WHERE dispatch_key IS NOT NULL AND state <> 'withdrawn'`. It touches no role, so a NOCREATEROLE migrator applies it. `lane.ticket_status_v1.dispatch_key` is the column (same type and position as the NULL it replaced).

The key is global, not per owner: callers namespace it (merge-train: `train:<repo>:<attempt>:<shard>`). `ld_enqueue` and `ld_enqueue_fenced` take a trailing `p_dispatch_key text DEFAULT NULL`; the 18- and 19-argument forms are dropped (a default on a longer overload would make the old calls ambiguous), and every older call shape still resolves to the new function through its defaults. `lane_dispatch.key_holder(key)` is the one definition of a taken key.

- A live (non-withdrawn) ticket under the key whose owner is the caller is returned as `ticket_view`, whatever id the call carries. A terminal ticket (`finished`, `cancelled`, `lost`) keeps its key, so a resubmit returns that ticket and its result and never runs again; a caller wanting a new run uses a new key (merge-train's carries an attempt number). **Retention.** Idempotency holds only while the ticket row is kept: `ld_prune` deletes a terminal row past `lanePull.rowRetentionMs` (default 7 days) and the key goes with it, so a `lane run --dispatch-key` after that executes the command again. Callers must use attempt-scoped keys and must not resubmit a key after the retention window.
- A key held by another owner raises SQLSTATE `LK409`. The client turns it into exit 64, not the generic `23505` of an id conflict.
- A withdrawn row frees its key, so a later submit creates a new ticket (a withdrawn row keeps its id, so a resubmit that reuses that id gets the withdrawn row back, as before).
- Two concurrent submits with one key: the loser of the unique index is answered with the winner's ticket (or `LK409` if the winner is another owner's).
- An id that already names a ticket of the caller's that never held this key raises `23505` ("already exists under another dispatch key").
- `ld_ticket_by_key(key)` (granted to `lane_submit`) returns the caller's own live ticket under the key as `ticket_view`, or null. `lane status --dispatch-key <key> [--json]` prints its id, state and host (exit 0; 1 when there is none; 64 for an invalid key).

`lane run --dispatch-key <key>` never falls back: a key needs the pull queue. The run is refused (exit 76, "dispatch key needs the pull queue") when the lane would not be dispatched, pull dispatch is off, or placement would choose a push runner, local execution or the legacy flow; and when the queue lacks the 19-argument enqueue (42883, a queue below migration 012) the key is never trimmed off. A key the queue already holds is observed, never submitted again: for a ticket this machine has a record of, the run answers with `lane wait` semantics; otherwise it attaches to the queue ticket (no placement, no snapshot, no supervisor), waits until it ends and reports its exit from the stored result, which it keeps as a local result so a later run answers from it (no history row: this machine ran nothing). `--detach` prints the held id at once. A `--detach` that submits prints an id only after the queue answered the enqueue, and it is the id the queue holds for the key, so two callers racing on one key print the same id. A refusal (old queue, foreign owner, a withdrawn ticket) is a ledgered decision published with its exit (76 or 64), so a recovery after a crash publishes it instead of sending the enqueue again. A keyed submission's envelope is version 2, which a release older than the key refuses (it checks for version 1), so it can never enqueue the run without its key. `--dispatch-key` and `--id` are exclusive (exit 64).

## Placement constraints (migration 013, PKG-439)

`013-placement-constraints.sql` adds `lane_dispatch.principals.platform` (`darwin`, `linux`, `win32` or NULL) and four columns to `tickets`: `capability_class` (`<platform>[+tag...]`, CHECKed), `class_platform`, `avoid_hosts text[] NOT NULL DEFAULT '{}'` and `target_host`. No backfill: every existing ticket is unconstrained. It touches no role, so a NOCREATEROLE migrator applies it. `functions.sql` then:

- adds `placeable_on(class_platform, avoid_hosts, target_host, host, platform)`, the one claim-side predicate. `ld_claim` (the candidate query and the `v_head` head-reserve query), `ld_needed_tools` and `scarcity` all call it. `ld_claim` reads the platform in its existing `FOR UPDATE` principal projection (so a `ld_set_platform` committed while a claim waits is seen by that claim); `ld_needed_tools` reads it through the new unlocked `me_platform()`; `me()` keeps its return type.
- re-signs `ld_enqueue` (19 to 22 arguments) and `ld_enqueue_fenced` (20 to 23) with trailing `p_capability_class text`, `p_avoid_hosts text[]`, `p_target_host text` (the old arities are dropped), and `scarcity` (2 to 5 arguments, the new ones defaulting to no constraint; the 2-argument form is dropped). `ld_placeable(repo, tags)` stays; `ld_placeable(repo, tags, class_platform, avoid_hosts, target_host)` and `ld_ticket_by_key(key, placement jsonb)` are overloads beside the old forms, and `ld_known_hosts()` is new, all granted to `lane_submit`. `ld_set_platform(host, platform)` is granted to `lane_admin` only.
- stores placement canonical: class platform first with the class's tags de-duplicated and sorted (and added to `requires_tags`), host ids lower-case, lists sorted and de-duplicated (C collation). A malformed value raises `22023`. `avoid_hosts` must be a known id (a current agent principal host, or a host on a retained `tickets.host` or `attempts.host`), `target_host` a current agent principal, else `LK422` "unknown host <id>"; a target that is also avoided is `LK422`.
- answers a repeat first: after the ownership check an existing row with the same id (or the live row under the same dispatch key) is returned before the hosts are looked up, so a retry after the targeted runner was removed still returns the ticket. A holder or id placed otherwise is refused `LK423` ("different placement"). A placed request is compared to the row's canonical values; an unconstrained request adopts any holder, placed or not (it ran under a placement at least as strict, and a pre-439 client never meets the new SQLSTATE).
- adds `unplaceable` (NULL, or `no-runner`, `tags`, `class`, `avoid`, `target`) and the three placement fields to `ticket_view`, which is what `ld_get`, `ld_list_mine`, `ld_enqueue` and `ld_ticket_by_key` return; the reason is computed over the scarcity set (agent, `max_claims > 0`) with the same `eligible` and `placeable_on` tests, in that order.
- appends `capability_class`, `avoid_hosts`, `target_host` to `lane.pull_tickets` and `platform` to `lane.pull_principals` once the columns exist (`ticket_status_v1` is unchanged).

**Setting platforms is a step of the rollout**, because a runner with no platform never claims a ticket that names one. Production values (pitelite, read 2026-11-26):

| host | platform |
|---|---|
| `evo-x2` | `linux` (WSL) |
| `wintop` | `linux` (WSL) |
| `skybox` | `linux` |
| `mac` | `darwin` |
| `mac-grandy` | `darwin` |
| `zirkbot` | `darwin` |

For each, run the dry run, read the printed `current:` row and the single call, then apply:

```sh
node ops/runner/lane-runner-admin.js set-platform wintop linux            # dry run
node ops/runner/lane-runner-admin.js set-platform wintop linux --apply
```

Do not use `add` for this: it rewrites tags, repos and max_claims through `ld_set_principal`. `status <host>` shows the platform on the principal row.

Rollout order: apply 013 (the migrator), set the platforms, then update the clients (`lane` on the submitters, the merge-train host). A queue below 013 refuses a placed run (`76`, "queue too old for placement constraints") and never runs it without its placement; an old client against a migrated queue is unaffected. The zirkbot collector `ops/monitoring/collectors/lane-pull.sql` reimplements `eligible` and does not know placement; it needs its own change.

## Weight-aware claims and requeue (migration 010, BRAIN-1106)

`010-claim-weight.sql` changes no table: it versions the function set. `ld_claim` gains two trailing parameters after the 1099 capacity pair, `p_free_weight` and `p_weight_limit` (NULL for an older agent: no weight check). `ld_register` answers `claim_weight: true`, and an agent sends the weights only to a queue that does. `functions.sql` drops the seven-argument `ld_claim`, so a 1099 agent's seven-argument call resolves to the new function through the defaults. `ld_finish` of `{kind: 'unfinished', reason: 'supervisor-died-while-queued', neverStarted: true}` requeues the ticket (the shared `not_before` backoff, token and host cleared, so a late `ld_finish`, `ld_start` or `ld_heartbeat` of the old attempt is fenced) while `requeues < 3` and no cancel is pending, answering `{fenced: false, state: 'queued'}`; after that it finishes unfinished with `requeuesExhausted: true` and `requeues` added. The reason stays `supervisor-died-while-queued`, which the submitter treats as proof nothing ran.

**Apply order.** Migrate the queue first, then upgrade agents. From a checkout of this release, as the database owner (the connection comes from the standard `PG*` variables):

```
PGHOST=<host or socket dir> PGUSER=<owner> PGDATABASE=<db> node --input-type=module -e "import { connect, applyMigrations } from './src/lane-db.js'; const c = await connect({}); console.log(await applyMigrations(c)); await c.end();"
```

It prints `{ version: 10, applied: [...] }` and is safe to repeat. An agent that reaches a queue below 010 claims without weights, logs it once and reports `fallback: true`; that fallback is removed once every agent and queue is on schema 10.
