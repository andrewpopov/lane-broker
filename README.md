# @andrewpopov/lane-broker

A machine-wide **test-lane coordinator**. Agents on a shared laptop run heavy
test suites with no awareness of each other; `lane run` serializes conflicting
lanes, caps how many heavy lanes run at once, and refuses to start new work
while the machine is already overloaded — instead of every agent guessing.

Zero runtime dependencies — Node `child_process`/`fs`/`os` only (Node ≥ 20).

## Install

**Do not `npm install -g github:owner/repo#tag` directly.** On npm 11.9.0 /
node 24 that form can exit 0 while leaving a dangling symlink into npm's own
git-clone cache — an empty package directory with no `bin/lane.js`, even
though `npm ls -g` still reports the right version. npm later prunes that
cache and `lane` breaks machine-wide. Pack first, then install the tarball:

```sh
tmp=$(mktemp -d)
(cd "$tmp" && npm pack "github:andrewpopov/lane-broker#v0.4.0")
npm install -g "$tmp"/*.tgz
rm -rf "$tmp"
```

This installs the `lane` command. Verify it by running it, not by reading a
version — `lane --help` must actually work, and after any upgrade check that
the installed source really changed (the version string can be correct while
the code behind it is stale, which is exactly how a merged fix goes
unnoticed).

Do not reinstall while lanes are running: `npm install -g` replaces the
package directory, and a supervisor that lazily imports a module afterwards
exits with `supervisor exited unexpectedly with no result`. Wait until
`lane status` shows nothing RUNNING or queued.

## Use

```
lane run --repo rouge --lane default -- npm test
lane run --repo rouge --lane lint    -- npm run lint
lane run --exclusive --lane release  -- ./deploy.sh      # whole-host exclusive lane (BRAIN-403 v1)
lane status [--json]
lane capabilities --json
lane suggest [--repo <name>] [--days 7] [--json]
lane cancel <id>
lane wait <id> [--timeout 5m]
lane pause "reason" | lane resume
lane migrate-scheduler [--dry-run | --when-idle [--timeout <duration>]]
lane gc [--dry-run] [--max <n>]
```

`lane status` adds `  log file unchanged for <duration>` to a RUNNING line once the
lane's log file mtime is 5+ minutes old (`logAgeMs` in `--json`). Report-only: it
describes the file, not the child (a capped log stops changing while the child
keeps writing; a quiet build looks identical to a wedged one), and never
triggers a cancel.

`lane status` says what each queued ticket is waiting on, taken from the scheduler's
own `tryStart` outcome rather than re-derived: a line `head <id8>: waiting on <label>
for <duration> (checked <duration> ago)` (or `no recorded wait yet`), and a
`  waiting: <label>` suffix on each QUEUE line (`wait: { reason, label, since, at }`
per queued entry in `--json`). The record is one file per ticket,
`<state root>/waits/<id>.json`, written by the supervisor polling that ticket after it
releases the lock, only when the reason changes or the record is a minute old. It is
removed when the ticket starts or ends, and files of tickets no longer queued are
pruned by the next admission pass; status ignores them. Telemetry only: admission
never reads it.

Everything after `--` is the command, passed as `argv` (not through a shell —
`spawn(cmd[0], cmd.slice(1))`). Use `sh -c '...'` explicitly if you need shell
features like pipes or globbing.

### `lane run` flags

| Flag | Purpose |
|---|---|
| `--repo <name>` | Repo label, used only as a fallback for the lease key. When `cwd` is inside a git repo, the git identity (`git rev-parse --git-common-dir`) always wins, so the same repo resolves to the same key whether or not `--repo` is passed; `--repo` only determines the key outside a git repo. |
| `--lane <name>` | Lane name (`default` if omitted); looked up in `.lane-broker.json`. If the repo config declares `lanes`, an undeclared name is refused (exit `64`) rather than silently keying on a private, unconflicting lane — unless the repo config sets `"undeclaredLanes": "allow"`, in which case an undeclared name resolves like the no-config-file case (default weight, never remote-eligible) instead of being refused. |
| `--weight <n>` | Override the configured weight for this run; must be a positive number (validated before enqueueing, exit `2` otherwise). |
| `--cpu <cores>` | Override the lane's CPU reservation; fractional cores are supported. |
| `--memory <size>` | Override the lane's memory reservation, e.g. `768MiB` or `4GiB`. |
| `--id <ticket-id>` | With `--detach` only: a client-supplied ticket id (`[A-Za-z0-9._-]`, up to 128 chars) that makes the submission idempotent. An id the broker already knows (queued, leased, finished or mid remote attempt) is returned as the same ticket: no new queue sequence and, for a remote-eligible lane, no second dispatch (the check runs before any probe). A remote-eligible lane needs a UUID id. |
| `--priority high\|medium\|low` | BRAIN-380 priority tier (default `medium`). Beats env `LANE_BROKER_PRIORITY`, which beats the lane's `priority` in `.lane-broker.json` (an `undeclaredLanes.as` template passes its tier on), which beats `medium`. Any invalid value from any of the three exits `64`. See "Priority (foundations)" below. |
| `--exclusive` | BRAIN-403 v1: claim the whole host for this run. See "Exclusive lanes (BRAIN-403 v1)" below. The lane config key `lanes.<name>.exclusive: true` does the same. |
| `--detach` | Print the run id and return immediately instead of waiting. |
| `--timeout <duration>` | e.g. `30s`, `5m`, `500ms`. Exit `75` if not finished in time — see below. |
| `--allow-local-sim` | Override a lane's `localRefused: true`. |
| `--log <path>` | Override the default log path. |
| `--local` | Never dispatch to a remote runner (same as `LANE_BROKER_LOCAL=1`). |

**Every run gets a detached supervisor**, always. `lane run` spawns it, then
only *waits* on the supervisor's result file. If the calling agent (or its
10-minute tool ceiling) kills the `lane run` process, the supervisor and the
running suite are unaffected — `lane status` still shows it RUNNING, and a
later `lane run` on the same key waits behind it. `lane wait <id>` reattaches;
this is the sanctioned way to outlive a short tool ceiling on a long lane.
`lane wait` on an id that is neither queued, leased, nor resulted fails fast
(after a short grace period, to avoid racing a `lane run --detach` that
hasn't finished enqueueing yet) rather than blocking forever on a typo.

**Foreground `lane run` streams the child's output live**, stream-separated:
the child's stdout goes to `lane run`'s own stdout, its stderr to `lane
run`'s own stderr, exactly as if the command had been run directly — and
`--log` keeps writing the same bytes to a file regardless. If the caller
disappears (killed, or the tool ceiling above), the lane keeps running and
logging normally; nothing is lost except the live view. `lane wait` and a
`--detach` run do *not* stream — a `--detach` caller returns before the
child even starts, and `lane wait` reattaches to a run whose supervisor was
never told to forward to it — both still get the `--log` file.

## Config

**Global** — `$LANE_BROKER_HOME/config.json` (default
`~/.config/lane-broker/config.json`):

```json
{
  "version": 1,
  "capacity": "auto",
  "schedulerMode": "active",
  "cpuAdmissionPercent": 75,
  "cpuReserveCores": 1,
  "preemptibleNiceMin": 1,
  "preemptibleShare": 0.8,
  "memoryReserveBytes": 2147483648,
  "defaultMemoryBytesPerWeight": 1073741824,
  "loadClose": 15,
  "loadOpen": 11,
  "loadOpenSamples": 3,
  "sampleMs": 5000,
  "resourceSkipLimit": 3,
  "resourceIdleOvershootCores": 1,
  "admissionLoadGate": false,
  "laneNice": 10,
  "noProgressTimeoutMs": 0
}
```

By default lane-broker detects the resources visible to its process and
actively reserves CPU and memory before starting a worker. On Linux this
includes cgroup v2 quota, CPU-set, and memory limits; WSL is treated as Linux.
On macOS it uses the logical CPUs visible to Node; available memory is sampled
via `vm_stat` (free + inactive + speculative pages), because
`os.freemem()` counts only free pages and undercounts available memory by an
order of magnitude on macOS. `capacity: "auto"` derives the legacy weight cap
from the detected CPU budget. A numeric capacity remains supported as an
additional compatibility cap.

`cpuAdmissionPercent` and `cpuReserveCores` retain CPU headroom for interactive
work. `memoryReserveBytes` is held back from worker reservations and current
available memory. Legacy lanes without explicit resource values reserve one
CPU core per weight unit and `defaultMemoryBytesPerWeight` bytes per weight
unit. Set `schedulerMode` to `"shadow"` to log resource decisions without
enforcing them.

`preemptibleNiceMin` (default `1`; `0` disables) and `preemptibleShare`
(default `0.8`, `0`-`1`): CPU spent by *lower-priority* processes does not stop
a lane from starting, because the kernel runs the lane ahead of them. A
process is preemptible only when its nice is STRICTLY GREATER than the nice the
candidate lane will run at (its ticket's `nice`, else this host's `laneNice`)
and at least `preemptibleNiceMin`; equal-nice load is a peer and counts in
full. Each is measured by identity over the same window as the host busy
sample: the sampler diffs every process's cumulative CPU time (Linux
`/proc/<pid>/stat` utime+stime, macOS `ps time=`) against the previous reading
keyed by pid plus start time, kept in the `cpu-sample.json` sidecar. A process
with no previous reading, or a reused pid, contributes 0. Processes in a held
lease's tree (process group, descendants, the BRAIN-419 recorded set) are never
preemptible whatever their nice; if a held lease's tree is not known yet or the
process table is unreadable, nothing is preemptible (fail closed). Admission
(the CPU gate's busy percent, `externalBusy`, and the projected-over-budget
check) uses `hostBusy - preemptibleShare * preemptibleBusy`, clamped to the
busy figure; the share is a safety floor, so at 0.8 a fifth of lower-priority
load still counts and a truly saturated host is never read as empty.
`lane status` shows `host CPU: <busy> busy cores, <n> preemptible` from the
last sample, and the admission log line carries `preemptibleBusy=` beside
`hostBusyCores=` and `externalBusy=` whenever it is non-zero. `LANE_BROKER_CPU_BUSY_FILE` accepts an
optional third field, `hostBusy,cores,preemptibleBusy`.

**Reading status: external CPU (BRAIN-463).** When `host CPU` is high but few slots
are used, `lane status` names the culprit instead of leaving you to trace `ps`:

```
external CPU: 12.00 busy cores outside any lease, top process trees:
  4.10 cores  pid 4821  xargs -P4 -n1 node --test  (cwd /work/tree)
admission starved by external CPU for 3h12m
```

The list is the top 5 process trees outside every held lease, ranked by cores over
the last sample window. A tree is reported against its topmost ancestor that is
not init/launchd, a shell or a terminal (so `zsh -> xargs -> timeout -> node
--test` with four workers is one entry with the summed cores). Lease trees, lease
supervisors, the status process and kernel threads are excluded; if a held lease's
tree is not known yet the list is empty rather than guessed. It reads the
per-process window the CPU sampler already stores (`cpu-sample.json`), so status
runs no extra scan, plus one command lookup for the few pids shown. `cwd` appears on
Linux only (`/proc/<pid>/cwd`); on macOS `lsof` costs seconds, so it is omitted. The
window is as old as the last admission poll, so an idle broker shows an old picture.

`admission starved by external CPU for <duration>` appears while admission denials
are `projected-over-budget` and external busy is at least 25% of the CPU budget. The
start is kept in `external-starved.json`, refreshed by each such denial, cleared by
any admission or by a denial the lanes themselves explain, and ignored once it has
gone unrefreshed for `max(30s, 6 * sampleMs)`.

`lane status --json` carries `externalCpu: { busyCores, top: [{ pid, cores, cmd,
cwd? }], starvedSinceMs | null }` (`starvedSinceMs` is a duration in ms, like
`noProgressSinceMs`). Exporting it as a metric and alerting on it is a zirkbot
(ops/worker-stats) follow-up and not done here.

`settledDemandEnabled` (default `true`): once a lease is settled (admitted at
least `settledDemandSettleMs`, default 120000, ago, with a fresh observation
and at least two observations in the last `settledDemandWindowMs`, default
180000), admission charges `max(observedNow, clamp(recentPeak *
settledDemandHeadroom, booking * settledDemandFloorFraction, booking))`
(defaults 1.25 and 0.5) instead of its full booking, so a lane that books 4
cores but uses 1 stops starving the queue. Unsettled leases are charged as
before; `false` restores that for every lease. The admission log's
`leaseDemand=<id8>:<cores>(settled|cold)` field shows the basis per held lease.

`historyDemandRaise` (default `true`, BRAIN-454): history may also RAISE a charge. A workload whose p90 peak
exceeds its declared cores is charged `min(host CPU budget, p90)` (source `history:raised`), so an under-declared
lane stops looking cheap; the budget is the one the admission predicate compares against, so a raised charge is
never unadmittable. `false` restores lowering-only.

`historyDemandEnabled` (default `true`) and `historyDemandMinRuns` (default 5): a workload with at
least that many compatible runs in `history.jsonl` (the last 50, no older than 14 days) is charged its
history estimate instead of its declared cores, as an admission candidate and as the cold demand of a
just-admitted lease. A workload is (repo, lane, `cmdFingerprint`, granted cores). `cmdFingerprint` is
recorded on the ticket, lease and history row when the run starts: the sha256 of the JSON of the full
argv, each element with the run's OWN checkout root (git toplevel) rewritten to `<repo>` and its own
`TMPDIR` to `<tmp>`, and nothing else, so the same command from another worktree of the repo is one
workload while another repo's path, a test-file argument, a flag or a number never merges. History rows
written before 0.23.1 have no fingerprint and are ignored, so relief starts from new history. An ad-hoc lane
name may also match the runs of its declared `configLane`, with the same fingerprint. A run counts only if it
succeeded, ran on the local executor, and was granted exactly the candidate's declared cores (a 1-core run
says nothing about an 8-worker candidate). The estimate is the p90 of the per-run PEAK observed cores,
floored at 0.5 and capped at the declaration, so a bursty lane is charged its burst. There is no repo-wide
pool, and no compatible history means the declaration. The settled-demand allowance above is unchanged and
never capped by the estimate. The snapshot is built before the admission lock is taken (never under it),
carries the config it was built under, and is used only while it matches the locked config and is under 5
minutes old. The admission log shows `candidateEstimate=<n>(history:exact|history:configLane|declared)`, and
`lane status --json` gives each queued ticket `cpuEstimate: { cores, source }`. `false` charges
declarations only.

`admissionLoadGate` (default `false`): whether the load gate is allowed to
deny a start at all. With the default, the gate is still sampled and reported
every poll (`lane status` shows it, `[informational]` suffixed), but it never
blocks a start — not for an idle broker, not for one already holding
non-conflicting leases. Set `true` to make a closed gate hard-deny again
(pre-BRAIN-207 behaviour, including the BRAIN-197 idle exemption).

`allocationShadow` (default `false`) and `simArmWindowMs` (default `300000`,
positive integer) — BRAIN-379 shadow mode. A lane may declare `"class": "test"`
(default) or `"sim"` in `.lane-broker.json`; an undeclared lane resolved via
`undeclaredLanes.as` inherits its template's class. With `allocationShadow` on,
every locked admission evaluation appends one `lane-broker-allocation-shadow`
line to `admission-decisions.log` (B, per-class used CPU, the 0.15·B test and
0.30·B sim soft locks, arm state, what class-aware allocation WOULD select, the
actual live outcome, per-candidate verdicts), and `lane status` gains an
`allocation` line/object. It never changes a live decision, skip counter or
reservation. The record is written after the broker lock is released, and only
for the head's poll or a poll that actually starts a ticket (to keep the log
small); every queued candidate's verdict (claim, effective and clamped claim,
reservation, and the existing guards that deny it) is in the head record's
`candidates=` list. A lane of class `sim` must resolve to at most 2 CPU cores.
Live enforcement (ROG-2181): the guarantee is that a `sim`-class ticket never delays
a waiting test ticket, not a literal start order. Behind a head that is not class
`sim`, a sim starts only through BRAIN-355's safe backfill, which is available on
every path (not just after a conflict-blocked head's skips are exhausted, and it
counts no skip): it must not conflict with the head or anything held, and held
weight + the head's weight + its own must fit capacity. Admission reserves the
head's CPU and memory claim while it is evaluated. A sim is never admitted by the
conflict, capacity or resource skip walks (they step over it, so a later test
ticket still backfills), and `conflictSafeBackfill: false` therefore turns every
sim pass off. A sim reservation owner never overrides a test ticket ranked ahead
of it: its reservation stays dormant until no test ranks above it. Test tickets,
and a sim behind a sim head, behave as before. The shadow snapshot above is
unchanged. `maxConcurrent` stays a ceiling only: a pool lane with a high ceiling
(e.g. 20 `fleet` tickets) is bounded by CPU and memory admission.

The sim arm stamp (`lastSimDemandAt`) lives in `sim-arm.json` in the
state root; a missing or unreadable file means unarmed.

`laneNice` (default `10`, range `0`-`19`): every lane is spawned under `nice
-n <laneNice>` so a heavy test run doesn't starve interactive work on a
shared machine. `0` disables niceing (the bare command is spawned with no
wrapper). A lane's own `nice` in `.lane-broker.json` overrides this. Niceing
changes the launch-failure contract for a missing/non-executable command:
with `nice > 0`, `nice` itself execs and reports the failure, so the lane's
recorded exit is `127` (command not found) or `126` (found but not
executable) instead of the structured spawn-error result a bare (`nice: 0`)
spawn would have produced.

**Per repo** — `.lane-broker.json`, discovered by walking up from `cwd` to the
git worktree root (repo identity is the realpath of `git rev-parse
--git-common-dir`, so every worktree of a repo shares one identity and one
set of leases):

```json
{
  "version": 1,
  "lanes": {
    "default": { "weight": 2, "cpuCores": 2, "memoryBytes": 2147483648 },
    "sim":     { "weight": 2, "localRefused": true },
    "lint":    { "weight": 1, "nice": 15 },
    "e2e":     { "weight": 2 },
    "prepush": { "weight": 2 }
  },
  "conflicts": [["sim", "*"]]
}
```

`conflicts` is a list of `[laneA, laneB]` pairs; `*` means "every other lane
in this repo". Two lanes with the *same key* (same repo + lane name) always
conflict, regardless of `conflicts` — unless that lane declares
`maxConcurrent`. With no config file, there is a single `default` lane of
weight 2.

A top-level `"undeclaredLanes": "allow"` (default `"refuse"`, today's
behaviour) lets an undeclared `--lane` name through instead of refusing it:
it resolves exactly like the no-config-file case (default weight, never
remote-eligible, no `conflicts` entry of its own), except that a declared
lane's `["*", "other"]` conflict still reaches it. This exists so a repo can
declare and remote-enable one specific lane (e.g. `prepush`) while its
sessions keep using ad-hoc lane names for everything else.

`"undeclaredLanes": {"as": "<declared lane>"}` (BRAIN-325) is a second allow
form: an undeclared `--lane` name still keeps its own key and name (the
same-key exclusivity rule above is unaffected — two sessions using the same
ad-hoc name still conflict with each other), but instead of the no-config-file
default it inherits `weight`, `cpuCores`, `minCpuCores`, `memoryBytes`, `nice`, `remote`,
`remoteDeps`, `remoteSetup`, `remoteOmitEscapingSymlinks`, `remoteArtifacts` and `remoteArtifactsOn` from the named declared lane. It does NOT
inherit that lane's own named `conflicts` entries or its `maxConcurrent`
(stays `1`) — only the `*` wildcard universe still reaches it, same as plain
`"allow"`. The named lane must be declared and must not be `localRefused`.
This is for a repo where every ad-hoc per-ticket lane (e.g. `zirk812`) should
run at the same size and remote eligibility as one canonical template lane,
without an operator hand-declaring each one. That includes `default` (a bare
`lane run`) when the file does not declare `default` itself (BRAIN-448):
before 0.25.1 the built-in `default` always counted as declared, so it ran
local at weight 2 and ignored the template.

A lane's own `maxConcurrent` (integer >= 1, e.g. `"fleet": { "weight": 1,
"maxConcurrent": 4 }`) relaxes ONLY the same-key rule above: up to that many
same-key tickets may hold a lease at once, each counted individually against
`capacity` and CPU/memory admission (a ceiling is the most that MAY run, not
a reservation — resource admission can still allow fewer). It never relaxes
a declared `conflicts` entry, which stays absolute regardless of either
lane's `maxConcurrent`. Omit it (the default, `1`) for today's exact
behaviour — a lane never declaring it is unconditionally exclusive against
itself, exactly as before this field existed. This is the shape a worker
pool needs (N sim runs at once), not a test lane (which wants exactly one).

A lane's `aging` (boolean, default `true`, ROG-2181) set to `false` stops its
tickets accruing priority age: score and effective rank use the tier alone, so an
old low ticket on that lane can never tie a fresh medium or high one (without it,
aging lets a low ticket tie a fresh high after 3 x `priorityAgeMaxMs`, and the older
ticket wins the tie). An undeclared lane resolved via `undeclaredLanes.as`
inherits its template's `aging`. Use it for a long-running `class: "sim"` lane
that must always queue behind test lanes. Lanes that omit it age exactly as before.

A lane with `localRefused: true` (the `sim` lane by default, matching the
rouge fleet split) is refused on `lane run` unless `--allow-local-sim` is
passed — print a refusal message and exit `69` instead. The message points at
`ROUGE_FLEET_SUBMIT_DSN` (never its value) only for a `class: "sim"` lane.

A lane's own `nice` (integer `0`-`19`) overrides the global `laneNice` for
that lane only; omit it to use the global default.

A supervisor that is still waiting to start (queued behind capacity or a
closed load gate) re-reads the global config on every poll, so an edit to
`config.json` — e.g. raising `loadOpen` to reopen a stuck gate — takes effect
within one `sampleMs`, not only for supervisors started afterward. A missing
or invalid config file during a reload is ignored and the last known-good
config keeps being used; it never crashes or wedges a running supervisor.

## Remote runners (BRAIN-319)

A lane can run on another machine instead of this one, and fall back to this
machine's broker when none is usable. It is opt-in twice: the machine lists
runners, and the repo marks a lane `remote`. With no `runners`, or a lane
without `remote: true`, nothing changes.

```json
// ~/.config/lane-broker/config.json (the client machine)
"runners": [
  { "name": "skybox", "ssh": "skybox-runner" },
  { "name": "grandy", "ssh": "mac-grandy", "shell": "zsh -lc", "speedFactor": 0.5 }
]
```

```json
// .lane-broker.json (the repo)
{
  "undeclaredLanes": "allow",
  "lanes": { "prepush": { "weight": 2, "remote": true } }
}
```

`undeclaredLanes: "allow"` opts one lane (`prepush` here) into remote
runners while leaving every other, ad-hoc lane name usable without being
refused as undeclared.

A `localRefused: true` lane (BRAIN-320) marked `remote` tries runners first;
only a *fallback to local execution* is refused, unless `--allow-local-sim`
is also passed. Such a lane never runs here by default, so it never takes the
"run locally because this machine could admit it" path (BRAIN-338): when every
runner is queued it queues on the least-loaded one regardless of local
capacity. When no runner can take it at all (every probe failed, or every
runner is paused/draining/over `maxRemoteQueue`) it exits `76` — retry later —
rather than the policy refusal `69`. A lane that is not remote-eligible, or
whose tree is ineligible for a snapshot, still exits `69`.

`ssh` is an ssh destination (an alias from `~/.ssh/config`, or
`ssh://user@host:port`); it may not start with `-`. `shell` (default
`bash -lc`) wraps every remote command so a non-interactive ssh finds
`node` and `lane`. `speedFactor` (BRAIN-405, positive number, default `1`, higher is
faster) scales this runner's estimated finish when several runners have room. `root` (default `~/.cache/lane-broker/remote` on the
runner) holds per-ticket work dirs. A runner has no `runners` of its own.

What a remote run does:

1. **Decide eligibility locally**, before touching the network. The file
   set is the worktree's tracked + untracked-not-ignored files that exist on
   disk (so staged, unstaged and deleted changes are all reflected). A tree
   with a submodule, special file, escaping symlink, or a secret-shaped path
   (`.env`, `.env.*` other than `*.example/.sample/.template/.dist`,
   `*.pem`, `id_*`) stays local. Gitignored files are never sent. The
   serialized file list (the snapshot's header line) is capped at ~16 MB —
   roughly 90k files — checked locally before dialing ssh; a larger tree
   stays local with that reason.
2. **Pick a runner**: probe every runner (`lane remote-probe`, 6 s hard
   deadline each) and keep those that are reachable, speak the protocol the
   lane needs (below), are not paused, have no queue, could ever fit the
   lane's reservation (weight, CPU budget, memory plus its reserve, from the
   runner's static capacity) **and have real room right now**: the probe's
   `headroom` (BRAIN-506: the room this runner's own admission would see for
   a ticket at the default `laneNice`, `budget − projectBusy` over its held
   leases with the same `leaseDemand` charges and preemptible discount, via
   admission's `cpuHeadroomCores`; from the supervisor's stored sample when it
   is fresh, else from a read-only host measurement taken by the probe; the
   larger of reserved and the 1-minute load average only when no sample can be
   taken; available memory minus the memory reserve; the probe also reports
   `headroom.laneNice`, the nice that discount assumed, and
   `headroom.cpuCoresUndiscounted`, and a submitter whose lane sets its own
   `nice` above that uses the undiscounted figure, since the runner runs it at
   the lane's nice rather than its own `laneNice`) covers the ticket's CPU (its
   elastic floor on a runner that admits below the declared claim) and memory.
   A figure the probe does not report never counts against a runner. A runner
   with an empty queue and no headroom is not chosen. Among the rest the
   earliest estimated finish wins: `(queued + 1) × the ticket's cpuCores ÷
   the runner's speedFactor` (a `runners[]` field, positive number, default
   `1`, higher is faster); a tie goes to the runner with more free CPU, then
   config order. None → local.
   If no runner is idle, the usable runner with the fewest queued tickets
   (ties: estimated finish, then config order) is taken **only if** it has at most `maxRemoteQueue`
   queued (global config, integer >= 0, default `2`; `0` = never queue on a
   runner) **and** this machine could not start the ticket right now either
   (its queue is non-empty, or a conflict, capacity, pause, load-gate,
   cooldown or memory check refuses it); otherwise the run stays local.
   A runner with room always beats any runner queue.
   A queued ticket whose supervisor is provably dead (pid gone, start time changed, or a different boot; an
   indeterminate probe counts as alive) is an orphan, e.g. after a runner reboot. It is reaped under the broker
   lock by every admission poll, `lane status` and `remote-probe`, so it never holds a `maxRemoteQueue` slot: it
   is dequeued, its result is published as exit 75 (`supervisor gone before admission (host restart?)`) and a
   history row is written. Leases are not touched here; the lease reaper owns them.
   A run queued this way records `queuedAt: "<runner>(<queued at pick time>)"`
   in its result and attempt record and logs `lane: queuing on ...`. The
   local check reaps stale records like every admission poll but never
   samples CPU or advances the load gate, so it skips the CPU projection (it
   can say "would admit" where the real admission later would not — that only
   means running locally, as before). With `admissionLoadGate` on it reads
   the persisted gate without sampling, so a gate one low-load sample from
   reopening reads closed and the ticket may queue remotely instead of
   starting locally.

   **Late rebinding (BRAIN-405).** A ticket that falls back to this
   machine's queue only because no runner had room keeps its eligibility
   (`remote.fallback: {reason, at}` on its queue record). While it is still
   queued, its own supervisor re-probes the runners every
   `remoteRebindIntervalMs` (global config, integer >= 0, default `30000`; `0`
   disables). When one has room it withdraws the ticket from the local queue
   in one locked step shared with the scheduler's own admission, so exactly
   one of the two takes it (a started, cancelled or already-gone ticket is
   never withdrawn), then dispatches as above, logging
   `lane: remote-rebind: <id>: seq <n> -> <runner>` to the admission log.
   The withdrawal is two-phase: the queue file is renamed to
   `<seq>-<id>.json.rebinding` (no listing reads it, and the scheduler keeps
   the ticket's fairness records for it) and the attempt records
   `rebinding`. If the supervisor dies before the dispatch is recorded in the
   attempt, the next stale-record reap puts the ticket back at its original
   seq; if it dies after, the attempt is the remote attempt to reconcile
   (ORPHANED-REMOTE) and the parked record is dropped.
   Only a proof that the runner never started the job puts the ticket back
   in the local queue **at its original sequence number** (`remote-rebind: …
   stays in the local queue at seq <n>`): a snapshot never completely sent,
   or the runner's confirmed preflight refusal (exit 64). Any other outcome
   that may have started it (ssh dropped and the result cannot be fetched)
   is **not** re-run locally, unlike a first dispatch: the supervisor logs
   `outcome unknown`, exits 1, and leaves the attempt with its runner so
   `lane status` shows it, `lane wait` names it and `lane cancel` cancels it
   on the runner. A running ticket never moves. Never rebound:
   `--local`, `LANE_BROKER_LOCAL=1`, an inherited lease, a lane that is not
   `remote`, and a ticket that fell back for any reason other than "no runner
   had room" or a provable never-started dispatch failure (below): an
   ineligible tree, a `remoteDeps` failure, a runner's rejection, or a
   dispatch that may have run.

   **Queued-runner rebind and dispatch history.** A dispatch that failed with
   proof the runner never started the job and that says nothing about other
   runners (the runner's queue timeout expired, the runner gave the ticket
   up, or the snapshot was never completely sent) also leaves the local
   fallback rebindable. By default only an idle runner takes a stranded
   ticket. Once the ticket has sat in this machine's queue for
   `remoteRebindMinLocalWaitMs` (global config, integer >= 0, default
   `120000`; `0` disables this path) it may also move to a runner that is
   itself queued, `1 <= queued <= maxRemoteQueue` deep (the same boundary a
   fresh dispatch uses; an idle runner is still preferred). The clock is the
   queue record's `localSince`, set when the ticket enters this machine's
   queue and reset when a failed rebind puts it back; its `seq`, priority age
   and fairness are untouched, and `createdAt` (which also counts time spent
   queued on runners) is not the clock. For a queued runner the decision to
   withdraw is taken in the same locked step as the withdrawal: if this
   ticket would be admitted locally right now (it is the head and capacity
   or its conflicting lease freed during the probe) it stays queued and
   starts here (`local capacity freed during the probe`); otherwise it is
   withdrawn and dispatched. This is a bounded policy for long-stranded
   tickets, not a finish-time prediction. Every runner a ticket is ever
   dispatched to is recorded on its attempt (`dispatchedRunners`, kept across
   fallback, restore and rebalance) and is never dispatched to again, so a
   ticket whose runners have all expired it stays in the local queue.

   **Local-first lanes (BRAIN-442).** A lane with `"remotePolicy": "local-first"`
   (default `"remote-first"`, today's behaviour) skips the initial dispatch and
   enqueues locally, keeping its remote eligibility (`remote.fallback.reason`
   is `local-first`). If this machine admits it, it simply runs here. Only once
   it has waited `localFirstWaitMs` in the local queue (lane key, else the global
   `localFirstWaitMs`, default `90000`) does late rebinding above apply to it,
   so it moves to a runner only when it would otherwise keep waiting. It needs
   `remoteRebindIntervalMs` > 0 to ever move. History rows and queue records carry
   `localFirst: true`, and the rebind log line ends `, local-first`.

   ```json
   "lanes": { "prepush": { "remote": true, "remotePolicy": "local-first", "localFirstWaitMs": 60000 } }
   ```

3. **Send a snapshot**, not history: a framed stream of exactly the listed
   files. Each file is re-read without following symlinks and its sha256
   checked before its bytes are sent. The runner validates every frame
   against the file list before writing it, into a fresh per-ticket dir,
   then re-hashes the whole tree and gives it a throwaway one-commit git
   repo (hooks off) so gates that call git still work.
4. **Run under the runner's own broker** as a fresh ticket — the caller's
   lease is never inherited, and `LANE_BROKER_LOCAL=1` stops re-dispatch.
   The runner's admission protects whatever else that machine is doing.
5. **Stream output live** (foreground stdout/stderr and `--log`), then fetch
   the result over a second ssh call. **ssh's own exit code is never
   used.** A result counts only if its protocol, ticket id, attempt
   generation and file-list hash all match what was sent.

Anything uncertain — unreachable runner, transport death mid-run, a result
that is missing or does not match, a rejected snapshot — **re-runs the
command locally**, printing `lane: remote-skip: <runner>: <reason> —
running locally`. Re-running costs CPU; reporting an unverified green would
cost the gate. A cancelled run is the one exception: it is never re-run,
and always reports `130`, whatever the command exited with.

The same rule decides a local run, including one that fell back from a runner:
a cancel the supervisor acted on (it saw the marker or a signal while the child
ran) reports `130`; a `lane cancel` that lands after the child has ended does
not change its result. A cancel that lands between admission and the child's start counts as acted on: the child is
reaped at once and the run reports `130`.

A remote run takes no local capacity or resource reservation. `lane status`
lists it under REMOTE; if its supervisor dies it shows as ORPHANED-REMOTE
(it survives a reboot), `lane wait` exits `1` naming it, and `lane cancel`
reconciles it (best-effort cancel on the runner, then a `130` result).

Runner setup: install this package (same release as the client), make
`lane` resolvable through the runner's `shell`, and install whatever the
repo's own gate needs there (for example the Playwright browsers its tests
launch). A snapshot carries no `node_modules` and no build output, so a
lane whose command needs them either installs them itself (as a pre-push
gate usually does) or declares them (below).

### Dependencies and setup on the runner (BRAIN-320)

```json
// .lane-broker.json (the repo)
"lanes": {
  "default": {
    "weight": 2,
    "remote": true,
    "remoteDeps": ["."],
    "remoteSetup": [["npm", "run", "build"]]
  }
}
```

- `remoteDeps`: dirs (relative to the repo root, `.` for the root) whose
  `package-lock.json` the runner installs from with
  `npm ci --no-audit --no-fund`, or from the runner's installed-deps cache
  when it has seen the same lockfile and environment before (below).
- `remoteDepsCache`: `false` opts this lane out of that cache.
- `remoteDepsCacheRootScriptsSafe`: `true` declares that the repo's root
  install/prepare scripts leave `node_modules` alone, so a cached tree is
  safe despite them.
- `remoteSetup`: argv arrays run in the repo root after deps, before the
  command.
- `remoteOmitEscapingSymlinks` (BRAIN-334): `true` leaves a tracked symlink
  whose target points outside the repo root (a relative target that climbs
  above it, or any absolute target) out of the snapshot instead of making the
  lane ineligible. Default `false`: such a link keeps the lane local, exactly
  as before. Only those two plain escapes are omitted; a link whose ancestor is
  a symlink, or whose target walks through another symlink entry, still
  refuses. Use it only when the lane's command tolerates the links being absent
  (an agent-brain-style repo whose `projects/*` mirrors point at sibling
  repos). The omission is submitter-side only: the runner never sees the flag
  and still rejects any escaping symlink that arrives in a manifest. Omitted
  paths are logged (`lane: remote snapshot omits N symlink(s) ...`, first 10
  names) and recorded on the history row as `remoteOmittedSymlinks` (count) and
  `remoteOmittedSymlinkNames`. Inherited through `undeclaredLanes.as`.

Both apply only when the lane runs remotely; a local run (including a
fallback) uses the local worktree as it is. A lane with either option
speaks protocol 2 and is only sent to runners that offer it; a lane with
neither speaks protocol 1 exactly as before.

On the runner, deps, setup and the command run as one pipeline under the
ticket's single admission, in one process group, so the CPU/memory grant
and cancellation cover all of them. `npm ci` runs with a controlled
environment: inherited `npm_config_*` and `NODE_ENV` are removed, and the
download cache (`<root>/npm-cache`, integrity-checked by npm and the only
thing reused between runs), user and global npmrc (`<root>/npmrc`,
`<root>/npmrc-global`) belong to the runner. A tracked repo `.npmrc` still
applies. Lifecycle scripts run, as they would locally.

Eligibility is checked on both ends: every deps dir must have a
`package-lock.json`, no `npm-shrinkwrap.json` (npm would prefer it), no
tracked files under its `node_modules`, and no two dirs may overlap. On
the client a violation keeps the run local; on the runner (which also
rejects a deps dir that is missing or reached through a symlink) it is a
rejection, which falls back.

A run is green only if the **command** phase exited 0. A failure in deps
or setup is a final red with that exit code — it is not re-run locally,
because the local tree's own dependencies could turn it green — and prints
`lane: remote failed during deps (exit N); this can also be a registry or
network failure on <runner>`.

### Installed-deps cache (BRAIN-389)

`npm ci` costs 25-45 s on every remote run, which dwarfs a short lane. The
runner keeps each installed `node_modules` under
`<root>/deps-cache/<key>/` and a later run with the same key gets a copy of
that tree instead of installing. The cache must never hand a run a different
tree than a fresh `npm ci` would, so it fails closed: anything it cannot
prove reproducible is installed normally and logged as `skip` with the
reason.

- **Eligibility**, per `remoteDeps` dir. The lockfile (`package-lock.json`,
  or `npm-shrinkwrap.json` when present) must be v2 or v3 with a `packages`
  map, and every non-root entry must be an `https://registry.npmjs.org/`
  tarball with `integrity`, a `git+...#<40-hex sha>`, or a `file:*.tgz` with
  `integrity`. `file:` dirs, links, workspaces, unresolved entries and
  anything else are refused. An entry with no `resolved` is allowed only when
  an enclosing package bundles it (the parent's `bundleDependencies`, or
  `inDepBundle`) and that parent passes the same checks; a top-level
  bundled entry gets the normal checks. Every root script `npm ci` can run
  (`predependencies`, `dependencies`, `postdependencies`, `preinstall`,
  `install`, `postinstall`, `prepublish`, `preprepare`, `prepare`,
  `postprepare`; from npm 11.9.0's `arborist/reify.js` and `lib/commands/ci.js`)
  must be an exact command on the allowlist (today only
  `git config core.hooksPath .githooks || true`), or the lane must declare
  `remoteDepsCacheRootScriptsSafe: true` ("these scripts do not touch
  `node_modules`").
- **Key**: sha256 of the lockfile and `package.json` bytes, the `.npmrc` of
  that dir and of the repo root, the bytes of every `file:` tarball the
  lockfile names, the node version, platform and arch, the glibc runtime
  version and `/etc/os-release` `ID` and `VERSION_ID`, the npm version, the
  exact `npm ci` argv, and the WHOLE deps-phase environment (plus the bytes
  of the runner's user/global npmrc). When any lock entry has an install
  script, the first line of `cc --version` and `python3 --version` too (a
  missing tool is a value). Also in the key: npm's effective `ignore-scripts` and
  `script-shell` (asked of `npm config get` in the install's own env and cwd),
  and, for each of `TMPDIR`, `TMP` and `TEMP` BY NAME (`unset`, or the fs
  type and `noexec`/`nosuid`/`nodev`/`ro` of its target; plus the
  `os.tmpdir()` fallback when `TMPDIR` is unset), the filesystem properties of
  the temp dirs (from `/proc/self/mountinfo` on Linux or `mount` on macOS),
  since a build can behave differently where it cannot execute from its temp
  dir. The paths themselves differ per run and are not hashed; they join the
  relocatability scan instead. The variables that differ on every run are
  REMOVED from the environment npm and every script see, not just left out of
  the key: `PWD`, `OLDPWD`, `SHLVL`, `_`, `GIT_CEILING_DIRECTORIES`, systemd's
  per-invocation `INVOCATION_ID`, `JOURNAL_STREAM`, `SYSTEMD_EXEC_PID`,
  `NOTIFY_SOCKET` and `MEMORY_PRESSURE_*`/`LISTEN_*`, logind's `XDG_SESSION_*`,
  and any `LANE_*` (BRAIN-423: left in, they gave every run its own key).
  Every other value is hashed verbatim. Each entry's `meta.json` records a
  digest per key input (`keyParts`, an HMAC under a host-local 0600 secret in
  the store root, never a plain hash), and a miss logs `keydiff=<labels>`
  against the newest entry, so a still-unstable input names itself. Only these named variables reach the install without being
  hashed: authentication (`SSH_AUTH_SOCK`, `SSH_AGENT_PID`, `GIT_SSH`,
  `GIT_SSH_COMMAND`) and ssh per-connection info (`SSH_CONNECTION`,
  `SSH_CLIENT`, `SSH_TTY`). Credentials decide whether a pinned git
  dependency can be fetched, not which commit it is; a pin does not constrain
  what a dependency's own lifecycle scripts generate, so every other `SSH_*`
  variable is hashed like any other. Any other change is a miss.
- **Hit**: the stored tree is COPIED into the work dir (a reflink where the
  filesystem has them), as private writable files, so nothing a lane does to
  its tree can reach the store. The store itself is read-only. The copy is
  slower than a hardlink farm and is the price of that isolation. Recorded
  install paths are then substituted: the stored copy of each recorded
  file holds placeholder tokens (`@@LANE_PREFIX_<roles>_<nonce>@@`, the
  nonce per entry in `meta.json`) where the install path was, and a restore
  replaces each token, as exact bytes, with the new path of its role (via a
  temp file renamed over the original with its mode kept; symlinks
  re-created with the substituted target). It then verifies that each
  recorded entry held a token and that no file or symlink target anywhere in
  the restored tree still holds a token of that nonce. At store time the
  published copy is verified the same way before it is committed (each
  recorded entry templated, no raw install path or token in any other file). Roles
  that shared a path at store time must still map to one path (else
  `relocation: ambiguous roles`), and every recorded role must exist. A
  failure discards the tree and installs normally, logging
  `deps-cache materialize failed, installing instead: relocation: ...`. `npm ci`
  is skipped, so the allowlisted root scripts (whose effect is on the work
  dir's `.git/config`, not on `node_modules`) are REPLAYED, in npm's order,
  by calling npm's own `@npmcli/run-script` the way `lib/commands/ci.js`
  does, with the install's env plus what `npm ci` adds (`npm_command=ci`,
  `INIT_CWD`, `npm_execpath` and friends; compared against a real `npm ci`).
  Not reproduced are the informational `npm_config_{prefix,global_prefix,
  init_module,noproxy,npm_version,user_agent}` echoes, which no allowlisted
  command reads. Nothing is replayed when npm's effective `ignore-scripts` is
  true, because a miss would not have run them.
- **Miss**: `npm ci` runs exactly as before, then the result is copied into
  the store and published with an atomic rename. Two runs missing on the same
  key at once both install; one publish wins and the other is discarded. The
  tree is NOT published when
  - the install changed a file outside `node_modules` (a hit would skip
    whatever wrote it). The only exception is the one `.git/config` line the
    allowlisted script writes (`hooksPath = .githooks`); a new hook file or
    any other `.git` write is not cached. The synthetic snapshot commit runs
    with `gc.auto=0` and `maintenance.auto=false`, because a snapshot of a few
    thousand files made git start a detached `gc --auto` that rewrote `.git` into the deps phase;
  - npm's own record (`node_modules/.package-lock.json`) lacks a lock entry
    that applies to this platform (`os`, `cpu`, `libc`), as when npm skips an
    optional dependency: `reason=incomplete-optional missing=<names>`;
  - a file names an install path (the work dir, its real path or a per-run
    temp dir) and is not rewritable text: it has a NUL byte, is not valid
    UTF-8, or its extension is not one of `.js .cjs .mjs .ts .json .map
    .prisma .txt .md .sh .yml .yaml` (or a `#!` script under `.bin/`):
    `reason=absolute-install-path-binary file=<rel>`;
  - an occurrence of an install path is not followed by a path terminator
    (`/`, a quote, a backtick, `) ] }`, whitespace, or the end), or is not preceded by a path
    start (start of file, a quote, a backtick, whitespace, `( [ {`), so `/backup/runs/job` and
    `/backup:/runs/job` never count as `/runs/job`; file-name-legal bytes such as `: = ; , \`
    on either side make the tree ambiguous: `reason=absolute-install-path-ambiguous
    file=<rel>`. A symlink target counts only when it is the install path or starts with it
    plus `/`.

    **Accepted residual:** only `/` and NUL are illegal in file names, so no byte rule can
    prove that `"/backup /runs/job/x"` names the install path rather than a directory called
    `backup ` (trailing space). The prefixes matched are the run's own per-ticket paths
    (`.../remote/tickets/<id>/work` and its temp dirs), which text only contains when it was
    generated from that path, so this case cannot arise from content not written for that run.

  Build leftovers that name the install path but are never loaded are
  neither scanned nor stored, and only these exact ones: under `<pkg>/build/`
  with `<pkg>/binding.gyp` beside it, `Makefile`, `binding.Makefile`,
  `*.target.mk`, `config.gypi`, `gyp-mac-tool`, `Release|Debug/.deps` and
  `Release|Debug/obj.target` (`build/deps/`, `build/Release/*.node` and the
  rest stay); and node-gyp's own `__pycache__` under `pylib/` where each
  `.pyc` has its `.py` beside it. A symlink pointing into a pruned path
  skips the cache (`reason=symlink-into-pruned-or-unresolvable`); every symlink is resolved with `realpath`, and one that leaves the tree or does not resolve is refused too (unless its text names an install path, which relocation handles). `npm rebuild`
  still works, as it runs `node-gyp rebuild`, which configures again.

  Every other tree that names an install path in text files or symlinks is
  published anyway (conda-style relocation, what Prisma's generated client
  needs): the decision is made once, here, against the original tree. Each
  occurrence in the stored copy becomes a placeholder token, and the places
  are recorded in `meta.json` (`relocation`: `nonce`, `prefixes` as
  `{ roles, path }` with roles sharing a path in one group, `entries` as
  `{ relPath, kind: text | symlink }`). Every file is scanned, large ones in
  chunks; a fresh nonce is drawn if the tree already holds the token text.

  The run still uses its own tree in every case, and the log says why.
- **Leases and eviction**: least recently used first, whenever a publish or a
  lease release leaves the store over `remoteDepsCacheMaxBytes` (runner
  global config, default 10 GiB). A run holds a lease on its key until its
  pipeline ends; taking and dropping a lease, and choosing and quarantining
  eviction victims, each happen under the broker lock, so a lease either
  protects its key or finds it already gone (a miss). An unreadable lease
  counts as live; one left by a dead process is ignored.
- **Config** (runner machine's global config): `remoteDepsCache` (default
  `true`) and `remoteDepsCacheMaxBytes`. A lane's `remoteDepsCache: false`
  opts that lane out.
- **Install locale** (BRAIN-429): the dependency install (`npm ci` and the
  install-time checks) runs with every `LC_*` and `LANG` removed and `LC_ALL`
  pinned to `C.UTF-8` (plain `C` on a host whose `locale -a` lacks it), and the
  pinned value is hashed as `install-locale`, so submitters with different
  locales share a key. The lane's own command still gets the submitter's locale.
- **Observability**: the run logs `deps-cache hit|miss key=<12 hex> dir=<d>
  ms=<n>` (`skip` when caching is off or refused, with a reason), and the
  result and history row carry `depsCache` (`hit`, `miss` or `skip`) and
  `depsMs` for the whole deps phase.

Not covered: native code that links against system libraries beyond glibc,
the compiler and python versions (a toolchain or library upgrade that keeps
the same versions); clear `<root>/deps-cache` after a runner OS or toolchain
change that those do not reflect.

### Cross-runner rebalance (BRAIN-436)

A ticket that has sat **queued** on its runner past `remoteRebalanceMinQueuedMs` is taken back and sent to a runner that has
room now. Only a ticket the runner has not admitted moves: nothing has run, so no deps, setup or command ran on the first
runner, and an admitted ticket never moves. It needs the runner's `remote-withdraw/1` capability (a runner without it is
never asked, and `remote-cancel` is never used in its place).

Global config (the submitter's), all integers in milliseconds except the cap:

| Key | Default | Meaning |
|---|---|---|
| `remoteRebalanceMinQueuedMs` | `300000` | queued this long on the runner before a move is considered; `0` disables rebalancing |
| `remoteRebalanceIntervalMs` | `60000` | how often the submitter looks at the runner while it waits (> 0) |
| `remoteRebalanceCooldownMs` | `600000` | the minimum gap between two moves of one ticket |
| `remoteRebalanceMaxMoves` | `1` | how many times one ticket may move |

**How a move goes.** The submitter picks the destination with the same probe dispatch uses, excluding the current runner and
every runner the ticket already left, and requires an empty queue and a *measured* free-CPU figure for the ticket (an unknown
measurement is no room). It then checks the worktree is still what was snapshotted (a changed tree is never moved),
records `moving: {from, to, phase: 'withdrawing'}` on the attempt, and runs `lane remote-withdraw` on the runner. That is one
irreversible decision taken under the runner's admission lock, so the ticket is withdrawn **or** admitted, never both. Only a
`withdrawn` answer, or the runner's own bound `unfinished/withdrawn` record fetched after a lost reply, counts as proof; every
other answer (started, cancelled, unknown) leaves the ticket where it is. On proof the attempt becomes
`moving.phase: 'dispatching'`, the dispatch is repeated to the new runner with the same ticket id and generation, and
`lane: remote-rebalance: <id>: A -> B (queued Ns on A; B X cores free)` goes to stderr and the admission log. `waitedMs` includes
the wait on the first runner, and the result and history row carry `rebalancedFrom`, `rebalancedAt`, `rebalanceReason` and
`moves` (the count).

**Guarantees.** The job never runs twice. If the submitter dies mid-move the attempt keeps its `moving` record: `lane wait` and
`lane status` report `MOVE-INTERRUPTED <id> withdrawn from <A>` instead of a generic orphan, and `lane cancel` cancels on every
runner the move names and clears it. Nothing resumes a stranded move automatically.

**Which runners are destinations.** A runner entry may set `"rebalanceTarget": false` (boolean, default `true`) to opt out of
receiving moved tickets. Free cores are a poor measure of lane capacity on a runner whose other workloads outweigh lanes (a
runner that runs simulations at a higher `CPUWeight` than lanes, deliberately), so such a runner should not look like the idle
one. It still runs tickets dispatched to it normally.

**A dispatch that may be live is never forgotten, and never re-run.** If the connection drops after a runner may have accepted
the job and nothing proves it never started (no `withdrawn` answer, no bound `unfinished`/`rejected` record), the ticket is not
run locally either. Its attempt is kept (`unresolved`) and reported by `lane status`/`lane wait` as `ORPHANED-REMOTE`, or
`MOVE-INTERRUPTED` while a move is recorded. `lane cancel` removes it only once every runner it names has confirmed the cancel
(or proven the ticket absent: a `remote-cancel`/`remote-withdraw` for a ticket the runner has not seen leaves a tombstone, and a
later `remote-exec` for that id is refused). The one exception is the result-wait expiry (BRAIN-363, `remoteResultWaitMs`): it
cancels the remote command and confirms that before any local retry, so the retry is sequential, not a concurrent second run.

**Herd avoidance is best-effort.** Several tickets can still choose the same idle runner in the same interval: the destination is
not reserved, only the runner's probe (which counts reserved cores at once) limits the pile-up. The cap, the cooldown and
the no-revisit rule keep a ticket from bouncing.

Lineage: this is requeueing of *pending* work as in Slurm (a requeue only for a job that has not started) and Kueue, and
work-stealing restricted to tasks that have not started; a started task is never stolen.

### Returning files from a remote run (BRAIN-398)

A gate that leaves a stamp for a later check (a lane stamp, a report) writes it on the runner, not in your worktree. List
those files and a remote run brings them back:

```json
"lanes": {
  "default": {
    "remote": true,
    "remoteArtifacts": ["artifacts/test-lane/default-latest.json", "reports/*.json"],
    "remoteArtifactsOn": "success"
  }
}
```

- `remoteArtifacts`: paths relative to the repo root, or simple globs (`*` within one path segment, `**` as a whole
  segment for any depth). At most 50, canonical, no `..`, no absolute paths, and each under a glob-free directory
  (`reports/*.json` is fine; `*.json`, `**/*.json` and a root-level file are config errors).
- `remoteArtifactsOn`: `success` (default, only when the command exits 0) or `always` (also when it fails; never for a
  cancelled, refused or unfinished run).
- Remote only. A local run, including a fallback, ignores both: the files are already in the worktree.

The runner, and the branch code it runs, are less trusted than the submitter, so the submitter is strict.

How it travels: after the command, and before the work dir is deleted, the runner copies the matches next to the
ticket's `result.json` (sha256 per file; nothing is created for zero files). The result carries only a summary. The
submitter then runs `lane remote-artifacts <ticketId>` over ssh (a framed stream, the same framing and reader as the
snapshot, protocol 1 exactly), checks every path, cap and sha256, and writes each file atomically at the same relative
path in your worktree, creating parent directories. On every exit path, success or not, it asks the runner to delete its
copies (`lane remote-artifacts-release`), and every `remote-exec` and `remote-probe` on a runner drops any older than
24 hours. What a runner prints to the submitter's ssh calls is captured only up to its last 64 KiB (1 MiB for the other
calls). The log gets `lane: remote artifacts: N file(s) returned: <names>`, and the history row records `remoteArtifacts`.

What it refuses, as a warning and never a change to the exit code or to the published result:

- Runner side: a protected path (below), a symlink, a directory or other special file, a path whose realpath is outside
  the work dir. Any of these refuses the whole set, as does a cap: 16 MiB per file, 64 MiB in total, 200 files (global
  config `remoteArtifactMaxFileBytes`, `remoteArtifactMaxTotalBytes`, `remoteArtifactMaxCount`; the runner and the
  submitter each apply their own). The walk stops at the count cap, and bytes are read through an `O_NOFOLLOW` fd limited
  to the cap, never trusting an earlier size.
- Submitter side: a path that is not canonical, not under the literal directory of a declared pattern it matches, or
  protected; a sha256 mismatch or an unknown stream version (the whole stream); a destination that is not a regular file
  or has ANY symlink ancestor directory; and any file git tracks, checked on the canonical spelling too. An artifact never
  overwrites tracked content, whether declared by glob or by exact path.
- Protected paths, wherever they appear: any segment equal to or starting with `.git` (so `.git` files in linked
  worktrees, `.github`, `.githooks`, `.gitmodules`), `.env*`, `.yarnrc*`, `.npmrc`, `.lane-broker.json`,
  `node_modules`, `package.json`, `package-lock.json`, `npm-shrinkwrap.json` and `*.lock`, compared case-insensitively.
- Writes go to a temp file in the destination directory; the ancestors and containment are re-checked, then the file is
  renamed into place. Residual race, out of scope: a process on the submitting machine that swaps a directory for a
  symlink between that re-check and the rename. The runner cannot touch that filesystem, and a local mutator can already
  write the worktree.
- A runner that does not advertise `artifacts/1` in `remote-probe` still runs the command; the submitter warns that
  nothing will be returned.

A warning shows in the log as `lane: warning: remote artifacts: ...` and in the history row as
`remoteArtifactsWarning`. The lane's success is about the command: a stamp that did not arrive shows up when whatever
reads it refuses.

### Queue timeout

`remoteQueueTimeoutMs` in the client machine's global config (unset by
default) bounds how long a remote ticket may wait in the runner's queue.
If the runner's broker has not started it by then, the runner expires it
(a ticket that has started always runs to completion) and the client runs
it locally instead. A user cancel still wins: exit `130`, no local run.

A ticket this machine can never run locally (a `localRefused` lane without `--allow-local-sim`, or a claim over the
local budget) carries no queue timeout: there is nothing to fall back to, so it waits in the runner's queue until it
starts instead of expiring into a refusal.

### When the ssh session drops mid-job

ssh keepalive (15 s x 4) ends a session after about a minute of silence
(network blip, laptop sleep), but the job on the runner keeps going. The
client then polls `lane remote-result` (5 s backoff, growing to 30 s) as
long as the runner reports the ticket `queued` or `running`, and uses the
result when it lands; it does not rerun the work locally. "Alive" means
the `remote-exec` process that will publish `result.json` is alive: it
records itself in the ticket's `publisher.json` (pid and start time)
as soon as the ticket dir exists, and `queued` vs `running` is only a
label. The client gives up (and falls back to local) as soon as the
runner reports `gone` (no such ticket, a dead publisher, or a ticket from
a runner version that writes no `publisher.json`), or after `remoteResultWaitMs` in the
client's global config (default 3 hours); on that expiry it first sends a
bounded best-effort `remote-cancel`, so the runner's copy does not keep running
beside the local rerun, then fetches the result once more: a result that landed
during the cancel is used as the run's outcome (only a valid one: after an
unconfirmed cancel, a late record from another generation or an incomplete one keeps
the ticket failed as below). Otherwise the runner must have
CONFIRMED the cancel (`cancelConfirmed: true` in its reply; an older runner that
prints no reply is unconfirmed) before the ticket falls back to local. If it did
not, the remote job may still be running, so the ticket ends as a failure (exit
`1`) naming the runner and saying it was not re-run, never a second execution. A runner too old to report a
state gets the previous behaviour: three back-to-back fetches, then local.
A cancel during the wait still sends `remote-cancel` and exits `130`, as does one during the expiry cancel or the late fetch.
Stale never-started ticket directories on the runner are not garbage
collected. `remote-exec` ignores SIGHUP, so a dropped ssh
session does not stop it. If `remote-exec` is killed some other way, the
ticket reads `gone` and the client reruns locally.

For a remote run, the history row's `waitedMs` is the time before the command
started, never the run and never the time the result took to reach the client
(poll backoff, an ssh-drop recovery). The runner reports its own pre-start wait
as `queuedMs` in the result (a duration on its clock, so host clock skew cannot
matter) and the client adds the local part it measured itself (enqueue until the
snapshot was handed to the runner); `startedAt` is `createdAt + waitedMs`. A
result from a runner that predates `queuedMs` falls back to receipt time minus
enqueue minus `runMs`, which also counts the delivery delay as wait. The runner's
`queuedMs` starts when its `remote-exec` starts and ends when the command starts,
so a protocol-2 run's deps and setup time counts in `runMs` (as it always has),
not in the wait. Known underestimate: `remote-exec` sends the client no early
signal, so the ssh connect and the runner process's startup (seconds) are counted
as neither wait nor run. The history row also records the runner-measured `runMs`,
which `lane suggest` uses for a run's duration instead of `endedAt - startedAt`
(`endedAt` is when the result reached the client).

## Scheduling

One atomic transaction, under a short-held global mutex: a queued ticket
starts only when **all** of:
- it is the head of the global FIFO, or the head is blocked in one of the ways
  the **Fairness and backfill** section below allows a bounded skip past,
- a queued ticket whose supervisor has already died is dequeued first, so a
  crashed supervisor can never wedge every other ticket behind it forever,
- no `RUNNING` **or `ORPHANED`** lease conflicts with its key — an ORPHANED
  lease still represents real, possibly-running work, so it blocks exactly
  like a RUNNING one (it just can't be auto-reaped; see ORPHANED handling),
- running weight (RUNNING + ORPHANED) + its weight fits the explicit or
  automatically detected capacity,
- CPU reservations plus measured external load fit the configured CPU budget,
- memory reservations and estimated growth leave the configured memory reserve,
- the load gate is open, **or `admissionLoadGate` is `false` (the default)**,
- macOS memory pressure is not `critical`,
- the broker is not paused.

**Fairness and backfill.** The queue is FIFO, and a head that cannot start may
be overtaken only in three bounded ways, each with its own allowance and each
ending in strict FIFO for that head once spent:

- *Conflict* (`conflictSkipLimit`, default 3): a head blocked by a lane-key
  conflict is skipped for the first non-conflicting ticket. Once the allowance
  is used, backfill is refused for `headBlockGraceMs` (default 10 minutes) and
  then resumes, so a multi-hour lease cannot stall everyone behind it. Except
  (`conflictSafeBackfill`, default `true`; BRAIN-355) a ticket that cannot delay
  the head still starts during that refusal: it conflicts with the head in
  neither direction (`*` included) and fits weight capacity, projected CPU and
  memory together with the head's own claim reserved. It is not counted as a
  skip and is logged as `lane-broker-head-block event=safe-backfill`. `false`
  restores the plain refusal.
  Before the allowance is used (BRAIN-365), if the first non-conflicting ticket
  is denied `projected-over-budget` (memory fine) it records that denial's
  ambient load and budget in `resource-skip-state.json`, marked
  `behindConflict`; while that record stands, the next ticket chosen behind the
  head is the smallest-CPU-claim one that fits (an elastic ticket at its
  `minCpuCores` floor), with the head's claim and weight reserved, no conflict
  with the head in either direction, and an unreadable record stopping the walk.
  If none fits, the first non-conflicting ticket is evaluated again, refreshing
  the record. Such a start still counts as a skip.
- *Capacity* (same `conflictSkipLimit`, counted separately): a head that does
  not fit the weight capacity is skipped for a ticket that does. No time-based
  resume: refusal is self-terminating because running work drains.
- *Resource* (`resourceSkipLimit`, default 3; BRAIN-346): a head denied **only**
  by `projected-over-budget` CPU (with memory fine; not cooldown, a closed CPU
  gate, memory, sample-unavailable, load gate or shadow mode) may be overtaken
  by up to `resourceSkipLimit` tickets. The head's own denial records the
  ambient load and budget it was denied against (`resource-skip-state.json`);
  the backfill candidate is the smallest-CPU-claim ticket that fits the
  remaining headroom, that does not conflict, and that would not make the head
  conflicted. It still goes through the normal, unchanged admission. The head
  keeps being evaluated every poll and starts the moment it fits. When the
  allowance is used the head is **reserved**: nothing but the head is admitted,
  and that also stops *capacity* backfill past the same head. Conflict-path
  backfill is unaffected. The head's count and reservation survive any other
  denial of the same head (closed CPU gate, memory, unavailable sample); such a
  denial only pauses backfill until the head is next denied by the projection.
  They are dropped only when the head starts, leaves the front of the queue, or
  the limit is 0 (behind the priority scheduler fence, see "Priority (ordered
  selection)" below, the front moves for reasons unrelated to the owner, so they
  are dropped only when the ticket starts or leaves the queue, or the limit is 0). A backfill whose count cannot be written to disk is
  refused, never uncounted. `resourceSkipLimit: 0` restores strict FIFO for
  resource denials.

**Bounded idle exemption** (`resourceIdleOvershootCores`, default 1; `0`
disables): ambient load can make a large lane unsatisfiable even with nothing
running (9 cores of budget, 5.4 busy, a 4-core head projects 9.4). When the
broker is fully idle (no RUNNING **or ORPHANED** lease) and the head's only
denial is `projected-over-budget` with memory admitting, the head starts if it
overshoots the budget by at most that many cores. It needs neither a record nor
a reservation, so a lone head benefits too. The two knobs are independent:
`resourceSkipLimit: 0` turns backfill and the reservation off but does **not**
disable the exemption; only `resourceIdleOvershootCores: 0` does. The check and the lease write share
one lock transaction, so two supervisors never both exempt. macOS memory
pressure `warn` or unknown does not block it, only `critical` does (as for every
start). It is logged as `current=start:resource-idle-exempt`.

### Elastic CPU claims (`minCpuCores`, BRAIN-360)

A lane may declare `"minCpuCores": <n>` (a positive number, at most its
`cpuCores`, or its `weight` when `cpuCores` is unset; both `cpuCores` and
`minCpuCores` are capped at 1024; a config that violates this is refused by
name, including through an `undeclaredLanes` template). Without it, nothing below applies and every
decision is byte-for-byte what it was.

If the lane's full `cpuCores` does not fit but the CPU projection
(`projected-over-budget`, memory fine) is the ONLY thing denying it, admission
re-runs the complete predicate at each smaller INTEGER claim: from the largest
integer below `cpuCores` that also fits the CPU headroom the full-claim
evaluation measured, down to `ceil(minCpuCores)` (a fractional floor rounds up;
a grant is never fractional and never below the floor), over the same held leases, sample, gate and cooldown, and
admits at the first that passes. Nothing else is ever relaxed: a memory,
conflict, weight-capacity, load-gate, closed-CPU-gate, cooldown, pause or
unavailable-sample denial waits exactly as before, memory is always reserved at
the full declared `memoryBytes`, and `weight` always counts in full (a partial
grant frees no capacity slots; the conservative choice, since `weight` is the
exclusivity unit and not a CPU measure). If even `minCpuCores` does not fit, the
lane waits with the same `projected-over-budget` denial (and the existing
backfill, reservation and idle-exemption behaviour, which judge the full
claim). Active `schedulerMode` only; shadow never denies, so it never relaxes.

An elastic grant never exceeds this host's CPU budget (BRAIN-362). In active mode the
claim an elastic ticket is evaluated and charged at is capped at `floor(budget)`
(`capElasticClaim`), so every admission path that admits
the full claim (a cold sample, the idle exemption, a plain fit) grants at most that,
and the lease, history and the child's `LANE_BROKER_CPU_CORES` report it. The
lease keeps the declared `cpuCores` beside the grant. Because grants are whole
cores, `lane run` also refuses at once (exit 64, the usual budget message) an
elastic lane whose smallest grant, `ceil(minCpuCores)`, exceeds `floor(budget)`
(e.g. a floor of 3.2 on a 3.4-core budget), since it could never start. A
non-elastic claim over the budget is refused as before, never capped; a
non-elastic fractional claim is charged as-is and is unaffected. A budget that
shrinks after `lane run` accepted the ticket, below its floor or to under one
core, never caps the claim below `ceil(minCpuCores)` (or to zero): the ticket is
denied `elastic-below-floor` before any exemption (cold sample, idle overshoot)
could admit its full claim, and keeps waiting in case the budget grows back. As the
queue head it is recorded futile (BRAIN-418), so it holds no reservation and smaller
tickets backfill past it. The queue record keeps the declared claim; the cap is
applied where the scheduler and `lane status` read it, against the budget at that
moment, so a budget that grows back raises the grant too.

The lease records `grantedCpuCores` and keeps `resources.cpuCores` as the
declaration. Everything that charges a lease's CPU uses the grant: the CPU
projection (including settled demand, BRAIN-354), safe-backfill head
reservations (BRAIN-355), `lane status`, and `history.jsonl`
(`grantedCpuCores`, additive beside `resources`) and on the result
(`result.json`). The lease, status entry and `result.json` carry the grant only for lanes that declare
`minCpuCores`; since BRAIN-425 every history row records `grantedCpuCores` (the declaration for a non-elastic lane). The child gets the grant in
`LANE_BROKER_CPU_CORES`, so a test runner sizing its workers from it uses what
it was actually given. `lane status` marks a short grant on its RUNNING line
(`cpu=2/4 (elastic)`), and the admission log line gains `declaredCpu=` and an
`elastic-grant` event only when a grant was reduced.

Queue fairness: an elastic head is granted a partial claim when it reaches the
head (the point of the feature). Backfill never uses elasticity to get ahead:
resource-backfill selection still requires the candidate's FULL claim to fit
the head's denial headroom, a reserved head refuses every backfill candidate
regardless, and a safe backfill (BRAIN-355) is judged with the blocked head's
full claim already reserved, so a smaller grant cannot delay the head.

Remote: a runner applies the same admission (it is the same code). The submitter
sends `minCpuCores` in the exec header (only for an elastic lane), and the
runner's grant comes back as an additive `grantedCpuCores` on the remote result,
so the submitter's result and history record it too. `remote-probe` advertises
the `elastic-claims/1` capability and reports `reservedCpuCores` (grants, not
declarations). The client's static "never fits" runner check uses the floor only
for a runner that advertises `elastic-claims/1`; any other (older) runner is
judged on the FULL claim, since it would be dispatched the full claim and refuse
it terminally (exit 64) if its budget is smaller. Upgrade runners before relying
on elasticity.

### `history.jsonl` record schema (BRAIN-425)

One JSON object per line, appended when a run reaches a terminal outcome. Every field except the identity and timing ones is
additive: readers must ignore unknown keys and tolerate absent ones.

| Field | Meaning |
|---|---|
| `id`, `key`, `repo`, `lane`, `configLane`, `weight`, `resources`, `command`, `headTree` | identity and the declaration (`configLane` only for an ad-hoc lane inheriting a template) |
| `executor`, `runner`, `localReason`, `remoteKind`, `remotePhase` | where it ran (`local`/`remote`), which runner, why local, and the runner's result kind/last phase |
| `exit`, `signal` | present on **every** finished row: `exit` is the code (130 for a cancel, `null` when killed by a signal or never run), `signal` the terminating signal or `null` |
| `cancelled`, `reason`, `error` | cancel flag; refusal/reap reason; supervisor-level error text |
| `startedAt`, `endedAt`, `waitedMs`, `queuedAt` | epoch ms; `waitedMs` is queue wait (`null` when unknown) |
| `grantedCpuCores`, `grantedMemoryBytes` | what the lane was charged: the elastic grant, else the declared reservation |
| `observedCpu` `{peak, mean, samples}`, `observedRssPeakBytes` | heartbeat observations (see above) |
| `cpuSeconds` | CPU seconds of the lease's process tree, an **estimate**: the heartbeat core samples integrated over time, the first reading held back to the start and the last forward to the end. Not rusage: `ps` pcpu is a decayed average and a supervisor cannot read reaped grandchildren's rusage |
| `depsCache`, `depsMs` | remote deps phase outcome (`hit`/`miss`/`skip`) and wall time |
| `remotePhasesMs` | remote runs: `uploadMs` (submitter writing the snapshot), `snapshotMs` (runner receive + verify), `setupMs` (deps + setup), `commandMs`, `artifactsMs` (runner collection), `artifactReturnMs` (submitter fetching them back). Each present only when measured |
| `remoteArtifacts`, `remoteArtifactsWarning` | returned artifact names / why they were not |
| `remoteOmittedSymlinks`, `remoteOmittedSymlinkNames` | remote runs with `remoteOmitEscapingSymlinks`: how many escaping symlinks the snapshot left out, and the first 10 paths |
| `priorityRequested`, `priorityAdmitted`, `priorityDemoted`, `effectiveRankAtStart`, `scoreAtStart` | priority audit (admitted tickets only) |
| `dequeuedDeadSupervisor` | marks a row for a queued ticket whose supervisor died (`exit: null`, `signal: null`) |

A runner's `result.json` carries the same `cpuSeconds` and a `phasesMs` object; the submitter folds them into its own row.

### Observed CPU, the OVERRUN flag and `lane suggest` (BRAIN-361)

Every heartbeat already observes a lease's whole process tree (CPU and RSS). Two
things are now kept from it, report-only: **nothing here is read by admission**.

- **`observedCpu` in `history.jsonl`.** A finished run's row carries
  `observedCpu: { peak, mean, samples }` (cores, 3 decimals) and
  `observedRssPeakBytes`. `peak` is the highest heartbeat reading; `mean` is
  **time-weighted** (each reading is held until the next, so a long quiet stretch
  outweighs a short spike); `samples` is the reading count. Additive: old readers
  ignore the fields. Since BRAIN-425 the supervisor also samples once ~1s after spawn and once when the leader exits, so a run shorter than one heartbeat still has a reading (only a run that ends before that first sample has none). A remote run
  carries them back on the remote result (like `grantedCpuCores`), so the
  submitter's history has them too.
- **`OVERRUN` in `lane status`.** A RUNNING lease whose observed cores stay above
  1.25 x its charged cores (its elastic grant, else its declared `cpuCores`)
  continuously for 2 minutes is marked `[OVERRUN peak <n> for <duration>]`; one
  reading back under the threshold resets the clock, so a single spike never
  flags. Each RUNNING line also shows `cpu <observed>/<booked>`. `--json` adds
  `bookedCpuCores` and `overrun: { sinceMs, observedPeak } | null` per lease.
- **`lane suggest [--repo <name>] [--days N=7] [--json]`.** Groups history by
  repo + lane; for groups with at least 5 finished runs that have `observedCpu`,
  prints the declared `cpuCores` against the p50 and p90 (nearest-rank) of the
  runs' per-run time-weighted MEAN cores (`mean p50/p90`), with the same of the
  PEAK cores (`peak p50/p90`) shown as information only. `suggested = ceil(p90 mean)`,
  never below 1 core. The basis is the mean because admission charges the booked
  cores for a lease's whole life and OVERRUN means sustained >1.25x for 2 minutes;
  a peak is a brief spike (a parallel tsc or lint step), so sizing from it books
  capacity no run holds for more than seconds. `--json` carries `meanP50`/`meanP90`
  alongside `p50Peak`/`p90Peak`. (The floor: config
  accepts any positive `cpuCores`, so 0.5 is legal; the UNDER/over label is judged on
  the unclamped ceil(p90 mean), so an idle lane booked at 0.5 is not flagged under-booked).
  The declared figure is the newest row's. `UNDER-BOOKED` means
  the unclamped p90 exceeds 1.25 x declared (`OVERRUN_FACTOR`, the tolerance OVERRUN
  uses, so both reports agree on "over its booking"; ceil rounding alone, like a p90
  of 4.15 on a booking of 4, is `ok` though `suggested` still shows 5);
  `over-booked` means suggested <= 0.5 x declared. Rows without `observedCpu`
  (older history, sub-heartbeat runs) are skipped and counted in the last line.
  An ad-hoc lane name that inherits a declared lane (`undeclaredLanes: {"as": ...}`)
  records `configLane` on its history row (only when it differs from `lane`), and
  `lane suggest` groups by `configLane ?? lane`, reporting how many ad-hoc names were
  folded in. Older rows without it that are too few to report on their own pool by
  identical `cpuCores`+`memoryBytes`+`minCpuCores` within a repo, labelled `(by resources)`.
  Two rules keep the mean honest (BRAIN-371). **Short runs:** a run under 2 minutes
  (`OVERRUN_MIN_MS`, the same bar OVERRUN uses) is excluded and counted as
  `shortRunsExcluded`, because its mean is mostly process startup, not sustained load
  (jun `focused` runs have a ~5 s median). A group left with fewer than 5 sustained
  runs prints `insufficient sustained runs (N short excluded)`, and in `--json` has
  `suggestedCpuCores: null` and `verdict: 'insufficient'`. **Elastic grants:** a row
  with `grantedCpuCores` was granted that many cores (possibly below declared) and its
  workers sized from the grant, so its raw mean understates need (rouge sim: declared
  4, granted 2, mean ~2). Its need is `mean / granted x declared` (declared being that
  row's own `cpuCores`), used in place of the mean for the percentile; peak stays
  unscaled. `elasticRuns` counts such rows and `grantedBelowDeclaredPct` is the share of
  sustained runs granted below declared; text output appends
  `elastic: X% of runs granted below declared` when it is above 0. Rows that can't be
  measured are excluded and counted, never guessed: `unfinishedRuns` (no finite start or
  end time) and `malformedRuns` (an elastic row whose declared cores aren't a positive
  number). So no NaN reaches the output.
  It never edits config.

Resource backfill events are explicit `lane-broker-head-block` lines in
`admission-decisions.log`, written after the lease is published:
`resource-backfill-start skipPast=<head> count=<n>`, `resource-reserved`, and
`resource-idle-exempt`. `current=start:ok` is not an admission signal for them.
`lane status` shows a `resource-blocked:` line (count/limit, reserved,
projected vs budget) while the head has a record.

Assumptions and limits. Backfill selection looks at CPU claims only; it does not
pre-check memory, so a small-CPU, large-memory ticket can be selected and then
denied by normal memory admission, holding back larger-CPU tickets meanwhile.
Nothing guarantees the head ever starts if external CPU never drops or a lease
never ends; the reservation only stops it being overtaken. A back-to-back CPU
sample whose counters did not advance reuses the last valid measurement while
it is younger than `sampleMs/2`, but only when the read itself is valid and its
counters are exactly unchanged on the same raw core count (a malformed or
regressing read, or a topology change, stays unavailable) (and does not advance the CPU gate's
hysteresis) rather than reading as "unavailable, admit". Supervisors only
enforce the reservation once running the new version: during a mixed-version
rollout an older supervisor can still backfill past a reserved head.

**Load gate**: closes immediately when the 1-minute loadavg exceeds
`loadClose`; reopens only after `loadOpenSamples` consecutive samples (spaced
`sampleMs` apart) below `loadOpen`. This hysteresis means a single low sample
right after a spike does not reopen it. The gate is always sampled and always
logged, but with `admissionLoadGate: false` (the default) it is
**informational only** — it never denies a start, `lane status` marks its
line `[informational]`, and the admission log carries `loadGateIgnored=true`
whenever a closed gate would otherwise have mattered. Set
`admissionLoadGate: true` to make it a hard gate again.

**Memory brake**: an always-on, unconditional check — not gated by
`admissionLoadGate` — that denies a start (`memory-critical`) when macOS
reports `kern.memorystatus_vm_pressure_level` as critical. A missing or
non-critical reading (including on non-macOS platforms, where this is always
a no-op) never denies.

**Reentrancy**: the supervisor exports `LANE_BROKER_LEASE=<id>` and
`LANE_BROKER_KEY=<key>` to the child. A nested `lane run` reuses the inherited
lease (no new acquisition, no deadlock) when either:
- it requests the *exact same* key, or
- it requests lane `prepush` under an inherited lease of the *same repo*
  (any lane) — so a pre-push hook wrapped in `lane run --lane prepush` works
  regardless of which lane it nests inside.

A reentrant run may not *widen* the inherited lease's weight, CPU reservation,
or memory reservation. Any other nested key — a
different repo, or a different lane that isn't `prepush` — is refused with
exit `64`; v1 has no general lane hierarchy.

### Priority (foundations, BRAIN-380 slice 1)

Without the scheduler fence (next section) the scheduler is strictly FIFO: tiers
and the priority clock are recorded but reorder nothing, and `lane status` says so
(`priority: inactive (legacy scheduler; run lane migrate-scheduler)`). Each
queued ticket persists `priorityRequested`, `priorityAdmitted` (the requested tier,
unless the per-repo high cap demoted it, see below), `priorityDemoted`, `prioOriginAt` and
`schedVersion: 2`. The resolved tier is exported to the lane child (and to a
reentrant child) as `LANE_BROKER_PRIORITY`; a `remote-exec` ticket ignores the
runner shell's value and is `medium`.

The priority clock is `nowEff = max(wall clock, hwm)`, where `hwm` is a
persisted high-water mark (`priority-hwm.json` in the state root) advanced under
the broker lock on every locked evaluation and every enqueue. `lane run` takes
the lock briefly to stamp `prioOriginAt = nowEff`, so a wall-clock step backwards
freezes a ticket's age instead of reversing it, and a ticket created during the
rollback starts at zero age. `createdAt` and every deadline derived from it stay
on the wall clock. `lane status` reads the mark and never writes it, and shows
each queued ticket's tier plus its effective rank once it has aged a step.

Global config (all optional): `priorityAgingMs` (integer in `[60000, 3600000]`,
default `600000`: one tier of age per period), `priorityAgeMaxMs` (integer in
`[priorityAgingMs, 86400000]`, default `2 x priorityAgingMs`),
`priorityWeights` `{tier, age, fairshare}` (`tier` and `age` in `[0.01, 100]`,
`age >= tier`, `fairshare` exactly `0`; defaults `2/2/0`) and
`maxQueuedHighPerRepo` (non-negative integer, default `1`; `0` demotes every high).
`src/priority.js` holds the pure score (`min(ceiling, W_tier*tierFactor +
W_age*ageFactor)`, where the ceiling is `W_tier` for a high and `0.99 * W_tier` for a
non-high until it has waited 3 x `priorityAgeMaxMs`, the starvation horizon, default 60 min,
so a fresh high strictly outranks an aged medium/low until then; this is score order only,
since reservation promotion, backfill, unreadable-record barriers and the sim-after-test rule
can still start a non-high first) and `orderQueue`; see "Priority (ordered selection)" for where they are live.

### Priority (ordered selection, BRAIN-380 slice 2)

Everything here runs only while a **valid** `sched-v2.json` (`{"version": 2,
"migratedAt": <ms>}`) is in the state root. Without one the scheduler is the
legacy FIFO over the three singleton skip files, unchanged. An unreadable,
malformed or wrong-version fence or `fairness-v2.json` also runs legacy mode and
logs `lane-broker-head-block event=scheduler-fence-invalid reason=...` (once per
process). `lane migrate-scheduler` (below) writes the fence.

- **One ordered view.** Each locked evaluation orders the queue once, with
  `nowEff = max(wall clock, hwm)`: `(score desc, seq asc)` within each run of
  readable records, never across an unreadable record. The active reservation
  owner, if any, is then moved to index 0 (never across an unreadable record,
  which suspends its reservation for that evaluation, nor while the owner is
  class-ineligible: `effectiveView` takes an `eligible(ticket)` filter that defaults
  to allow-all because BRAIN-379 allocation is shadow-only, and is to be wired to
  its enforcement when that goes live). Every selector (conflict,
  capacity and resource walks, BRAIN-365's conflict backfill, BRAIN-355's safe
  backfill) and the allocation shadow snapshot receive that one array, so
  "head" means index 0 of it. "Smallest fitting claim" ties go to the earlier
  ordered-view position. `lane status` runs the same pipeline read-only, shows
  `priority: active`, positions from that array and the promoted owner.
- **Per-ticket fairness.** `fairness-v2.json` maps ticket id to one record per
  blocking reason (`conflict`, `capacity`, `resource`), each
  `{reason, skipsCharged, ...}`: conflict adds `blockedSince`, `graceStartedAt`
  and `loggedPhase`; capacity adds `loggedPhase`; resource adds `reserved`,
  `reservationSeq`, `inScope`, `behindConflict`, `deniedAt`, `budget` and
  `externalBusy`. The skip budget applies to the current head using its own
  counter. A displaced ticket's counter pauses and its conflict grace keeps
  running from its own `graceStartedAt`. A record is deleted only when the ticket
  departs (starts, is cancelled, expires or is reaped; removal is noticed on the
  next evaluation, except while an unreadable queue record hides which ids
  left). The singleton files are never read or written behind the fence.
- **One active reservation.** A reservation record carries `reservationSeq`,
  drawn from the monotonic queue counter when the resource allowance is used
  up, never a timestamp. The lowest `reservationSeq` whose owner is queued and
  not behind an unreadable record is active and promoted; the others are
  dormant (kept with their counters, reserving nothing). A higher-priority
  arrival does not take it over. It is released when the owner starts or leaves
  the queue, or `resourceSkipLimit` is 0.
- **Backfill.** BRAIN-355 safe backfill stays poll-driven: whichever ticket
  polls and proves it cannot delay the effective head may start, an exception to
  priority order that makes no promise about other queued tickets.

### Migrating to the priority scheduler (BRAIN-380 slice 3)

Priority ordering stays off until `lane migrate-scheduler` writes the fence. The cutover is explicit, drained and
fail-closed, and it is **per broker**: it migrates the state root of the machine it runs on (`$LANE_BROKER_STATE`), so
the Mac and each runner are migrated separately, each in its own drained window. Upgrade the Mac and both runners
together, and migrate each (a mixed-version fleet is "no worse than today", see Scheduling).

Run **`lane migrate-scheduler --when-idle [--timeout <duration>]`** (default timeout 6h). It drains the broker, then
pauses, migrates and resumes in one step, and leaves nothing half-done on any outcome:

1. It writes a `draining` marker (atomically, under the lock). From then on every NEW intake refuses with exit 75 and
   "scheduler migration pending (draining)": `lane run` ticket creation, remote dispatch, attempt creation, `enqueue`
   and `remote-exec` on a runner. Tickets already in the queue and leases already running are NOT affected: the broker
   keeps admitting and finishing them, which is the point. A ticket created a moment before the marker but not yet
   queued is refused at its `enqueue` with the same exit 75 (retry it). `lane remote-probe` reports the runner as
   `draining` (and `paused`, for older submitters), so a submitter skips it the way it skips a paused runner, and
   `lane status` shows "draining for scheduler migration (pid N, since T)".
2. It polls every 5 seconds until the broker is quiescent (the same checks as the migration, below, minus "paused"),
   printing a line each time the counts change: `draining: queued 2, running 1, attempts 0, lane processes 4`.
3. At quiescence, in one hold of the lock, it pauses the broker, runs the migration unchanged (marker, checks,
   `fairness-v2.json`, the queue fence, the scheduler fence), resumes, and removes the `draining` marker, so nothing can
   slip in between. If the broker was already paused when it started, it stays paused at the end. A check that fails
   only because something became busy again (a stray `lane status` is a lane process) sends it back to waiting.
4. It exits 1, with the `draining` marker removed and the pause state exactly as it found it, on the timeout and on any
   refusal that cannot clear by waiting (an unreadable record, an unreadable process table, another migrator). Ctrl-C
   and SIGTERM do the same (exit 130 / 143). Already migrated: it exits 0 at once.

A broker that is paused while queued tickets exist never drains (a paused broker starts nothing); the command says so
and waits out its timeout. `lane resume` first, or let it drain unpaused.

**A SIGKILLed `--when-idle` leaves its `draining` marker behind.** The marker holds `{pid, startTime, startedAt}` (a drain
that cannot read its own start time refuses to start, so no marker can outlive a reused pid), so it is recognised as
stale (pid dead, or reused by a process with another start time) and then it refuses nothing. When the drain paused the
broker it also records `pausedByDrain: true` first and writes its own reason ("paused for scheduler migration (lane
migrate-scheduler --when-idle, pid N)") into PAUSE. Whoever clears a stale marker (the next `lane run`, under the
lock; a re-run of `--when-idle`, before or after the fence) resumes the broker only if the marker says the drain paused
it AND PAUSE still holds that exact reason: a pause an operator set is never lifted. `lane status` reports a stale marker.
(A kill in the sub-second migration window leaves the existing "crashed migrator" `migrating` marker too, below; intake
stays refused until a re-run finishes the job.)

**An unreadable or malformed `draining` file fails closed.** If the marker cannot be read or parsed it cannot be judged:
new intake refuses, the probe reports `draining`, `lane status` says so, no `lane` command deletes it, and
`--when-idle` will not start over it. If no `lane migrate-scheduler --when-idle` is running, remove it by hand
(`rm "$LANE_BROKER_STATE/draining"`, default `~/.cache/lane-broker/draining`).

**The manual path** is the same sequence, and its order matters: `lane pause` stops the broker admitting anything,
including tickets that are already queued, so a paused broker never empties its queue. Wait for idle FIRST, with the
broker unpaused (`lane status`: nothing queued, nothing running; a lease left by a crashed run counts until an
admission poll reaps it), and only then, with nothing new able to arrive, run it back to back:

1. `lane pause`, then at once `lane migrate-scheduler --dry-run` (reports every precondition as `ok:` or `refused:`,
   changes nothing). If anything slipped in, `lane resume` and wait again.
2. `lane migrate-scheduler`.
3. `lane resume`. The broker stays paused after a manual migrate, on purpose.

Without `--when-idle`, intake is not stopped while you wait, so under steady load the window may never come: use
`--when-idle`.

Under one hold of the broker lock the command writes a `migrating` marker, then checks (all of them, fail-closed):

- the broker is paused;
- the queue is empty;
- no lease is `RUNNING` or `ORPHANED`;
- no attempt record is left in `attempts/` (a supervisor still probing or dispatching a remote attempt is invisible
  to the queue);
- no live process runs lane-broker's `bin/lane.js`, `src/supervisor.js` or `src/remote-pipeline.js`, whatever its
  version. Every argv token of every process is examined (so `node --require x.js /old/bin/lane.js` is caught) and
  symlinks are resolved; a process that merely mentions one of those paths, an editor for instance, blocks it too.
  The check reads the machine's process table, so a lane process serving a different state root blocks it too.
  The migrator and its ancestors are excluded. It is a snapshot, taken once, so it is defence in depth: the queue
  fence below is what stops an old process that starts after it.

Lease, attempt and queue records are read strictly: an unreadable record refuses the migration instead of being
skipped. On any failure the marker is removed, the broker stays paused, and the command exits 1 naming every failed
check with its ids or pids. While the marker exists, `lane run`, remote dispatch, `enqueue`, attempt creation and
`remote-exec` on a runner all refuse with exit 75 and "scheduler migration in progress" (the `draining` marker refuses
the same entry points, with "scheduler migration pending (draining)").

If everything holds, it writes `fairness-v2.json` (`{"version": 2, "tickets": {}}`, all there is at a drained
point), then fences the queue (below), then writes `sched-v2.json` (the commit point), each fsynced along with its
directory, then renames the legacy
`conflict-skip-state.json`, `capacity-skip-state.json` and `resource-skip-state.json` to `*.migrated-<timestamp>`
and removes the marker. Re-running is safe: `fairness-v2.json` without a fence redoes the cutover, and a valid fence
prints "already migrated" and exits 0. A marker left by a crashed migrator is taken over by the next run.

**The queue fence.** Old code ignores `migrating` and the scheduler fence, so the migration also makes old code fail by
itself. It renames the (empty) `queue/` directory to `queue.legacy-<timestamp>/` and creates a regular FILE named
`queue` holding "lane-broker scheduler migrated to v2; upgrade lane-broker". New code keeps its queue in `queue-v2/`
(every queue read and write goes through `paths().queue`; `seq` stays shared). An old `lane run` then fails at its
first `mkdir queue`, and an old supervisor that was already past it fails at its first queue write (ENOTDIR). Old
`tryStart` starts only a ticket it finds in the queue, so no old process can be admitted. If a valid fence is found
together with a non-empty legacy `queue/` directory (a crash or manual tampering), new code refuses admission with an
error naming the directory rather than guessing.

A migrator that dies after the fence leaves its `migrating` marker behind. Re-running `lane migrate-scheduler` sees the
valid fence, removes the marker (fsyncing the directory) and reports "already migrated (recovered)".

Behind the fence, a queue record without `schedVersion` (only an escaped pre-migration process can write one) is
moved to `queue-quarantine/`, logged as `lane-broker-head-block event=legacy-record-after-fence`, and never
selected. Its supervisor finds its queue file gone and exits as cancelled. If the quarantine rename itself fails, the
record is still never selectable: it reads as an unreadable-record barrier for that evaluation and the failure is
logged as `action=quarantine-failed`. A valid fence over a missing or malformed `fairness-v2.json` (every field a
selector reads is type-checked) runs the legacy scheduler and logs `scheduler-fence-invalid`. Turning resource
backfill off (`resourceSkipLimit` 0, or shadow mode) releases every reservation latch; turning it back on makes a
ticket earn one again.

### The high cap, the remote hop and the audit (BRAIN-380 slice 4)

**The high cap.** At most `maxQueuedHighPerRepo` (default `1`) tickets of one repo can sit in the queue as `high`.
`enqueue` counts them under the same state-root lock that allocates the sequence number and writes the queue record
(so two racing `lane run`s cannot both see "none queued"), across lanes and worktrees. A queued high whose supervisor
is dead (pid plus start time, a read-only check) does not count; `enqueue` never reaps or otherwise changes other tickets.
A high at or over the cap is admitted as `medium`: the record keeps `priorityRequested: high` and gets
`priorityAdmitted: medium`, `priorityDemoted: true`, and `lane run` prints
`lane run: priority high demoted to medium (repo already has a queued high ticket)` to stderr. A demoted ticket does
not hold the slot. The cap is recorded in legacy mode too, but it only reorders anything behind the scheduler fence.
The lane child sees the admitted tier in `LANE_BROKER_PRIORITY`.

**The cap is per broker.** Remote dispatch happens before the local enqueue, and every runner has its own broker and
its own cap. A repo can therefore hold one queued high on the Mac and one on each runner at the same time. That is
deliberate.

**Remote.** The submitter adds two fields to the `remote-exec` header: `priorityRequested` (the tier before any cap)
and `priorityAccruedMs` (`max(0, nowEff - prioOriginAt)` on the submitter's own priority clock, so it includes any
probing or waiting before dispatch; ssh and snapshot transfer time is not counted). No wall-clock timestamp crosses
the hop, so clock skew between hosts cannot change a ticket's age. The runner validates both (an unknown tier is
`medium`; a wait that is not an integer in `[0, 86400000]` is `0`), applies its own cap in its own `enqueue`, and
sets `prioOriginAt = runnerNowEff - priorityAccruedMs` on its own clock. It never touches `createdAt`, and it never
reads the runner shell's `LANE_BROKER_PRIORITY`. The probe advertises the capability `priority/1` next to
`elastic-claims/1`. A runner without it ignores the fields and its tickets are effectively `medium`; runner
selection does not change. If the remote attempt falls back to local, the local ticket keeps its own original
`prioOriginAt`: the time spent on the attempt counts once, through the local clock, and no remote age is added.

**Audit.** When a ticket is admitted, its lease records `priorityRequested`, `priorityAdmitted`, `priorityDemoted`,
`effectiveRankAtStart` and `scoreAtStart`, and its terminal `history.jsonl` row carries the same fields next to
`waitedMs`. Each `admission-decisions.log` decision line also ends with `headTier=`, `headRank=` and `headScore=`
(the queue head's tier, effective rank and score at that evaluation). Polls by a ticket that is not up are still not
logged. Tier and rank describe what the scheduler saw. They cannot show how much time a tier saved anyone, and
nothing here claims it: compare wait distributions by tier, and do not read a causal effect into them.

### Estimates engine and `lane estimates` (BRAIN-408 slice A)

`src/estimates.js` is the pure estimates engine of the unified queue (spec 5.7): per-key EWMA
(alpha 0.1, ratio winsorised to [0.2, 5]) over `ln(ref-s)`, censored runs (signal kills,
timeouts) that can only raise an estimate, the cold-start chain (exact, no-code-family,
no-arm-set, no-through-act, runner-wide, class-default; sigma x1.5 per step), speed factors
per host, work class and bucket (mac-grandy pinned at 1.00; `rebaseAnchor` moves the anchor
after 30 days uncalibrated), overrun residuals and per-bucket remaining-work conversion.
The first observation's sigma is 0.6 (p90 about 2.2x p50). It does no I/O and nothing reads
it yet.

`lane estimates [--json] [--root <stateHome>]` folds `history.jsonl` through it, read-only,
keyed by (repo, lane): n, censored count, p50/p90 (seconds), `est_source` and the p90 of
`observedRssPeakBytes`. A row that never started, was cancelled or failed on its own is
skipped (counted); a run killed by an unrequested signal is censored. History rows carry no
class, so a lane named like a sim (`sim`, `sims`, `sim-*`) is a sim, else a test. The host
factor is 1.0 (no calibration) and is printed.

### Exclusive lanes (BRAIN-403 v1)

`lane run --exclusive`, or `"exclusive": true` on a lane in `.lane-broker.json`, runs the command with the
whole host to itself. v1 is deliberately stateless and hookless: there is no hold file and nothing to wedge, and
the only durable facts are the ticket's `exclusive` flag and the full-budget lease it is admitted with.

A non-exclusive lane whose `weight` exceeds the global `maxLaneWeight` (integer, default 4, the same on every host) is refused
with exit 64 at every submission, a runner's intake included (BRAIN-452: declare `exclusive: true`, or use `cpuCores` for size),
so a submission valid on one host is valid on all of them. A ticket queued earlier by an older config is admitted clamped to
`min(maxLaneWeight, floor(capacity / 2))` (logged as `event=weight-clamped`), and `lane status`'s `queue stalled` line names the
oversized weight when it is why the head does not fit.

Semantics:

- **While it is the head of the queue, nothing else is admitted.** The queue's effective head (after priority
  ordering and reservation promotion) being exclusive denies every other ticket with `exclusive-head` before any
  selector runs, so no backfill of any kind (conflict skip, capacity skip, resource backfill, conflict-safe or
  sim-safe backfill) gets past it. The running lanes finish and are never killed; the exclusive head reports
  `exclusive-draining` until the last one leaves. A non-holder still gets its own queue-timeout and withdrawal
  result: those checks run before the gate.
- **It starts only as the effective head**, never as a skip or backfill candidate.
- **It claims the whole admission budget.** Once admitted, its lease carries `exclusive: true`,
  `weight` = the effective weight capacity and `resources` = the whole CPU budget and memory budget (the
  declared claim is kept as `declaredResources`). Every ordinary candidate is then denied `exclusive-held` by an
  explicit check, independent of the capacity arithmetic (a `1e-20` weight cannot slip in beside it), and
  `couldAdmitNow` refuses too. It skips the load gate, the CPU projection, `externalBusy`, the idle-overshoot
  rule and class caps. A critical memory reading still denies it: there is no acquire hook to clear one.
- **Priority is `high` by default.** `--priority` > `LANE_BROKER_PRIORITY` > the lane's `priority` > `high` (instead of
  `medium`). The per-repo high cap can still demote it. An exclusive earns **no age credit**, so aging never promotes
  one past a higher-priority arrival.
- **Local only.** An exclusive is never dispatched or rebound to a runner. A runner (`lane remote-exec`) ignores a
  lane-config `exclusive` in an incoming snapshot, with a note on stderr, so a snapshot can never hold a runner.
- **No nesting.** An `--exclusive` run under an inherited lease of any other lane exits `64` before enqueue (it
  would wait on its own parent forever). A same-key nested run is reentrant as it is today.
- **`exclusive` is a boolean on the lane** and is not inherited through an `undeclaredLanes.as` template.
- `lane status` shows `HOLD (exclusive) <id> (<key>) waiting for N running lane(s)` while it drains, `[exclusive]` on
  its QUEUE line and on its RUNNING entry once admitted; `--json` adds `exclusiveHold` and `exclusive: true`. The
  history row carries `exclusive: true`. `exclusive/1` is in the capability list.

Hooks are **not supported yet**. `exclusiveHooks` (or `acquire` / `release`) in the host config is refused at `lane run`
submission (exit `64`: "not supported yet (BRAIN-403 follow-up)"), and the same keys in a repo config are refused
through the ordinary config exit-`64` path. Acquire/release hooks and their crash-safe lifecycle are a follow-up.

Documented limits, not bugs:

- **Not a durable latch.** The head is recomputed each decision. A NEWER ticket cannot displace a queued high
  exclusive (equal score, and the older sequence wins), but an OLDER queued medium or low that has waited past the starvation horizon (3 x `priorityAgeMaxMs`), or a
  BRAIN-346 reservation owner, can still go first.
- **Starvation.** An explicitly `low` exclusive, or one demoted by the high cap, can starve behind sustained
  higher-priority arrivals.
- **Mixed versions.** During an upgrade, supervisors still running older code do not see the gate; it holds only for
  tickets admitted by code at or after this release.
- **A capacity raise mid-run** leaves room beside the full-budget lease for the capacity arithmetic, but the explicit
  held-exclusive check still refuses everything else; raising the capacity is an operator action either way.
- `drainMs` (time from becoming head to admission) is not recorded in v1.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | The command's own exit code (or a normal `0`). |
| the child's exit code | Passed straight through on completion. |
| `1` | The command errored, was signalled, or the supervisor died unexpectedly. |
| `2` | Bad CLI usage (missing command / argument). |
| `64` | Nested `lane run` would widen the inherited lease — refused; or an invalid `--priority` / `LANE_BROKER_PRIORITY` / lane `priority`; or an `--exclusive` lane nested under another lane; or `exclusiveHooks` in host or repo config (not supported yet). |
| `69` | Local-sim lane refused (fleet-offload message); use `--allow-local-sim`. |
| `76` | A remote-only (`localRefused` + `remote`) lane found no runner able to take it right now (BRAIN-455). Retryable, unlike `69`; `--allow-local-sim` runs it here. |
| `75` | `--timeout` elapsed while still queued/running — **"waited, not failed."** Not a test failure; report it as such. Also `lane run` / `remote-exec` refused with "scheduler migration in progress" while `lane migrate-scheduler` runs, or "scheduler migration pending (draining)" while `--when-idle` waits. |
| `124` | Killed by the no-progress watchdog (`reason: "no-progress"`, see below). |
| `130` | Cancelled (SIGINT/SIGTERM) while still queued, before the lane ever started. |

### No-progress watchdog (BRAIN-431)

A run that hangs holds its slot indefinitely (a vitest pool that stopped mid-run once sat silent for 100 minutes and
blocked a push). Every supervisor heartbeat observes the lease's tree, and an interval is **idle** when the command wrote
no output (stdout + stderr), no process joined or left the tree, and the tree's CPU delta stayed under 0.05 CPU-seconds
per minute (scaled to the interval). CPU is the per-process cumulative-time delta by identity (on Linux including reaped
children's `cutime`/`cstime`), not `ps` pcpu, a lifetime average that stays high long after a process goes idle. Anything
else is progress and restarts the streak, and so is anything the supervisor could not observe (a failed descendant scan,
an unreadable process table, a member with no row); the first observation only sets the baseline. Timing is monotonic.

**The stall is always recorded; the kill is opt-in.** The idle streak is stamped on the lease as `noProgressSinceMs`
(`lane status` shows `no-progress=<t>` from a minute on, `--json` carries the field), and the history row records the
longest streak as `maxNoProgressMs`, for every lane, so monitoring can alert on a stall without anything being killed.
The kill is off by default (global `noProgressTimeoutMs: 0`) because legitimate work can look idle to a process-tree
observer: slow network I/O, a daemon-backed `docker` build whose CPU lives outside the tree, output buffered until the end,
work written only to other files. Opt a lane in by setting `noProgressTimeoutMs` in `.lane-broker.json`; the right lanes
are test or verify lanes whose output streams (a vitest/jest run, a `verify` script), with a timeout well above their
longest legitimate quiet stretch. Do not opt in a lane that builds images, downloads, or buffers its output.

| Setting | Where | Default | Meaning |
|---|---|---|---|
| `noProgressTimeoutMs` | global `config.json` | `0` | Consecutive idle time that triggers the kill; `0` never kills (the stall is still recorded). |
| `noProgressTimeoutMs` | a lane in `.lane-broker.json` | the global value | Per-lane value (an undeclared lane inherits its `as` template's); `0` disables for the lane. |

On trigger the tree is reaped as for a cancel (TERM, grace, KILL, descendants) through the one reap a cancel or leader
exit also uses (a cancel arriving mid-reap starts no second one and its `exit: 130` result wins). The result and history
row carry `exit: 124`, `reason: "no-progress"` and `noProgressTimeoutMs`, and the admission log gets
`lane-broker-reap id=<id> descendants-reaped=<n> reason=no-progress`. The submitter sees `lane run: no progress for 15m
(no output, no CPU) — killed` (also appended to the lease log). A remote run uses the **submitter's** resolved timeout
(its lane, else its global value), sent to the runner in the dispatch header, so a runner's own config never changes it;
the runner relays the reason and window back, and the submitter's history row is `executor: "remote"`, exit 124,
`reason: "no-progress"`.

### Group reap on leader exit (ROG-2181 T1b)

When a lease's leader process (the command `lane run` spawned) exits on its own, the supervisor checks whether
anything is still alive in its process group. If so it TERMs the group, waits the cancel grace period, KILLs and
verifies the group is gone, and only then writes the result and releases the lease, so a surviving descendant never
runs unleased. The result and the history row carry `reason: "leader-exited-group-reaped"`. A run whose group is
already empty at exit is unchanged. Advertised as `group-reap/1`.

### Descendant reap (BRAIN-419)

A command can start processes in their own session or process group (Playwright's `webServer` through turbo/pnpm
does), which a group kill never reaches. While a lease runs, each heartbeat reads the process table once and records
every member of the lease's tree, with a start token, on the lease (`descendants`). A process is a member when it
descends from the leader or from a recorded member (ppid walk), or when its environment carries
`LANE_BROKER_LEASE=<lease id>`, which the leader's descendants inherit even after being reparented to init (read from
`/proc/<pid>/environ` on Linux; on macOS a second `ps -E` listing with each pid's plain command stripped as a
prefix, so an argument that merely spells the marker never matches). macOS hides the environment of SIP-protected
binaries, so one of those, detached by a short-lived leader and reparented before any scan saw it, cannot be found;
the ppid walk covers it only if a scan observed it first. `lane cancel` and a leader exit TERM the group plus every live member, rescanning each
iteration so a replacement spawned in a TERM handler is caught, wait the cancel grace period, KILL what remains for
up to 3 s more, and log `descendants-reaped=<n>` to the admission log. A member whose pid now has a different start
token is never signalled, and neither are the supervisor, its parent or processes outside the lease. If the table
cannot be read, or members survive the KILL, the log also carries `descendants-reap-incomplete survivors=<pids>` and
the lease is still released. A lease whose supervisor and group are gone but whose members are alive stays ORPHANED and heals itself on
the admission polls: TERM on the first pass, KILL after the grace period, release once none are alive (or 30 s after
the KILL, or 60 s after the TERM if the table is unreadable, with the incomplete-reap line); `lane cancel` does the same synchronously. The residual window between validating a pid and signalling it is accepted: Node has
no pidfd signalling, and macOS pids climb to 99999 before wrapping. Likewise, on macOS a process whose argv changes between the
two `ps` reads, keeping its old command as a prefix and appending this lease's exact random id, would be misread as a member.

## Work classes and per-machine caps (BRAIN-321)

A lane's `class` (`test`, the default, or `sim`) already keeps a sim from passing a waiting test. The optional global
`classes` block adds per-machine RESERVATION caps on the sim class, so a fleet of long sim shards can never take the
cores a test needs:

```json
"classes": {
  "mode": "active",
  "test": { "reserveCores": 4 },
  "sim":  { "capCores": 8, "maxTickets": 8, "capMemoryBytes": 10737418240 }
}
```

- `mode` is `off`, `shadow` or `active`. An absent block is today's behaviour, byte for byte. `shadow` and `active` need
  every number spelled out; unknown keys are refused.
- Caps are counted against BOOKED grants: the claims of the live `class:sim` leases (an elastic lease at its grant) plus the
  candidate's own final grant, never demand estimates or observed CPU. In `active` mode a sim is denied `class-cap` when
  booked sim cores + grant exceed `capCores`, the sim count is at `maxTickets`, booked sim memory + grant exceed
  `capMemoryBytes`, or booked sim cores + grant exceed `cpuBudget - test.reserveCores` (sims never book into the test
  reserve). An elastic sim is lowered to what the caps leave but never below its `minCpuCores`; an immutable claim is
  never shrunk, only denied.
- A `class-cap` denial is skip-free: the sim leaves every selection, so it is never the head, never blocks a test or any
  other ticket, and cannot hold a reservation-owner promotion. At an equal effective score a test is ordered before a sim.
- `shadow` computes the same verdict and writes `classCap=class-cap(shadow):<reason>` on the admission-log line, without
  denying anything.
- **Grant fence.** Only `active` mode with a valid block stamps `classEnforcement: {mode, generation, stateRoot, configHash}`
  on a sim lease, and exports it to the leased child as JSON in `LANE_BROKER_CLASS_ENFORCEMENT`. `generation` is
  `sched-v2@<migratedAt>` of the scheduler fence when the root has one, else `pkg-<version>`. A pool gate that must only
  claim work under live caps checks that (and `lane capabilities --json`'s `classes`) before claiming.
- **`classEnforcement: "required"` (lane key in `.lane-broker.json`).** A ticket from such a lane (the rouge `fleet` lane) is
  granted ONLY when `classes.mode` is `active` and the global config loaded cleanly; otherwise it is denied
  `class-enforcement-unavailable`, skip-free (never the head, never blocks anything), and switching to `off` or `shadow`
  stops new grants for it at the next pass. A sim lane WITHOUT the key keeps today's behaviour in `off`, `shadow` or absent
  mode, so `shadow` stays useful for measuring. The granted lease carries `classEnforcement` for any required ticket.
- **Fail closed.** A `classes` block that fails validation is set aside (the rest of the config still loads) and every sim is
  denied `class-config-invalid` until it is fixed; it never reads as "no caps". Any unreadable or invalid global config (a reload that fails,
  or a fresh supervisor starting on a malformed file) marks the class policy unknown the same way, rather than reading as `off` or
  keeping stale caps, but only on a host where classes were ever configured (a durable `classes-configured` marker in the state
  dir, or a block in the previous config); elsewhere an unreadable config behaves as before. A config file that VANISHES after
  a `classes` block was in force keeps the fence until a valid file returns. An inaccessible config (EACCES, an unsearchable
  parent directory) is no longer read as "no file, use defaults": a running supervisor keeps its PREVIOUS settings (an
  intentional difference from 0.28.x, which silently reset them), `lane status` reports `config: global config unreadable`, an
  orphaned-remote `lane cancel` keeps the attempt and exits 1 (it cannot reach the runner), and a runner's `remote-probe` answers
  with `configError` and `paused` so submitters skip it ("runner config unreadable"). `lane status` and
  `lane capabilities --json` show `valid: false`.
- `lane status` prints `classes (<mode>): sim booked/cap cores, tickets, memory; test booked/budget` in `shadow` and `active`.

## `lane capabilities --json`

Prints one JSON line: `{"version", "capabilities": [...], "schedulerMode", "admissionMode", "classes": {"mode", "configHash", "stateRoot", "valid"}}`. `capabilities` is the
same list `lane remote-probe` advertises (`elastic-claims/1`, `priority/1`, `artifacts/1`, `sim-safe-backfill/1`,
`lane-aging/1`, `group-reap/1`, `remote-withdraw/1`, `exclusive/1`, `attempt-logpath/1`: a remote attempt record carries the submitter's `logPath` and `cwd`, so a caller that crashed between `lane run --detach` and learning the id can find its attempt by `--log` path, BRAIN-462, `classes/1`). `classes` reports the work-class caps in force (see "Work classes and per-machine caps"). `schedulerMode` is `priority` when a valid `sched-v2.json` fence is present and
`legacy` otherwise; `admissionMode` is the global config's `schedulerMode` (`active` or `shadow`). A caller that needs
a feature (for example a pool gate that requires `group-reap/1`) checks this before relying on it.

## ORPHANED handling

A lease is reaped (removed) only when **(the supervisor is dead AND its child
process group is dead AND no recorded descendant is alive) OR the machine's boot id has changed** (a reboot). PID
existence alone never decides this — process start-time is compared against
what was recorded at spawn time, to defeat PID reuse.

If the supervisor dies but the child group is still alive, the lease is
marked **ORPHANED** and is *never* auto-reaped — `lane status` flags it, and
the scheduler continues to treat it as held (see Scheduling above): it blocks
conflicting keys and counts against capacity exactly like a RUNNING lease.
Use `lane cancel <id>` to tear it down: it TERMs the process group, waits a
grace period, KILLs, verifies the group is gone, and only then releases the
lease. This is intentionally conservative: an ORPHANED lease still represents
real, possibly-important, running work.

`lane cancel` only ever reports success once it has verified the outcome. If
the lease's supervisor is alive but unresponsive (stopped, starved) and does
not release the lease within its grace period, `lane cancel` exits non-zero
and reports the lease as still held, rather than silently declaring success —
it does not take over killing the group out from under a supervisor that
might still resume and act on its own.

## The current-load caveat

The load gate reads the *current* machine-wide loadavg, not "load this broker
caused." On a heavily loaded machine (many concurrent agent sessions, none of
them going through `lane run`), the broker will refuse every new lane
immediately and keep refusing — correctly, but it cannot repair processes it
did not start. Bringing load back down (stopping stray sessions, `pkill`ing
runaway processes) is a human job; `lane status` shows the current load and
the queue so you know what's waiting and why.

## State

`$LANE_BROKER_STATE` (default `~/.cache/lane-broker`): `leases/`, `queue/`,
`priority-hwm.json` (the priority clock's high-water mark), `migrating` (present only while `lane migrate-scheduler` runs), `draining` (present only while `lane migrate-scheduler --when-idle` waits for the broker to go idle), `queue-quarantine/` (see below), `sched-v2.json` and `fairness-v2.json` (the priority scheduler fence and its per-ticket fairness records), `logs/` (per-run, capped at 50MB with a truncation notice), `results/` (both pruned, see Garbage collection),
`history.jsonl` (one line per completed run, plus one `dequeuedDeadSupervisor: true`
row `{id, key, error, supervisorPid, endedAt, executor}` (`error: "supervisor died while queued"`, so a history reader counts it as an error, not a failed run) when a queued ticket whose
supervisor died is dropped from the queue; it is written only once the queue
record is actually removed), and `PAUSE` (present while paused). All writes are atomic (temp file + rename) and never follow
symlinks.

## Garbage collection (BRAIN-438)

What a run leaves behind is collected; what a live one owns never is. Everything here is also available on demand as
`lane gc [--dry-run] [--max <n>] [--root <remoteRoot>]` (both sweeps, unbounded unless `--max`, one JSON summary line;
`--dry-run` reports and removes nothing).

- **Runner ticket GC** (global config `remoteTicketRetentionMs`, default 7 days; `remoteGcMaxTicketsPerRun`, default 10).
  Runs at the start of every `lane remote-exec` and from `lane gc`. A ticket's `work/` and tmp dir (recorded in its
  `tmp-dir` file) are removed once it has a terminal `result.json`; the whole ticket directory is removed once it is older
  than the retention. Either way only if all of these hold, checked under the broker lock: its `publisher.json` pid is
  dead, its broker ticket (`remote-id`) has no lease and no queue entry, no cancel/withdraw/expiry marker is pending for
  it, and its id has no tombstone. A directory is first renamed into `<remoteRoot>/gc-trash` inside the lock and deleted
  afterwards (detached from `remote-exec`, so the submitter's ssh session never waits on it). `tickets/` and `gc-trash/` must
  be real directories inside the runner root (a symlink is refused, and only real directories inside `gc-trash` are ever
  deleted). At most `remoteGcMaxTicketsPerRun` directories are acted on and `remoteGcMaxExaminedPerRun` (default 50) even
  looked at per `remote-exec`, resuming from a persisted cursor (`gc-cursor`) so all are eventually visited.
- **Tombstones are never pruned.** A tombstone (BRAIN-436) is the only thing refusing an exec for an id that was cancelled
  before it arrived, and nothing bounds how late that exec can be (a suspended process or long-lived ssh session is not
  covered by `ConnectTimeout`). Expiring one would let it recreate and run a cancelled ticket. They are tiny files. GC also
  never touches a tombstoned id's ticket directory.
- **Logs and results** (`logRetentionMs`, default 14 days; `logGcMaxFilesPerRun`, default 5000). Only broker-generated names
  are pruned: `logs/<uuid>.log` and `results/<uuid>.json` older than the retention, whose uuid has no lease, queue entry or
  attempt record. A custom `lane run --log <path>` file is never touched, so no alias or new-user race exists. `logs/` and
  `results/` must be real directories inside the state root; otherwise that sweep is skipped with one
  `lane-broker-gc warning` line in the admission log. Runs from `lane gc` and, at most once a day (marker `gc-last-run`), at a
  supervisor's start, never on the admission path. A sweep that stops at its bound leaves the marker unclaimed so the next
  supervisor continues.
- **Tmp dirs.** `remote-exec` writes `.lane-broker-owner` (the ticket id) inside each ticket's tmp dir. GC deletes a recorded
  tmp dir only if it is a real `lb-*` directory directly under `/var/tmp` carrying a marker naming that ticket; a legacy
  dir without a marker, or with another ticket's, is left and reported as `tmpSkipped`.
- **Threat model.** Accidental configuration (a symlinked directory, a stale recorded path, a custom log path) and ordinary
  concurrency between broker processes. A same-uid adversary swapping directories for symlinks during a sweep is out of scope:
  they can already delete anything that uid owns.
- **Off the dispatch path.** `remote-exec` schedules ticket GC after it has read its header and created its ticket (a later
  event-loop turn), so dispatch latency never includes it; failures go to the broker log.
- **`admission-decisions.log`.** Each line starts with an ISO-8601 UTC timestamp. A `lane-broker-admission` line is written
  only when a candidate's (current decision:reason, new decision:reason) differs from the last one written for it (kept in
  `admission-last.json`; an admitted candidate is forgotten). The log rotates to `admission-decisions.log.1` at 10 MiB,
  replacing the previous `.1`. Readers that match a line by content (`candidate=...`) are unaffected by the prefix; one
  an unchanged decision is written again every 60 seconds (`ADMISSION_LOG_REFRESH_MS`, 60 s; polls are every 5 s, so dedup still
  cuts ~12x), so a waiting candidate's line in the tail is never more than 60 s stale.

## Testing hooks

- `LANE_BROKER_HOME` / `LANE_BROKER_STATE` — relocate config/state (used by
  the test suite to isolate runs).
- `LANE_BROKER_LOADAVG_FILE` — if set, `lane` reads the 1-minute load from the
  first line of this file instead of `os.loadavg()`, for deterministic gate
  tests.
- `LANE_BROKER_BOOT_ID` — override the detected boot id, to simulate a reboot.
- `LANE_BROKER_TEST_ROUNDS` — override the dead-owner-lock contention sweep's
  round count (default 4; the original suite ran 10) for a fuller sweep, e.g.
  `LANE_BROKER_TEST_ROUNDS=10 npm test`.
- `LANE_BROKER_TEST_LOCK_TIMEOUT_MS` — overrides `withLock`'s default 15s
  acquire deadline, so a test can force a lock-acquire timeout (BRAIN-345: a
  queued supervisor must treat one as contention and keep polling).
- `LANE_BROKER_TEST_PAUSE_AFTER_TICKET_ID` — if set, `lane remote-exec` pauses
  right after writing its ticket's `remote-id` file AND checking its own
  ticket-local `cancelled` marker (finding it not yet set), until the named
  file appears -- so a test can deterministically land `lane remote-cancel`
  inside the window that check cannot see: after `remote-id` is recorded but
  before the supervisor (not yet spawned) has enqueued or leased that same id
  with the local broker.
- `LANE_BROKER_TEST_PAUSE_BEFORE_WAIT_ORPHAN` — if set, `lane wait` pauses,
  right after finding no result yet and an attempt record whose supervisor
  is dead, until the named file appears -- so a test can deterministically
  land a supervisor's publish-then-exit (result written, attempt removed)
  inside the exact gap between wait's own unlocked reads of result.json and
  the attempt's liveness, the race BRAIN-319 fixed (a completed run
  observed mid-publish must never be reported as ORPHANED-REMOTE).
- `LANE_BROKER_TEST_PAUSE_AFTER_QUEUE_TIMEOUT` — if set, a queued supervisor
  pauses right after `tryStart` returns `reason: 'queue-timeout'`, before its
  own recheck of the durable cancel marker, until the named file appears --
  so a test can deterministically land a `lane cancel` write inside the
  window between an expiry being recorded and the expiry being finalized
  (BRAIN-320 review fix B: a cancel that lands in that window must still win
  over the expiry).
- `LANE_BROKER_TEST_HOLD_AT=local-finalize` (with `LANE_BROKER_TEST_HOLD_READY` and
  `LANE_BROKER_TEST_HOLD_GO`) — a local supervisor pauses after its child has ended and its
  output is drained, before writing the result, so a test can land a `lane cancel` in that
  window (BRAIN-364: it must not change the child's own outcome). `local-admitted` pauses after admission,
  before the spawn, to land a cancel there instead.

## `lane-store` (BRAIN-407, unified queue P0b)

`bin/lane-store.js` is the content-addressed snapshot store behind detached submit (spec `.spec/unified-queue-spec-rev11.md`
8.1-8.3, 9.3). Zero dependencies (`http`, `crypto`); code in `src/store/`; systemd templates (not installed) in `ops/lane-store/`.

- **Objects.** Blobs live at `blobs/ab/cd/<sha256>`: streamed to `tmp/`, hashed while written, refused on mismatch or over
  `--max-blob-bytes`, fsynced, renamed, then journaled. A re-upload is hash-checked and deduplicated, never overwritten.
  Job manifests (`manifests/<job>.json`, immutable per job, every referenced blob must already be stored) register the file list.
- **Reads are job-scoped.** A `read` token carries `job`; it may fetch only that job's manifest and the blobs it references.
  Tokens are verified by a pluggable `verifier` (`createStore({verifier})`); `src/store/auth.js` is an HMAC stand-in
  (`LANE_STORE_SECRET`) until the DB-backed principal check lands. Roles: `submit`, `read`, `replica`, `admin`.
- **Receiver.** `materializeSnapshot` (`src/store/materialize.js`) replays the blobs as the same framed stream a remote runner
  gets, so path/symlink/hash refusal is `remote-manifest.js` / `remote-stream.js`, not a second implementation.
- **Replication runs inside the primary.** `serve --replicate-to URL` (token `LANE_STORE_REPLICA_TOKEN`, role `replica`)
  starts a loop in the serving process, one round every `--replicate-interval-s` (default 300). It ships `journal.log`
  records in `seq` order over HTTP (the replica is a second lane-store; it hash-verifies on write and again on
  `GET /verify/<path>`, which re-reads its own disk), reading the journal only through the in-memory committed view.
  Nothing outside the serving process reads the journal while it runs (no marker file). Exclusive ownership is
  kernel-held on Linux (the production hosts): the process binds an abstract Unix socket named after the store root, so a
  second owner gets `EADDRINUSE` and a dead process (even SIGKILL) frees it instantly, with no stale state. On macOS
  (dev and tests only) it is a best-effort link-created `store.lock` plus a per-write self-check fence (`LockLostError`).
  The file lock refuses to run on Linux, and only `ObjectStore.open` constructs a store, so each platform has one lock
  mechanism. The lock guards against concurrent processes using the entry points (`lane-store serve`, the CLI); the
  internal classes (`ObjectStore` with a hand-built lock, `Journal`) are not a security boundary against code that sets
  out to bypass it.
  An offline tool (`rebuild-watermark`) takes the same lock and refuses while a server holds it. At open the journal is fsynced before it becomes readable.
  `replicated_seq` (`replication.json`, `{format: 2, seq, offset, ...}`) advances only after the replica's own check matches.
  Metric `oldest_unreplicated_object_age_seconds` (plus `lane_store_replicated_seq`, `_journal_head_seq`,
  `_replication_failed_rounds`, `_lost_objects`, `_bytes`) is served live at `GET /metrics`. `lane-store compare [--deep]
  [--repair] --url URL` (weekly timer) asks the running primary to compare itself with its replica (`POST /admin/compare`);
  exit 2 on divergence or a lost object. An unrecognised `replication.json` refuses to start: recompute it offline with
  `lane-store rebuild-watermark --root DIR --seq N`.
- **Lost objects.** Retention deletes in three durable steps: `delete-intent`, unlink, `delete`. At startup an object that is
  missing with an intent gets its `delete` journaled (interrupted retention). An object missing with NO intent is a loss:
  it is never tombstoned (that would delete the replica's intact copy), replication of that path blocks, and it is counted
  in `lane_store_lost_objects` and compare's `lostAtPrimary`. Recovery is an operator step: re-upload the blob
  (`PUT /blobs/<sha>`), or fetch it from the replica (`GET /blobs/<sha>` with a replica token) and PUT it to the primary.
- **Deletions are never replicated.** Replication ships puts (blobs, manifests) and pin/terminal state only; `delete-intent`,
  `delete` and `delete-cancel` records stay local. A put whose object is gone from the primary is skipped only if the journal
  holds a committed `delete` for it later; otherwise replication blocks there. The replica runs its own time-based retention
  (`serve --replica [--replica-grace-hours N]`, default 336 = the primary's 14-day terminal retention, so `compare` never reports a replica sweep as missing): it sweeps a manifest only when the replicated state says terminal and
  unpinned and it received it more than the grace ago, and a blob only when no remaining manifest references it and it is
  older than the grace, so a manifest still in flight keeps its blobs. A stall longer than the grace is already alerting via
  `oldest_unreplicated_object_age_seconds`. (`compare` can therefore report `missingAtReplica` for primary objects the replica
  already swept; the primary keeps terminal manifests 14 d.) Registering a manifest or a duplicate upload cancels a pending
  delete-intent for the blobs it needs (`delete-cancel`).
  Store-wide: nothing under the root may be a symlink (ancestors lstat-checked, leaves opened `O_NOFOLLOW`, root `realpath`ed);
  manifests count toward `--cap-bytes`, and admission reserves declared bytes atomically.
  Accepted residual risk: `O_NOFOLLOW` protects only the final path component, so an ancestor directory swapped for a symlink
  between the lstat check and the open is not caught; exploiting it needs a local actor with write access to the store root.
  A store started with `--replica` keeps no in-memory journal tail.
  `POST /admin/sweep` (and `--sweep-interval-s`): manifests expire 14 d after `PUT /jobs/<job>/terminal`,
  pinned jobs (`PUT /pins/<job>`) never expire, unreferenced blobs go after 24 h, above 80% of `--cap-bytes` the oldest
  terminal unpinned groups are evicted, and uploads are refused (507) at 95%.

## Verify locally

```bash
npm install
npm test
npm run lint
```

`npm run lint` is `node --check` — a syntax parser, not a full linter. It
catches parse errors only (no unused vars, no shadowing, no undefined
globals); the pre-push hook's real coverage is `npm test`.

Before committing, run `npm run verify` — it runs exactly what the pre-push
hook runs: lint, then the suite through `lane run` when the broker is on
PATH (falling back to a direct `npm test` otherwise). That distinction
matters — a test can pass standalone and still fail only under `lane run`,
so a `verify` that just called `npm test` directly would miss exactly the
failure the hook is there to catch.

## License

[MIT](./LICENSE)

### Running the tests

`npm test` runs the suite with `--test-concurrency=1`. That is deliberate, not
timidity: every test here drives **real** processes — detached supervisors,
process groups, SIGSTOP/SIGTERM/SIGKILL, an 8×200 mutex contention hammer, a
200k-line log tee. Running the files in parallel multiplies that by the number
of workers and the suite starves itself: `no-backfill` then times out waiting
for a lease that a scheduled-out CLI has not written yet, on a machine where it
passes standalone in under two seconds. A test suite for a tool that exists to
stop parallel processes from starving each other should not itself be a
thundering herd.
