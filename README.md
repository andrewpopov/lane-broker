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
lane status [--json]
lane cancel <id>
lane wait <id> [--timeout 5m]
lane pause "reason" | lane resume
```

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
  "memoryReserveBytes": 2147483648,
  "defaultMemoryBytesPerWeight": 1073741824,
  "loadClose": 15,
  "loadOpen": 11,
  "loadOpenSamples": 3,
  "sampleMs": 5000,
  "admissionLoadGate": false,
  "laneNice": 10
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

`admissionLoadGate` (default `false`): whether the load gate is allowed to
deny a start at all. With the default, the gate is still sampled and reported
every poll (`lane status` shows it, `[informational]` suffixed), but it never
blocks a start — not for an idle broker, not for one already holding
non-conflicting leases. Set `true` to make a closed gate hard-deny again
(pre-BRAIN-207 behaviour, including the BRAIN-197 idle exemption).

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

A lane with `localRefused: true` (the `sim` lane by default, matching the
rouge fleet split) is refused on `lane run` unless `--allow-local-sim` is
passed — print a fleet-offload message and exit `69` instead.

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
  { "name": "grandy", "ssh": "mac-grandy", "shell": "zsh -lc" }
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

`ssh` is an ssh destination (an alias from `~/.ssh/config`, or
`ssh://user@host:port`); it may not start with `-`. `shell` (default
`bash -lc`) wraps every remote command so a non-interactive ssh finds
`node` and `lane`. `root` (default `~/.cache/lane-broker/remote` on the
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
2. **Pick a runner**: probe each in config order (`lane remote-probe`, 6 s
   hard deadline) and take the first that is reachable, speaks the protocol
   the lane needs (below), is not paused, has no queue, and could ever fit
   the lane's reservation (weight, CPU budget, memory plus its reserve, from
   the runner's static capacity — never its momentary load). None → local.
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
  `package-lock.json` the runner installs from with a fresh
  `npm ci --no-audit --no-fund` on every run.
- `remoteSetup`: argv arrays run in the repo root after deps, before the
  command.

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

### Queue timeout

`remoteQueueTimeoutMs` in the client machine's global config (unset by
default) bounds how long a remote ticket may wait in the runner's queue.
If the runner's broker has not started it by then, the runner expires it
(a ticket that has started always runs to completion) and the client runs
it locally instead. A user cancel still wins: exit `130`, no local run.

## Scheduling

One atomic transaction, under a short-held global mutex: a queued ticket
starts only when **all** of:
- it is the head of the global FIFO (strict FIFO — no backfill behind it),
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

## Exit codes

| Code | Meaning |
|---|---|
| `0` | The command's own exit code (or a normal `0`). |
| the child's exit code | Passed straight through on completion. |
| `1` | The command errored, was signalled, or the supervisor died unexpectedly. |
| `2` | Bad CLI usage (missing command / argument). |
| `64` | Nested `lane run` would widen the inherited lease — refused. |
| `69` | Local-sim lane refused (fleet-offload message); use `--allow-local-sim`. |
| `75` | `--timeout` elapsed while still queued/running — **"waited, not failed."** Not a test failure; report it as such. |
| `130` | Cancelled (SIGINT/SIGTERM) while still queued, before the lane ever started. |

## ORPHANED handling

A lease is reaped (removed) only when **(the supervisor is dead AND its child
process group is dead) OR the machine's boot id has changed** (a reboot). PID
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
`logs/` (per-run, capped at 50MB with a truncation notice), `results/`,
`history.jsonl` (one line per completed run), and `PAUSE` (present while
paused). All writes are atomic (temp file + rename) and never follow
symlinks.

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
