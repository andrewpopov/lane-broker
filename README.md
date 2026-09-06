# @andrewpopov/lane-broker

A machine-wide **test-lane coordinator**. Agents on a shared laptop run heavy
test suites with no awareness of each other; `lane run` serializes conflicting
lanes, caps how many heavy lanes run at once, and refuses to start new work
while the machine is already overloaded — instead of every agent guessing.

Zero runtime dependencies — Node `child_process`/`fs`/`os` only (Node ≥ 20).

## Install

```
npm install -g github:andrewpopov/lane-broker#v0.1.0
```

This installs the `lane` command.

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
| `--lane <name>` | Lane name (`default` if omitted); looked up in `.lane-broker.json`. If the repo config declares `lanes`, an undeclared name is refused (exit `64`) rather than silently keying on a private, unconflicting lane. |
| `--weight <n>` | Override the configured weight for this run; must be a positive number (validated before enqueueing, exit `2` otherwise). |
| `--detach` | Print the run id and return immediately instead of waiting. |
| `--timeout <duration>` | e.g. `30s`, `5m`, `500ms`. Exit `75` if not finished in time — see below. |
| `--allow-local-sim` | Override a lane's `localRefused: true`. |
| `--log <path>` | Override the default log path. |

**Every run gets a detached supervisor**, always. `lane run` spawns it, then
only *waits* on the supervisor's result file. If the calling agent (or its
10-minute tool ceiling) kills the `lane run` process, the supervisor and the
running suite are unaffected — `lane status` still shows it RUNNING, and a
later `lane run` on the same key waits behind it. `lane wait <id>` reattaches;
this is the sanctioned way to outlive a short tool ceiling on a long lane.
`lane wait` on an id that is neither queued, leased, nor resulted fails fast
(after a short grace period, to avoid racing a `lane run --detach` that
hasn't finished enqueueing yet) rather than blocking forever on a typo.

## Config

**Global** — `$LANE_BROKER_HOME/config.json` (default
`~/.config/lane-broker/config.json`):

```json
{
  "version": 1,
  "capacity": 2,
  "loadClose": 15,
  "loadOpen": 11,
  "loadOpenSamples": 3,
  "sampleMs": 5000
}
```

**Per repo** — `.lane-broker.json`, discovered by walking up from `cwd` to the
git worktree root (repo identity is the realpath of `git rev-parse
--git-common-dir`, so every worktree of a repo shares one identity and one
set of leases):

```json
{
  "version": 1,
  "lanes": {
    "default": { "weight": 2 },
    "sim":     { "weight": 2, "localRefused": true },
    "lint":    { "weight": 1 },
    "e2e":     { "weight": 2 },
    "prepush": { "weight": 2 }
  },
  "conflicts": [["sim", "*"]]
}
```

`conflicts` is a list of `[laneA, laneB]` pairs; `*` means "every other lane
in this repo". Two lanes with the *same key* (same repo + lane name) always
conflict, regardless of `conflicts`. With no config file, there is a single
`default` lane of weight 2.

A lane with `localRefused: true` (the `sim` lane by default, matching the
rouge fleet split) is refused on `lane run` unless `--allow-local-sim` is
passed — print a fleet-offload message and exit `69` instead.

## Scheduling

One atomic transaction, under a short-held global mutex: a queued ticket
starts only when **all** of:
- it is the head of the global FIFO (strict FIFO — no backfill behind it),
- a queued ticket whose supervisor has already died is dequeued first, so a
  crashed supervisor can never wedge every other ticket behind it forever,
- no `RUNNING` **or `ORPHANED`** lease conflicts with its key — an ORPHANED
  lease still represents real, possibly-running work, so it blocks exactly
  like a RUNNING one (it just can't be auto-reaped; see ORPHANED handling),
- running weight (RUNNING + ORPHANED) + its weight fits `capacity`,
- the load gate is open,
- the broker is not paused.

**Load gate**: closes immediately when the 1-minute loadavg exceeds
`loadClose`; reopens only after `loadOpenSamples` consecutive samples (spaced
`sampleMs` apart) below `loadOpen`. This hysteresis means a single low sample
right after a spike does not reopen it.

**Reentrancy**: the supervisor exports `LANE_BROKER_LEASE=<id>` and
`LANE_BROKER_KEY=<key>` to the child. A nested `lane run` reuses the inherited
lease (no new acquisition, no deadlock) when either:
- it requests the *exact same* key, or
- it requests lane `prepush` under an inherited lease of the *same repo*
  (any lane) — so a pre-push hook wrapped in `lane run --lane prepush` works
  regardless of which lane it nests inside.

A reentrant run may not *widen* the inherited lease's weight (a `--weight`
higher than the inherited lease's is refused). Any other nested key — a
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

## Verify locally

```bash
npm install
npm test
npm run lint
```

`npm run lint` is `node --check` — a syntax parser, not a full linter. It
catches parse errors only (no unused vars, no shadowing, no undefined
globals); the pre-push hook's real coverage is `npm test`.

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
