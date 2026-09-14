#!/usr/bin/env sh
# The suite step of `npm run verify`. Split out from package.json's script
# chain because, unlike lint (a single command), running the suite needs
# real shell: try the lane broker first, and only fall back to a direct
# `npm test` when the broker isn't on PATH or we're already inside a lane.
# That conditional can't live in a one-line npm script chain.
set -e

LABEL="${LABEL:-${1:-verify}}"

# Run the suite through the lane broker itself when it's available, so a
# pre-push gate on this repo doesn't contend with other lanes on the box.
# Skip the broker when we're already inside a lane (the inherited lease is
# reused for `prepush` under the same repo per README "Reentrancy", but a
# `lane` binary that isn't actually on PATH there must still fall back
# rather than fail the push outright).
if [ -z "$LANE_BROKER_LEASE" ] && command -v lane >/dev/null 2>&1; then
  # `lane run` swallows the child's stdout/stderr into its log file, so a
  # failure here is otherwise silent. Point it at a scratch log and replay
  # that log on failure only; always clean it up, and never let this
  # replay step change the suite's own exit code.
  log_file="$(mktemp "${TMPDIR:-/tmp}/lane-broker-verify.XXXXXX")"
  set +e
  lane run --repo lane-broker --lane prepush --log "$log_file" -- npm test
  status=$?
  set -e
  if [ "$status" -ne 0 ]; then
    echo "$LABEL: npm test failed under lane run (exit $status); log follows:" >&2
    cat "$log_file" >&2
    rm -f "$log_file"
    exit "$status"
  fi
  rm -f "$log_file"
else
  echo "$LABEL: running npm test directly (no lane broker on PATH, or already inside a lane)"
  npm test
fi
