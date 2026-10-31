#!/usr/bin/env sh
# The fast tier of the pre-push gate: only the tests related to the change
# (scripts/affected-tests.js). The full suite is `npm run verify`.
set -e

LABEL="${LABEL:-${1:-verify-fast}}"

selection="$(node scripts/affected-tests.js)"
case "$selection" in
  affected:*)
    echo "$selection"
    exit 0
    ;;
esac

# shellcheck disable=SC2086 -- test paths contain no whitespace
set -- $selection
if [ -z "$LANE_BROKER_LEASE" ] && command -v lane >/dev/null 2>&1; then
  lane run --repo lane-broker --lane prepush -- node --test --test-concurrency=1 "$@"
else
  echo "$LABEL: running tests directly (no lane broker on PATH, or already inside a lane)"
  node --test --test-concurrency=1 "$@"
fi
