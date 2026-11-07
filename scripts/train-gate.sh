#!/usr/bin/env sh
# The merge train's command. Usage: train-gate.sh [pr|heavy|all] (default pr).
# PR CI runs the pr tier; the master verify runs `all` (the pr tier plus the
# heavy scale/soak files). Lint, then the tier at the lane's CPU grant
# (scripts/test-tier.js). A failed install stays infra; only a failed lint or
# suite prints the train's red line.
set -e
tier="${1:-pr}"
case "$tier" in
  pr|heavy|all) ;;
  *)
    echo "usage: sh scripts/train-gate.sh [pr|heavy|all]" >&2
    exit 2
    ;;
esac
npm ci --prefer-offline --no-audit --no-fund
{ npm run lint && node scripts/test-tier.js "$tier"; } || {
  echo "gate FAILED: lint or the $tier test tier exited non-zero."
  exit 1
}
