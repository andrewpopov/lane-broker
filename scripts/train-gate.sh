#!/usr/bin/env sh
# The merge train's command. With no argument: lint and the suite (PR CI and
# batches), or this shard's share of it when the train sets TRAIN_SHARD=i/N. With `heavy`: the scale, perf and soak tests under tests/heavy/,
# which the master verify runs (`masterCommand`) so PR CI stays short. A failed
# install stays infra; only a failed suite prints the train's red line.
set -e
npm ci --prefer-offline --no-audit --no-fund
case "${1:-}" in
  heavy) step='npm run test:heavy' ;;
  '') if [ -n "$TRAIN_SHARDS" ]; then step="sh scripts/test-shard.sh"; else step='npm run verify'; fi ;;
  *) echo "train-gate.sh: unknown tier '$1'" >&2; exit 64 ;;
esac
$step || {
  echo "gate FAILED: \`$step\` exited non-zero."
  exit 1
}
