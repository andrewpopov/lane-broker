#!/usr/bin/env sh
# The merge train's command: the full suite on a batch, before it lands. A
# failed install stays infra; only a failed suite prints the train's red line.
set -e
npm ci --prefer-offline --no-audit --no-fund
npm run verify || {
  echo 'gate FAILED: `npm run verify` exited non-zero.'
  exit 1
}
