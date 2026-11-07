#!/usr/bin/env sh
# One merge-train shard: lint, then this shard's files (scripts/test-files.js), serially as `npm test` runs them.
set -e
npm run lint
files=$(node scripts/test-files.js)
# shellcheck disable=SC2086 # one path per word; test file names never contain spaces
node --test --test-concurrency=1 --import ./tests/helpers/hermetic-host.js $files
