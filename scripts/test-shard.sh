#!/usr/bin/env sh
# One merge-train shard: lint, then this shard's files (scripts/test-files.js), run the way `npm test` runs them.
set -e
npm run lint
sh scripts/test-run.sh "$@"
