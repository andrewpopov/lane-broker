#!/usr/bin/env sh
# The test files given (default: this shard's share, scripts/test-files.js), several at a time, then the tmp-leak guard
# alone: it checks what every other file left behind, so nothing may still be running beside it (README "Running the tests").
set -e
LAST=tests/zz-tmp-leak-guard.test.js
files=${*:-$(node scripts/test-files.js)}
rest=$(printf '%s\n' $files | grep -vxF "$LAST" || true)
conc=${LANE_BROKER_TEST_CONCURRENCY:-$(node -e 'const c = Number(process.argv[1]) || 2; console.log(Math.max(1, Math.min(4, Math.floor(2 * c))))' "${LANE_BROKER_CPU_CORES:-}")}
# shellcheck disable=SC2086 # one path per word; test file names never contain spaces
[ -z "$rest" ] || node --test --test-concurrency="$conc" --import ./tests/helpers/hermetic-host.js $rest
case " $(echo $files) " in *" $LAST "*) node --test --import ./tests/helpers/hermetic-host.js "$LAST" ;; esac
