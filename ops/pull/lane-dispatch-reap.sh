#!/bin/sh
# pitelite: the queue reaper the runbook requires ("Timers on the database host"). Two transactions; the guard runs even when the reap fails.
url="${LANE_QUEUE_URL:?LANE_QUEUE_URL is required}"
psql "$url" -X -q -At -v ON_ERROR_STOP=1 -o /dev/null -c "BEGIN; SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '10s'; SELECT lane_dispatch.ld_reap(); COMMIT;"
reap=$?
psql "$url" -X -q -At -v ON_ERROR_STOP=1 -o /dev/null -c "SELECT lane_dispatch.ld_guard('30 seconds');"
guard=$?
[ "$reap" -eq 0 ] && [ "$guard" -eq 0 ]
