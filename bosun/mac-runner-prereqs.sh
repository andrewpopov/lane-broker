#!/bin/bash
# Compatibility name for installed bosun configs (BRAIN-1145); the logic lives in runner-prereqs.sh.
exec "$(dirname "$0")/runner-prereqs.sh" "$@"
