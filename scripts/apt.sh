#!/usr/bin/env bash
set -euo pipefail

progs=(
    jq      # json processing
    sqlite3 # sqlite processing
    atool   # for reading compressed stuff
    vim     # for vimdiff
    fdupes  # duplicate detection tool, for tests
)

apt-get update && apt-get --yes install ${progs[@]}
