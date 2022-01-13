#!/usr/bin/env bash
set -euo pipefail

progs=(
    sqlite3 # [optional] sqlite processing
    vim     # [optional] for vimdiff
    fdupes  # [optional] duplicate detection tool, for tests
)

apt-get update && apt-get --yes install ${progs[@]}
