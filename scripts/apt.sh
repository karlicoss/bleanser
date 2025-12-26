#!/usr/bin/env bash
set -euo pipefail

progs=(
    sqlite3 # [optional] sqlite processing
    vim     # [optional] for vimdiff
)

apt-get update && apt-get --yes install ${progs[@]}
