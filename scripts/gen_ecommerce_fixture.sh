#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${GOCACHE:-}" ]]; then
  export GOCACHE="$(pwd)/.gocache"
fi

go run ./cmd/gencsv -rows 1000000 -out fixtures/ecommerce_1m.csv -seed 42
