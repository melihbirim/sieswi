#!/usr/bin/env bash
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

DATASET_SIZE="${1:-10m}"
DATASET="fixtures/ecommerce_${DATASET_SIZE}.csv"

if [ ! -f "${DATASET}" ]; then
  echo "ERROR: ${DATASET} not found"
  exit 1
fi

echo "========================================="
echo "  NO LIMIT Benchmark - ${DATASET_SIZE} rows"
echo "========================================="
echo

# Build sieswi
go build -o sieswi ./cmd/sieswi 2>/dev/null

# Define all queries
declare -a QUERIES=(
  "SELECT * FROM '${DATASET}' WHERE country = 'UK'"
  "SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US'"
  "SELECT * FROM '${DATASET}' WHERE country = 'UK' AND total_minor > 10000"
  "SELECT * FROM '${DATASET}' WHERE NOT country = 'UK'"
  "SELECT * FROM '${DATASET}' WHERE (country = 'UK' OR country = 'US') AND total_minor > 10000"
  "SELECT * FROM '${DATASET}' WHERE total_minor > 5000 AND total_minor < 10000"
)

declare -a LABELS=(
  "country = 'UK'"
  "country = 'UK' OR country = 'US'"
  "country = 'UK' AND total_minor > 10000"
  "NOT country = 'UK'"
  "(country = 'UK' OR country = 'US') AND total_minor > 10000"
  "total_minor > 5000 AND total_minor < 10000"
)

# Phase 1: Run all queries WITHOUT index
echo "========================================="
echo "PHASE 1: WITHOUT INDEX"
echo "========================================="
echo
rm -f "${DATASET}.sidx"

for i in "${!QUERIES[@]}"; do
  echo "TEST $((i+1)): ${LABELS[$i]}"
  TIME_OUTPUT=$( { /usr/bin/time -p ./sieswi "${QUERIES[$i]}" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )
  echo "  ${TIME_OUTPUT}s"
  echo
done

# Phase 2: Build index once, then run all queries WITH index
echo "========================================="
echo "PHASE 2: WITH INDEX"
echo "========================================="
echo
echo "Building index..."
./sieswi index "${DATASET}" 2>/dev/null
echo

for i in "${!QUERIES[@]}"; do
  echo "TEST $((i+1)): ${LABELS[$i]}"
  TIME_OUTPUT=$( { /usr/bin/time -p ./sieswi "${QUERIES[$i]}" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )
  echo "  ${TIME_OUTPUT}s"
  echo
done

# Phase 3: Run all queries with DuckDB
echo "========================================="
echo "PHASE 3: DUCKDB"
echo "========================================="
echo

# Create modified queries for DuckDB that write to CSV to force all rows
declare -a DUCKDB_QUERIES=(
  "COPY (SELECT * FROM '${DATASET}' WHERE country = 'UK') TO '/tmp/bench_duck.csv' (HEADER, DELIMITER ',')"
  "COPY (SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US') TO '/tmp/bench_duck.csv' (HEADER, DELIMITER ',')"
  "COPY (SELECT * FROM '${DATASET}' WHERE country = 'UK' AND total_minor > 10000) TO '/tmp/bench_duck.csv' (HEADER, DELIMITER ',')"
  "COPY (SELECT * FROM '${DATASET}' WHERE NOT country = 'UK') TO '/tmp/bench_duck.csv' (HEADER, DELIMITER ',')"
  "COPY (SELECT * FROM '${DATASET}' WHERE (country = 'UK' OR country = 'US') AND total_minor > 10000) TO '/tmp/bench_duck.csv' (HEADER, DELIMITER ',')"
  "COPY (SELECT * FROM '${DATASET}' WHERE total_minor > 5000 AND total_minor < 10000) TO '/tmp/bench_duck.csv' (HEADER, DELIMITER ',')"
)

for i in "${!DUCKDB_QUERIES[@]}"; do
  echo "TEST $((i+1)): ${LABELS[$i]}"
  TIME_OUTPUT=$( { /usr/bin/time -p duckdb -c "${DUCKDB_QUERIES[$i]}" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )
  echo "  ${TIME_OUTPUT}s"
  echo
done

echo "========================================="
echo "Done!"
