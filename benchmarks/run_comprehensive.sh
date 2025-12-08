#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "========================================"
echo "  sieswi vs DuckDB Comprehensive Benchmark"
echo "========================================"
echo

# Build sieswi
echo "Building sieswi..."
go build -o sieswi ./cmd/sieswi
echo

# Check DuckDB
if ! command -v duckdb >/dev/null 2>&1; then
  echo "ERROR: duckdb not found. Install with: brew install duckdb"
  exit 1
fi

RESULTS_DIR="benchmarks/results"
mkdir -p "${RESULTS_DIR}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="${RESULTS_DIR}/comprehensive_${TIMESTAMP}.txt"

exec > >(tee "${RESULT_FILE}") 2>&1

echo "Results will be saved to: ${RESULT_FILE}"
echo
echo "Test Datasets:"
echo "  1. fixtures/ecommerce_1m.csv (random data, 1M rows)"
echo "  2. fixtures/sorted_1m.csv (sorted order_id, 1M rows)"
echo
echo "========================================" 
echo

# === Test 1: Random Data (ecommerce) ===
echo "TEST 1: Random Data (ecommerce_1m.csv)"
echo "========================================"
echo

DATASET="fixtures/ecommerce_1m.csv"
QUERY1_COL="country"
QUERY1_VAL="UK"

echo "Query: SELECT order_id, ${QUERY1_COL} FROM '${DATASET}' WHERE ${QUERY1_COL} = '${QUERY1_VAL}' LIMIT 1000"
echo

# sieswi without index
echo "[1a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT order_id, ${QUERY1_COL} FROM '${DATASET}' WHERE ${QUERY1_COL} = '${QUERY1_VAL}' LIMIT 1000" > /tmp/bench_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[1b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT order_id, ${QUERY1_COL} FROM '${DATASET}' WHERE ${QUERY1_COL} = '${QUERY1_VAL}' LIMIT 1000" > /tmp/bench_sieswi_index.csv 2>&1
grep -E "real|peak" /tmp/bench_sieswi_index.csv | head -2
ROWS=$(wc -l < /tmp/bench_sieswi_index.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[1c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT order_id, ${QUERY1_COL} FROM '${DATASET}' WHERE ${QUERY1_COL} = '${QUERY1_VAL}' LIMIT 1000) TO '/tmp/bench_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb_out.txt
grep -E "real|peak" /tmp/duckdb_out.txt | head -2
ROWS=$(wc -l < /tmp/bench_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 2: Sorted Data - Early Query ===
echo
echo "TEST 2: Sorted Data - Query EARLY rows"
echo "========================================"
echo

DATASET="fixtures/sorted_1m.csv"
QUERY2_COL="order_id"
QUERY2_VAL="ORD000100000"

echo "Query: SELECT ${QUERY2_COL}, created_at FROM '${DATASET}' WHERE ${QUERY2_COL} < '${QUERY2_VAL}' LIMIT 1000"
echo "Expected: First ~2 blocks, prune last 14 blocks (87.5%)"
echo

# sieswi without index
echo "[2a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT ${QUERY2_COL}, created_at FROM '${DATASET}' WHERE ${QUERY2_COL} < '${QUERY2_VAL}' LIMIT 1000" > /tmp/bench2_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench2_sieswi_noindex.csv | head -2
echo

# sieswi with index
echo "[2b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
SIDX_DEBUG=1 /usr/bin/time -l ./sieswi "SELECT ${QUERY2_COL}, created_at FROM '${DATASET}' WHERE ${QUERY2_COL} < '${QUERY2_VAL}' LIMIT 1000" > /tmp/bench2_sieswi_index.csv 2>&1
grep -E "\[sidx\]|real|peak" /tmp/bench2_sieswi_index.csv | head -3
echo

# DuckDB
echo "[2c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT ${QUERY2_COL}, created_at FROM '${DATASET}' WHERE ${QUERY2_COL} < '${QUERY2_VAL}' LIMIT 1000) TO '/tmp/bench2_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb2_out.txt
grep -E "real|peak" /tmp/duckdb2_out.txt | head -2
echo

# === Test 3: Sorted Data - Late Query ===
echo
echo "TEST 3: Sorted Data - Query LATE rows"
echo "========================================"
echo

QUERY3_VAL="ORD000900000"

echo "Query: SELECT ${QUERY2_COL} FROM '${DATASET}' WHERE ${QUERY2_COL} > '${QUERY3_VAL}' LIMIT 1000"
echo "Expected: Last ~2 blocks, prune first 14 blocks (87.5%)"
echo

# sieswi without index
echo "[3a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT ${QUERY2_COL} FROM '${DATASET}' WHERE ${QUERY2_COL} > '${QUERY3_VAL}' LIMIT 1000" > /tmp/bench3_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench3_sieswi_noindex.csv | head -2
echo

# sieswi with index
echo "[3b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
SIDX_DEBUG=1 /usr/bin/time -l ./sieswi "SELECT ${QUERY2_COL} FROM '${DATASET}' WHERE ${QUERY2_COL} > '${QUERY3_VAL}' LIMIT 1000" > /tmp/bench3_sieswi_index.csv 2>&1
grep -E "\[sidx\]|real|peak" /tmp/bench3_sieswi_index.csv | head -3
echo

# DuckDB
echo "[3c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT ${QUERY2_COL} FROM '${DATASET}' WHERE ${QUERY2_COL} > '${QUERY3_VAL}' LIMIT 1000) TO '/tmp/bench3_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb3_out.txt
grep -E "real|peak" /tmp/duckdb3_out.txt | head -2
echo

echo
echo "========================================"
echo "  Benchmark Complete!"
echo "========================================"
echo
echo "Full results saved to: ${RESULT_FILE}"
