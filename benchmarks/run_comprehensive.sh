#!/usr/bin/env bash
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Parse command line arguments
DATASET_SIZE="1m"
if [ $# -gt 0 ]; then
  case "$1" in
    1m|10m)
      DATASET_SIZE="$1"
      ;;
    *)
      echo "Usage: $0 [1m|10m]"
      echo "  1m  - Run benchmarks on 1M row dataset (default)"
      echo "  10m - Run benchmarks on 10M row dataset"
      exit 1
      ;;
  esac
fi

echo "========================================"
echo "  sieswi vs DuckDB Comprehensive Benchmark"
echo "  Dataset: ${DATASET_SIZE} rows"
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

# Check if dataset exists
ECOMMERCE_FILE="fixtures/ecommerce_${DATASET_SIZE}.csv"
SORTED_FILE="fixtures/sorted_${DATASET_SIZE}.csv"

if [ ! -f "${ECOMMERCE_FILE}" ]; then
  echo "ERROR: ${ECOMMERCE_FILE} not found"
  echo "Generate it with: go run cmd/gencsv/main.go -rows ${DATASET_SIZE/m/000000} -out ${ECOMMERCE_FILE}"
  exit 1
fi

if [ ! -f "${SORTED_FILE}" ]; then
  echo "WARNING: ${SORTED_FILE} not found. Tests 2-3 will be skipped."
  echo "Generate it with: go run cmd/gencsv/main.go -rows ${DATASET_SIZE/m/000000} -sorted -out ${SORTED_FILE}"
fi

RESULTS_DIR="benchmarks/results"
mkdir -p "${RESULTS_DIR}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="${RESULTS_DIR}/comprehensive_${DATASET_SIZE}_${TIMESTAMP}.txt"

exec > >(tee "${RESULT_FILE}") 2>&1

echo "Results will be saved to: ${RESULT_FILE}"
echo
echo "Test Datasets:"
echo "  1. ${ECOMMERCE_FILE} (random data, ${DATASET_SIZE} rows)"
if [ -f "${SORTED_FILE}" ]; then
  echo "  2. ${SORTED_FILE} (sorted order_id, ${DATASET_SIZE} rows)"
fi
echo
echo "========================================" 
echo

# === Test 1: Random Data (ecommerce) ===
echo "TEST 1: Random Data (${ECOMMERCE_FILE})"
echo "========================================"
echo

DATASET="${ECOMMERCE_FILE}"
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
if [ -f "${SORTED_FILE}" ]; then
  echo
  echo "TEST 2: Sorted Data - Query EARLY rows"
  echo "========================================"
  echo

  DATASET="${SORTED_FILE}"
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
else
  echo
  echo "TEST 2 & 3: SKIPPED (sorted dataset not found)"
  echo
fi

# === Test 4: Boolean AND Predicate ===
echo
echo "TEST 4: Boolean AND (two columns)"
echo "========================================"
echo

DATASET="${ECOMMERCE_FILE}"

echo "Query: SELECT * FROM '${DATASET}' WHERE country = 'UK' AND total_minor > 10000 LIMIT 1000"
echo

# sieswi without index
echo "[4a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE country = 'UK' AND total_minor > 10000 LIMIT 1000" > /tmp/bench4_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench4_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench4_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[4b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE country = 'UK' AND total_minor > 10000 LIMIT 1000" > /tmp/bench4_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench4_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench4_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[4c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE country = 'UK' AND total_minor > 10000 LIMIT 1000) TO '/tmp/bench4_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb4_out.txt
grep -E "real|peak" /tmp/duckdb4_out.txt | head -2
ROWS=$(wc -l < /tmp/bench4_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 5: Boolean OR Predicate ===
echo
echo "TEST 5: Boolean OR (two values)"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' LIMIT 1000"
echo

# sieswi without index
echo "[5a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' LIMIT 1000" > /tmp/bench5_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench5_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench5_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[5b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' LIMIT 1000" > /tmp/bench5_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench5_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench5_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[5c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' LIMIT 1000) TO '/tmp/bench5_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb5_out.txt
grep -E "real|peak" /tmp/duckdb5_out.txt | head -2
ROWS=$(wc -l < /tmp/bench5_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 6: Boolean NOT Predicate ===
echo
echo "TEST 6: Boolean NOT operator"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE NOT country = 'UK' LIMIT 1000"
echo

# sieswi without index
echo "[6a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE NOT country = 'UK' LIMIT 1000" > /tmp/bench6_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench6_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench6_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[6b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE NOT country = 'UK' LIMIT 1000" > /tmp/bench6_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench6_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench6_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[6c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE NOT country = 'UK' LIMIT 1000) TO '/tmp/bench6_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb6_out.txt
grep -E "real|peak" /tmp/duckdb6_out.txt | head -2
ROWS=$(wc -l < /tmp/bench6_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 7: Complex Nested Expression ===
echo
echo "TEST 7: Complex nested boolean expression"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE (country = 'UK' OR country = 'US') AND total_minor > 10000 LIMIT 1000"
echo

# sieswi without index
echo "[7a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE (country = 'UK' OR country = 'US') AND total_minor > 10000 LIMIT 1000" > /tmp/bench7_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench7_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench7_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[7b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE (country = 'UK' OR country = 'US') AND total_minor > 10000 LIMIT 1000" > /tmp/bench7_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench7_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench7_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[7c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE (country = 'UK' OR country = 'US') AND total_minor > 10000 LIMIT 1000) TO '/tmp/bench7_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb7_out.txt
grep -E "real|peak" /tmp/duckdb7_out.txt | head -2
ROWS=$(wc -l < /tmp/bench7_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 8: Deep Nesting (4 levels) ===
echo
echo "TEST 8: Deep Nesting (4 levels)"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE ((country = 'UK' OR country = 'US') OR country = 'FR') AND status != 'cancelled' LIMIT 1000"
echo

# sieswi without index
echo "[8a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE ((country = 'UK' OR country = 'US') OR country = 'FR') AND status != 'cancelled' LIMIT 1000" > /tmp/bench8_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench8_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench8_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[8b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE ((country = 'UK' OR country = 'US') OR country = 'FR') AND status != 'cancelled' LIMIT 1000" > /tmp/bench8_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench8_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench8_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[8c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE ((country = 'UK' OR country = 'US') OR country = 'FR') AND status != 'cancelled' LIMIT 1000) TO '/tmp/bench8_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb8_out.txt
grep -E "real|peak" /tmp/duckdb8_out.txt | head -2
ROWS=$(wc -l < /tmp/bench8_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 9: Double NOT ===
echo
echo "TEST 9: Double NOT (logical equivalence)"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE NOT NOT country = 'UK' LIMIT 500"
echo "Note: NOT NOT X should be logically equivalent to X"
echo

# sieswi without index
echo "[9a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE NOT NOT country = 'UK' LIMIT 500" > /tmp/bench9_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench9_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench9_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[9b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE NOT NOT country = 'UK' LIMIT 500" > /tmp/bench9_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench9_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench9_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[9c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE NOT NOT country = 'UK' LIMIT 500) TO '/tmp/bench9_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb9_out.txt
grep -E "real|peak" /tmp/duckdb9_out.txt | head -2
ROWS=$(wc -l < /tmp/bench9_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 10: Mixed NOT with AND/OR ===
echo
echo "TEST 10: Mixed NOT with AND/OR"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE NOT status = 'cancelled' AND (country = 'UK' OR country = 'US') LIMIT 1000"
echo

# sieswi without index
echo "[10a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE NOT status = 'cancelled' AND (country = 'UK' OR country = 'US') LIMIT 1000" > /tmp/bench10_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench10_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench10_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[10b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE NOT status = 'cancelled' AND (country = 'UK' OR country = 'US') LIMIT 1000" > /tmp/bench10_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench10_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench10_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[10c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE NOT status = 'cancelled' AND (country = 'UK' OR country = 'US') LIMIT 1000) TO '/tmp/bench10_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb10_out.txt
grep -E "real|peak" /tmp/duckdb10_out.txt | head -2
ROWS=$(wc -l < /tmp/bench10_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 11: Numeric Range (AND) ===
echo
echo "TEST 11: Numeric Range with AND"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE total_minor > 5000 AND total_minor < 10000 LIMIT 1000"
echo

# sieswi without index
echo "[11a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE total_minor > 5000 AND total_minor < 10000 LIMIT 1000" > /tmp/bench11_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench11_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench11_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[11b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE total_minor > 5000 AND total_minor < 10000 LIMIT 1000" > /tmp/bench11_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench11_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench11_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[11c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE total_minor > 5000 AND total_minor < 10000 LIMIT 1000) TO '/tmp/bench11_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb11_out.txt
grep -E "real|peak" /tmp/duckdb11_out.txt | head -2
ROWS=$(wc -l < /tmp/bench11_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# === Test 12: Multiple OR Chain (5 countries) ===
echo
echo "TEST 12: Multiple OR Chain (5 values)"
echo "========================================"
echo

echo "Query: SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' OR country = 'FR' OR country = 'DE' OR country = 'JP' LIMIT 1000"
echo

# sieswi without index
echo "[12a] sieswi (no index):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' OR country = 'FR' OR country = 'DE' OR country = 'JP' LIMIT 1000" > /tmp/bench12_sieswi_noindex.csv 2>&1
grep -E "real|peak" /tmp/bench12_sieswi_noindex.csv | head -2
ROWS=$(wc -l < /tmp/bench12_sieswi_noindex.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# sieswi with index
echo "[12b] sieswi (with .sidx index):"
./sieswi index "${DATASET}" 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' OR country = 'FR' OR country = 'DE' OR country = 'JP' LIMIT 1000" > /tmp/bench12_sieswi.csv 2>&1
grep -E "real|peak" /tmp/bench12_sieswi.csv | head -2
ROWS=$(wc -l < /tmp/bench12_sieswi.csv)
echo "Output: $((ROWS - 1)) rows"
echo

# DuckDB
echo "[12c] DuckDB:"
/usr/bin/time -l duckdb -c "COPY (SELECT * FROM '${DATASET}' WHERE country = 'UK' OR country = 'US' OR country = 'FR' OR country = 'DE' OR country = 'JP' LIMIT 1000) TO '/tmp/bench12_duckdb.csv' (HEADER, DELIMITER ',')" 2>&1 > /tmp/duckdb12_out.txt
grep -E "real|peak" /tmp/duckdb12_out.txt | head -2
ROWS=$(wc -l < /tmp/bench12_duckdb.csv)
echo "Output: $((ROWS - 1)) rows"
echo

echo
echo "========================================"
echo "  Benchmark Complete!"
echo "========================================"
echo
echo "Full results saved to: ${RESULT_FILE}"
echo
echo "Summary:"
echo "  Tests 1-3: Single predicate comparisons (SIDX v3 block pruning)"
echo "  Tests 4-7: Boolean predicates (AND/OR/NOT operations)"
echo "  Tests 8-12: Tricky SQL edge cases"
echo "    - Test 8: Deep nesting (4 levels)"
echo "    - Test 9: Double NOT (logical equivalence)"
echo "    - Test 10: Mixed NOT with AND/OR"
echo "    - Test 11: Numeric range (AND on same column)"
echo "    - Test 12: Multiple OR chain (5 values)"
echo
echo "Expected Results:"
echo "  - Selective queries: sieswi 10-20x faster (SIDX pruning excels)"
echo "  - Memory usage: sieswi 16-40x less than DuckDB"
echo "  - Larger datasets: Sub-linear scaling (performance improves with size)"
