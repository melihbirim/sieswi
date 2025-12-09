#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "========================================="
echo "  GROUP BY Performance Benchmark"
echo "  sieswi vs DuckDB"
echo "========================================="
echo

# Build sieswi
echo "Building sieswi..."
go build -o sieswi ./cmd/sieswi
echo "✓ Built"
echo

# Check DuckDB
if ! command -v duckdb >/dev/null 2>&1; then
  echo "ERROR: duckdb not found. Install with: brew install duckdb"
  exit 1
fi
echo "✓ DuckDB found"
echo

# Test datasets
DATASET_1M="fixtures/ecommerce_1m.csv"
DATASET_10M="fixtures/ecommerce_10m.csv"

if [ ! -f "${DATASET_1M}" ]; then
  echo "ERROR: ${DATASET_1M} not found"
  exit 1
fi

echo "========================================="
echo "Test 1: Simple GROUP BY with COUNT"
echo "Query: SELECT country, COUNT(*) FROM data.csv GROUP BY country"
echo "========================================="
echo

echo "[1M Dataset - sieswi]"
/usr/bin/time -l ./sieswi "SELECT country, COUNT(*) FROM '${DATASET_1M}' GROUP BY country" > /dev/null 2>&1

echo
echo "[1M Dataset - DuckDB]"
/usr/bin/time -l duckdb :memory: -csv "SELECT country, COUNT(*) FROM '${DATASET_1M}' GROUP BY country" > /dev/null 2>&1

if [ -f "${DATASET_10M}" ]; then
  echo
  echo "[10M Dataset - sieswi]"
  /usr/bin/time -l ./sieswi "SELECT country, COUNT(*) FROM '${DATASET_10M}' GROUP BY country" > /dev/null 2>&1
  
  echo
  echo "[10M Dataset - DuckDB]"
  /usr/bin/time -l duckdb :memory: -csv "SELECT country, COUNT(*) FROM '${DATASET_10M}' GROUP BY country" > /dev/null 2>&1
fi

echo
echo "========================================="
echo "Test 2: GROUP BY with SUM"
echo "Query: SELECT country, SUM(total_minor) FROM data.csv GROUP BY country"
echo "========================================="
echo

echo "[1M Dataset - sieswi]"
/usr/bin/time -l ./sieswi "SELECT country, SUM(total_minor) FROM '${DATASET_1M}' GROUP BY country" > /dev/null 2>&1

echo
echo "[1M Dataset - DuckDB]"
/usr/bin/time -l duckdb :memory: -csv "SELECT country, SUM(total_minor) FROM '${DATASET_1M}' GROUP BY country" > /dev/null 2>&1

if [ -f "${DATASET_10M}" ]; then
  echo
  echo "[10M Dataset - sieswi]"
  /usr/bin/time -l ./sieswi "SELECT country, SUM(total_minor) FROM '${DATASET_10M}' GROUP BY country" > /dev/null 2>&1
  
  echo
  echo "[10M Dataset - DuckDB]"
  /usr/bin/time -l duckdb :memory: -csv "SELECT country, SUM(total_minor) FROM '${DATASET_10M}' GROUP BY country" > /dev/null 2>&1
fi

echo
echo "========================================="
echo "Test 3: Multi-column GROUP BY with WHERE"
echo "Query: SELECT country, status, COUNT(*) FROM data.csv WHERE total_minor > 10000 GROUP BY country, status"
echo "========================================="
echo

echo "[1M Dataset - sieswi]"
/usr/bin/time -l ./sieswi "SELECT country, status, COUNT(*) FROM '${DATASET_1M}' WHERE total_minor > 10000 GROUP BY country, status" > /dev/null 2>&1

echo
echo "[1M Dataset - DuckDB]"
/usr/bin/time -l duckdb :memory: -csv "SELECT country, status, COUNT(*) FROM '${DATASET_1M}' WHERE total_minor > 10000 GROUP BY country, status" > /dev/null 2>&1

if [ -f "${DATASET_10M}" ]; then
  echo
  echo "[10M Dataset - sieswi]"
  /usr/bin/time -l ./sieswi "SELECT country, status, COUNT(*) FROM '${DATASET_10M}' WHERE total_minor > 10000 GROUP BY country, status" > /dev/null 2>&1
  
  echo
  echo "[10M Dataset - DuckDB]"
  /usr/bin/time -l duckdb :memory: -csv "SELECT country, status, COUNT(*) FROM '${DATASET_10M}' WHERE total_minor > 10000 GROUP BY country, status" > /dev/null 2>&1
fi

echo
echo "========================================="
echo "Test 4: Multiple aggregates"
echo "Query: SELECT country, COUNT(*), SUM(total_minor), AVG(total_minor), MIN(total_minor), MAX(total_minor) FROM data.csv GROUP BY country"
echo "========================================="
echo

echo "[1M Dataset - sieswi]"
/usr/bin/time -l ./sieswi "SELECT country, COUNT(*), SUM(total_minor), AVG(total_minor), MIN(total_minor), MAX(total_minor) FROM '${DATASET_1M}' GROUP BY country" > /dev/null 2>&1

echo
echo "[1M Dataset - DuckDB]"
/usr/bin/time -l duckdb :memory: -csv "SELECT country, COUNT(*), SUM(total_minor), AVG(total_minor), MIN(total_minor), MAX(total_minor) FROM '${DATASET_1M}' GROUP BY country" > /dev/null 2>&1

if [ -f "${DATASET_10M}" ]; then
  echo
  echo "[10M Dataset - sieswi]"
  /usr/bin/time -l ./sieswi "SELECT country, COUNT(*), SUM(total_minor), AVG(total_minor), MIN(total_minor), MAX(total_minor) FROM '${DATASET_10M}' GROUP BY country" > /dev/null 2>&1
  
  echo
  echo "[10M Dataset - DuckDB]"
  /usr/bin/time -l duckdb :memory: -csv "SELECT country, COUNT(*), SUM(total_minor), AVG(total_minor), MIN(total_minor), MAX(total_minor) FROM '${DATASET_10M}' GROUP BY country" > /dev/null 2>&1
fi

echo
echo "========================================="
echo "              SUMMARY"
echo "========================================="
echo
echo "GROUP BY Performance Highlights:"
echo "  • Simple aggregations: Single-pass through data"
echo "  • Memory-efficient: Only stores unique groups + aggregates"
echo "  • Works with WHERE filtering"
echo "  • Maintains insertion order"
echo
echo "Note: DuckDB uses in-memory database with hash aggregation"
echo "      sieswi uses streaming with hash map accumulation"
echo "========================================="
