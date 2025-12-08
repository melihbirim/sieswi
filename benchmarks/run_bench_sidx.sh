#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${REPO_ROOT}/bin"
DATA_PATH="${REPO_ROOT}/fixtures/ecommerce_1m.csv"

echo "=== .sidx Index Benchmark ==="
echo

# Build sieswi
echo "Building sieswi..."
cd "${REPO_ROOT}"
go build -o sieswi ./cmd/sieswi

# Build index
echo "Building index..."
if [[ -f "${DATA_PATH}.sidx" ]]; then
  rm "${DATA_PATH}.sidx"
fi
./sieswi index "${DATA_PATH}"
echo

# Test 1: Selective query (low cardinality column)
echo "Test 1: Selective query WHERE country = 'AU'"
echo "----------------------------------------"

echo "WITHOUT index:"
rm -f "${DATA_PATH}.sidx"
/usr/bin/time -l ./sieswi "SELECT order_id, country FROM '${DATA_PATH}' WHERE country = 'AU' LIMIT 1000" > /tmp/bench_noindex.csv 2>&1 | grep -E "real|maximum"

echo
echo "WITH index:"
./sieswi index "${DATA_PATH}" 2>&1 | head -1
/usr/bin/time -l ./sieswi "SELECT order_id, country FROM '${DATA_PATH}' WHERE country = 'AU' LIMIT 1000" > /tmp/bench_index.csv 2>&1 | grep -E "real|maximum"

echo
diff /tmp/bench_noindex.csv /tmp/bench_index.csv && echo "✓ Outputs identical"
echo

# Test 2: High selectivity (specific ID)
echo "Test 2: High selectivity WHERE order_id = 'ORD000500000'"
echo "----------------------------------------"

echo "WITHOUT index:"
rm -f "${DATA_PATH}.sidx"
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATA_PATH}' WHERE order_id = 'ORD000500000'" > /tmp/bench_noindex2.csv 2>&1 | grep -E "real|maximum"

echo
echo "WITH index:"
./sieswi index "${DATA_PATH}" 2>&1 | head -1
/usr/bin/time -l ./sieswi "SELECT * FROM '${DATA_PATH}' WHERE order_id = 'ORD000500000'" > /tmp/bench_index2.csv 2>&1 | grep -E "real|maximum"

echo
diff /tmp/bench_noindex2.csv /tmp/bench_index2.csv && echo "✓ Outputs identical"
echo

# Test 3: Numeric range query
echo "Test 3: Numeric range WHERE price_minor > 9000"
echo "----------------------------------------"

echo "WITHOUT index:"
rm -f "${DATA_PATH}.sidx"
/usr/bin/time -l ./sieswi "SELECT order_id, price_minor FROM '${DATA_PATH}' WHERE price_minor > 9000 LIMIT 100" > /tmp/bench_noindex3.csv 2>&1 | grep -E "real|maximum"

echo
echo "WITH index:"
./sieswi index "${DATA_PATH}" 2>&1 | head -1
/usr/bin/time -l ./sieswi "SELECT order_id, price_minor FROM '${DATA_PATH}' WHERE price_minor > 9000 LIMIT 100" > /tmp/bench_index3.csv 2>&1 | grep -E "real|maximum"

echo
diff /tmp/bench_noindex3.csv /tmp/bench_index3.csv && echo "✓ Outputs identical"
echo

echo "=== Benchmark Complete ==="
