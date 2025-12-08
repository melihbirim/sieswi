#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "================================================"
echo "  Phase 2: 10GB Validation & Performance Profile"
echo "================================================"
echo

# Build sieswi
echo "Building sieswi..."
go build -o sieswi ./cmd/sieswi
echo "✓ Built"
echo

DATASET="fixtures/ecommerce_10gb.csv"

# Check if 10GB file exists
if [ ! -f "${DATASET}" ]; then
    echo "ERROR: ${DATASET} not found"
    echo "Generate it with: ./bin/gencsv -rows 130000000 -out ${DATASET}"
    exit 1
fi

FILE_SIZE=$(du -h "${DATASET}" | cut -f1)
echo "Dataset: ${DATASET} (${FILE_SIZE})"
echo

echo "================================================"
echo "  Test 1: Time-to-First-Row"
echo "================================================"
echo
echo "Query: SELECT * FROM ${DATASET} WHERE country = 'UK' LIMIT 1000"
echo

echo "[1] Without index (measure first-row latency):"
rm -f "${DATASET}.sidx"
time ./sieswi "SELECT order_id, country, total_minor FROM '${DATASET}' WHERE country = 'UK' LIMIT 1000" > /tmp/phase2_1.csv 2>&1
echo "   Rows: $(( $(wc -l < /tmp/phase2_1.csv) - 1 ))"
echo

echo "[2] With index:"
./sieswi index "${DATASET}" 2>&1 | grep "Index"
time ./sieswi "SELECT order_id, country, total_minor FROM '${DATASET}' WHERE country = 'UK' LIMIT 1000" > /tmp/phase2_2.csv 2>&1
echo "   Rows: $(( $(wc -l < /tmp/phase2_2.csv) - 1 ))"
echo

echo "================================================"
echo "  Test 2: Memory Usage"
echo "================================================"
echo
echo "Query: Full scan (no LIMIT)"
echo

echo "[1] Without index (measure peak memory):"
rm -f "${DATASET}.sidx"
/usr/bin/time -l ./sieswi "SELECT order_id FROM '${DATASET}' WHERE country = 'UK'" > /tmp/phase2_3.csv 2>&1
ROWS=$(( $(wc -l < /tmp/phase2_3.csv) - 1 ))
echo "   Rows: ${ROWS}"
echo

echo "[2] With index:"
./sieswi index "${DATASET}" 2>&1 | grep "Index"
/usr/bin/time -l ./sieswi "SELECT order_id FROM '${DATASET}' WHERE country = 'UK'" > /tmp/phase2_4.csv 2>&1
ROWS=$(( $(wc -l < /tmp/phase2_4.csv) - 1 ))
echo "   Rows: ${ROWS}"
echo

echo "================================================"
echo "  Test 3: CPU Profile (30s sample)"
echo "================================================"
echo

# Generate CPU profile
rm -f "${DATASET}.sidx"
go build -o sieswi_profile ./cmd/sieswi

echo "Running CPU profile..."
CPUPROFILE=/tmp/sieswi_cpu.prof ./sieswi_profile "SELECT * FROM '${DATASET}' WHERE country = 'UK' LIMIT 100000" > /tmp/phase2_5.csv 2>&1

echo "Top 10 functions by CPU time:"
go tool pprof -top -cum /tmp/sieswi_cpu.prof | head -20
echo

echo "================================================"
echo "  Phase 2 Success Criteria"
echo "================================================"
echo
echo "Target metrics for 10GB file:"
echo "  ✓ First row in < 150ms"
echo "  ✓ Peak memory < 500 MB"
echo "  ✓ Throughput > 0.7 GB/s"
echo
echo "Check results above to validate."
echo
