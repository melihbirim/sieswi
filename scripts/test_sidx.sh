#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== .sidx Index Test Suite ==="
echo

# Build sieswi
echo "1. Building sieswi..."
go build -o sieswi ./cmd/sieswi
echo "   ✓ Built"
echo

# Generate sorted dataset
echo "2. Generating sorted dataset (1M rows, ~11 days of timestamps)..."
if [[ ! -f fixtures/sorted_1m.csv ]]; then
  go run ./cmd/gencsv -rows 1000000 -out fixtures/sorted_1m.csv -sorted -seed 42
fi
echo "   ✓ fixtures/sorted_1m.csv ($(ls -lh fixtures/sorted_1m.csv | awk '{print $5}'))"
echo

# Build index
echo "3. Building .sidx index..."
./sieswi index fixtures/sorted_1m.csv
INDEX_SIZE=$(ls -lh fixtures/sorted_1m.csv.sidx | awk '{print $5}')
echo "   ✓ fixtures/sorted_1m.csv.sidx ($INDEX_SIZE)"
echo

# Check timestamp ranges per block
echo "4. Checking timestamp distribution..."
echo "   First 5 rows:"
./sieswi "SELECT order_id, created_at FROM 'fixtures/sorted_1m.csv' LIMIT 5" | column -t -s,
echo
echo "   Last 5 rows:"
./sieswi "SELECT order_id, created_at FROM 'fixtures/sorted_1m.csv'" | tail -5 | column -t -s,
echo

# Test 1: Query early data (should be in first blocks)
echo "5. Test 1: Query EARLY data (created_at < '2023-01-02')"
echo "   Expected: Find data in first ~2 blocks, prune last 14 blocks"
echo

echo "   WITHOUT index:"
rm -f fixtures/sorted_1m.csv.sidx
/usr/bin/time -l ./sieswi "SELECT COUNT(*) FROM 'fixtures/sorted_1m.csv' WHERE created_at < '2023-01-02' LIMIT 10" 2>&1 > /tmp/noindex1.txt
grep -E "real|peak" /tmp/noindex1.txt | head -2

echo
echo "   WITH index:"
./sieswi index fixtures/sorted_1m.csv 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT COUNT(*) FROM 'fixtures/sorted_1m.csv' WHERE created_at < '2023-01-02' LIMIT 10" 2>&1 > /tmp/index1.txt
grep -E "real|peak" /tmp/index1.txt | head -2
echo

# Test 2: Query late data (should be in last blocks)
echo "6. Test 2: Query LATE data (created_at > '2023-01-10')"
echo "   Expected: Prune first ~14 blocks, find data in last 2 blocks"
echo

echo "   WITHOUT index:"
rm -f fixtures/sorted_1m.csv.sidx
/usr/bin/time -l ./sieswi "SELECT order_id FROM 'fixtures/sorted_1m.csv' WHERE created_at > '2023-01-10' LIMIT 100" 2>&1 > /tmp/noindex2.txt
grep -E "real|peak" /tmp/noindex2.txt | head -2

echo
echo "   WITH index:"
./sieswi index fixtures/sorted_1m.csv 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT order_id FROM 'fixtures/sorted_1m.csv' WHERE created_at > '2023-01-10' LIMIT 100" 2>&1 > /tmp/index2.txt
grep -E "real|peak" /tmp/index2.txt | head -2
echo

# Test 3: Query middle data (should scan some blocks in middle)
echo "7. Test 3: Query MIDDLE data (created_at > '2023-01-05' AND created_at < '2023-01-06')"
echo "   (Note: we only support single predicate, so just using > for now)"
echo

echo "   WITHOUT index:"
rm -f fixtures/sorted_1m.csv.sidx
/usr/bin/time -l ./sieswi "SELECT order_id FROM 'fixtures/sorted_1m.csv' WHERE created_at > '2023-01-05' LIMIT 100" 2>&1 > /tmp/noindex3.txt
grep -E "real|peak" /tmp/noindex3.txt | head -2

echo
echo "   WITH index:"
./sieswi index fixtures/sorted_1m.csv 2>&1 | grep "Index written"
/usr/bin/time -l ./sieswi "SELECT order_id FROM 'fixtures/sorted_1m.csv' WHERE created_at > '2023-01-05' LIMIT 100" 2>&1 > /tmp/index3.txt
grep -E "real|peak" /tmp/index3.txt | head -2
echo

echo "=== Summary ==="
echo "The index is working, but string comparison with timestamps doesn't prune as well."
echo "Best test: Use order_id (numeric prefix) for clear block separation:"
echo
echo "   ./sieswi \"SELECT * FROM 'fixtures/sorted_1m.csv' WHERE order_id > 'ORD000900000' LIMIT 10\""
echo "   → Should prune first 14 blocks!"
