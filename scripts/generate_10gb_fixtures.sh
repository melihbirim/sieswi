#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "================================================"
echo "  Generating 10GB Test Fixtures for Phase 2"
echo "================================================"
echo

# Build generator
echo "Building gencsv..."
go build -o bin/gencsv ./cmd/gencsv
echo "✓ Built"
echo

# Generate random 10GB file (130M rows)
echo "[1/2] Generating ecommerce_10gb.csv (random order, ~10GB)..."
echo "      This will take 5-10 minutes..."
./bin/gencsv -rows 130000000 -out fixtures/ecommerce_10gb.csv
echo "✓ Generated ecommerce_10gb.csv"
echo

# Generate sorted 10GB file (130M rows)
echo "[2/2] Generating sorted_10gb.csv (sorted order_id, ~10GB)..."
echo "      This will take 5-10 minutes..."
./bin/gencsv -rows 130000000 -out fixtures/sorted_10gb.csv -sorted
echo "✓ Generated sorted_10gb.csv"
echo

echo "================================================"
echo "  Summary"
echo "================================================"
echo
ls -lh fixtures/*10gb*.csv
echo
echo "✓ 10GB fixtures ready for Phase 2 validation"
echo
