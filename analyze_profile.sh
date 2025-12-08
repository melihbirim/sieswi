#!/usr/bin/env bash
set -euo pipefail

PROF="internal/sidx/build_profile.prof"

if [ ! -f "$PROF" ]; then
    echo "Error: Profile not found at $PROF"
    echo "Run: go test -run TestBuildProfileLong ./internal/sidx"
    exit 1
fi

echo "=== CPU Profile Analysis ==="
echo

echo "[1] Top functions by cumulative time:"
go tool pprof -top -cum "$PROF" | head -20
echo

echo "[2] Top functions by flat time (self time):"
go tool pprof -top "$PROF" | head -20
echo

echo "[3] CSV parsing functions:"
go tool pprof -top -cum "$PROF" | grep -E "(csv|CSV|Read|parse)" | head -10
echo

echo "[4] String operations:"
go tool pprof -top -cum "$PROF" | grep -E "(string|String)" | head -10
echo

echo "=== Analysis Summary ==="
echo "If csv.Reader.Read() is >70%: Parsing dominates → Consider pipeline model"
echo "If string comparisons are >20%: Stats dominate → Consider block-level parallelism"
echo "If both are balanced: Hybrid approach may be needed"
echo

echo "To view interactive graph: go tool pprof -http=:8080 $PROF"
