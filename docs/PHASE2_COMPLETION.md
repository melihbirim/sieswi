# Phase 2: 10GB Validation - Completion Plan

## Overview
Phase 2 validates that sieswi scales to the PLAN.md targets with 10GB files, matching/beating DuckDB on first-row latency, using ≤50% of its memory, and achieving within 2× throughput for full scans.

---

## Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| Time-to-first-row | < 150ms | `time sieswi "... LIMIT 1000"` |
| Peak memory | < 500 MB | `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux) |
| Throughput | > 0.7 GB/s | Full scan time / file size |
| Memory flat vs file size | ✓ | 10GB file should use similar memory as 1GB |

---

## Step 1: Generate 10GB Fixtures ⏳

**Command:**
```bash
./scripts/generate_10gb_fixtures.sh
```

**Output:**
- `fixtures/ecommerce_10gb.csv` (~10GB, 130M rows, random data)
- `fixtures/sorted_10gb.csv` (~10GB, 130M rows, sorted order_id)

**Time:** 10-15 minutes total

---

## Step 2: Run Validation Benchmarks

**Command:**
```bash
./benchmarks/phase2_validation.sh
```

**Tests:**
1. **Time-to-first-row** (with and without index)
2. **Memory usage** (peak RSS for full scan)
3. **CPU profile** (identify hotspots)

**Expected Results:**
- First row: 90-150ms ✓
- Peak memory: 10-50 MB (streaming) ✓
- Throughput: Calculate from full scan timing

---

## Step 3: CPU/Memory Profiling

### CPU Profile
```bash
# Build with profiling
go build -o sieswi_profile ./cmd/sieswi

# Run with CPU profile
CPUPROFILE=/tmp/cpu.prof ./sieswi_profile "SELECT * FROM 'fixtures/ecommerce_10gb.csv' WHERE country = 'UK' LIMIT 100000" > /dev/null

# Analyze
go tool pprof -top /tmp/cpu.prof
go tool pprof -web /tmp/cpu.prof  # Opens in browser
```

### Memory Profile
```bash
# Run with memory profile
go build -o sieswi_profile ./cmd/sieswi
MEMPROFILE=/tmp/mem.prof ./sieswi_profile "SELECT * FROM 'fixtures/ecommerce_10gb.csv' WHERE country = 'UK'" > /dev/null

# Analyze
go tool pprof -top /tmp/mem.prof
go tool pprof -alloc_space -top /tmp/mem.prof  # Total allocations
```

---

## Step 4: Compare with DuckDB

### Install DuckDB (if not present)
```bash
# macOS
brew install duckdb

# Linux
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
chmod +x duckdb
```

### Benchmark Comparison
```bash
# sieswi
time ./sieswi "SELECT order_id, country FROM 'fixtures/ecommerce_10gb.csv' WHERE country = 'UK' LIMIT 1000"

# DuckDB
time duckdb :memory: "SELECT order_id, country FROM 'fixtures/ecommerce_10gb.csv' WHERE country = 'UK' LIMIT 1000"
```

**Measure:**
- Time-to-first-row (real time)
- Peak memory (/usr/bin/time -l)
- Full scan time

---

## Step 5: Document Results

Create `benchmarks/PHASE2_RESULTS.md` with:

```markdown
# Phase 2: 10GB Validation Results

**Date:** [Date]
**Dataset:** ecommerce_10gb.csv (10GB, 130M rows)

## Success Criteria

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Time-to-first-row | < 150ms | XXms | ✅/❌ |
| Peak memory | < 500 MB | XXM B | ✅/❌ |
| Throughput | > 0.7 GB/s | X.XGB/s | ✅/❌ |

## Comparison with DuckDB

### Query: WHERE country = 'UK' LIMIT 1000

| Tool | Time | Memory | Throughput |
|------|------|--------|------------|
| sieswi | XXms | XXM B | - |
| DuckDB | XXms | XXXMB | - |

### CPU Hotspots

Top 5 functions by CPU time:
1. ...
2. ...

### Memory Profile

Peak allocations:
- ...

## Conclusions

- [Summary of results]
- [Any optimizations needed]
- [Phase 2 complete? Yes/No]
```

---

## Step 6: Optimization (If Needed)

If any success criteria are not met, profile and optimize:

### Common Hotspots
1. **CSV parsing** - Consider faster parser or buffering
2. **String comparisons** - Cache column indices
3. **Memory allocations** - Pool buffers, reduce copies
4. **I/O buffering** - Tune buffer sizes

### Optimization Process
```bash
# Identify hotspot
go tool pprof -top /tmp/cpu.prof

# Optimize code
# Re-benchmark
./benchmarks/phase2_validation.sh

# Compare before/after
```

---

## Phase 2 Checklist

- [ ] **Step 1:** Generate 10GB fixtures (ecommerce_10gb.csv, sorted_10gb.csv)
- [ ] **Step 2:** Run validation benchmarks
- [ ] **Step 3:** CPU/memory profiling
- [ ] **Step 4:** Compare with DuckDB
- [ ] **Step 5:** Document results in benchmarks/PHASE2_RESULTS.md
- [ ] **Step 6:** Optimize if needed (optional)
- [ ] **Verify:** All success criteria met ✅
- [ ] **Sign-off:** Phase 2 complete, ready for Phase 4 (Boolean Predicates)

---

## Quick Start

To complete Phase 2 now:

```bash
# 1. Generate fixtures (~15 mins)
./scripts/generate_10gb_fixtures.sh

# 2. Run validation (~5 mins)
./benchmarks/phase2_validation.sh

# 3. Compare with DuckDB
time duckdb :memory: "SELECT * FROM 'fixtures/ecommerce_10gb.csv' WHERE country = 'UK' LIMIT 1000"

# 4. Document results
# Edit benchmarks/PHASE2_RESULTS.md with actual numbers

# 5. Done!
```

---

## Notes

- **Phase 3 (SIDX) is already complete** - We jumped ahead and implemented it
- Phase 2 is primarily about validation and documentation
- 10GB fixtures will be gitignored (too large for repo)
- Document generation commands in README for reproducibility
