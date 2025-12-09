# Benchmarks

This directory contains the benchmarking script and results for comparing sieswi's performance against DuckDB.

## Quick Start

Run the consolidated benchmark:

```bash
# Test with 10M rows (768MB file)
bash benchmarks/run_benchmark.sh 10m

# Test with 10GB file (130M rows)
bash benchmarks/run_benchmark.sh 10gb
```

## What It Tests

The benchmark script (`run_benchmark.sh`) runs 3 test queries comparing:

1. **sieswi without index** - Full CSV scan
2. **sieswi with SIDX index** - Index-accelerated queries  
3. **DuckDB** - Industry standard comparison

### Test Queries

1. `WHERE country = 'UK'` - Low selectivity string filter
2. `WHERE country = 'US'` - Low selectivity string filter
3. `WHERE total_minor > 15000` - Numeric range filter

## Results

Results are saved to `benchmarks/results/benchmark_{size}_{timestamp}.txt`

### Key Metrics

- **Query Time**: Real wall-clock time for query execution
- **Memory Usage**: Peak resident set size during execution
- **Index Size**: Size of the .sidx index file on disk
- **Index Build Time**: Time to build the parallel SIDX index

### Expected Performance

**10M rows (768MB):**
- Index build: ~1s
- Index size: ~7KB
- Query time (no index): 0.00-0.25s
- Query time (with index): <0.01s (instant)
- DuckDB query time: 0.11-0.20s
- sieswi memory: 4-6 MB
- DuckDB memory: 120+ MB

**10GB (130M rows):**
- Index build: ~9s
- Index size: ~7KB
- Query time (with index): <0.01s (instant)
- DuckDB query time: 0.1-0.2s
- sieswi memory: 5-15 MB
- DuckDB memory: 100+ MB

### Key Takeaways

✅ **Speed**: sieswi with index is instant (<10ms) vs DuckDB (100-200ms)  
✅ **Memory**: sieswi uses 20-30x less memory than DuckDB  
✅ **Index Size**: Tiny indexes (KB) for multi-GB files  
✅ **Scalability**: Handles 10GB+ files with minimal memory

## Prerequisites

- DuckDB CLI available in PATH: `brew install duckdb`
- Test datasets:
  - `fixtures/ecommerce_10m.csv` (10M rows, 768MB)
  - `fixtures/ecommerce_10gb.csv` (130M rows, 9.7GB)

Generate test data if needed:

```bash
go run cmd/gencsv/main.go -rows 10000000 -out fixtures/ecommerce_10m.csv
go run cmd/gencsv/main.go -rows 130000000 -out fixtures/ecommerce_10gb.csv
```

## Optimization History

See `index_build_baseline.md` for detailed optimization journey:
- Tier 1: Block size optimization (32KB blocks)
- Tier 2: Parallel index building (5.7x speedup)
- Worker scaling analysis (1-12 workers)
- 10GB validation tests
