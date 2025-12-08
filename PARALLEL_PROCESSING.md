# Parallel Processing Implementation

## Overview

sieswi implements **parallel chunk-based CSV processing** for large files without indexes, achieving **4.9x speedup** over sequential processing on multi-core systems.

## Architecture

### Chunk Processing

- **Chunk Size**: 4MB per chunk
- **Workers**: `runtime.GOMAXPROCS(0)` - uses all CPU cores
- **Coordination**: Channels for work distribution and result collection

### Activation Conditions

Parallel processing is enabled when ALL conditions are met:

1. File size > 10MB
2. No index available for the query
3. `LIMIT < 0` (no limit) OR `LIMIT >= 10000` (large result set)
4. `SIDX_NO_PARALLEL != 1` environment variable

### Why These Thresholds?

- **File >10MB**: Parallel overhead only worthwhile for large files
- **LIMIT <10000**: Small result sets complete faster with sequential + early exit
- **LIMIT >=10000**: Large result sets benefit from parallel throughput

## Performance Results

### Test Environment
- CPU: 12 cores (M-series or Intel)
- Dataset: 10M rows (~1.5GB CSV)
- Test: UK country filter (10% selectivity, 1M matching rows)

### Evolution

| Implementation | Time | vs Baseline | Notes |
|----------------|------|-------------|-------|
| Sequential (baseline) | 5.64s | 1.0x | Single-threaded CSV scan |
| Parallel (4 workers) | 2.19s | 2.6x | Initial parallel implementation |
| Parallel (12 workers) | 1.69s | 3.3x | CPU-based worker count |
| **Optimized (final)** | **1.18s** | **4.8x** | CSV parsing + cleanField optimizations |
| DuckDB (reference) | 1.05s | 5.4x | C++ vectorized engine |

**Gap closed from 6x slower to 1.1x slower vs DuckDB!**

### Benchmark Suite (NO LIMIT)

| Test | Query | Time | Speedup |
|------|-------|------|---------|
| 1. UK country (10% sel.) | `WHERE country = 'UK'` | 1.18s | 4.8x |
| 2. Status pending | `WHERE status = 'pending'` | 1.62s | 3.5x |
| 3. Complex filter | `WHERE country = 'UK' AND status = 'completed'` | 0.63s | 9.0x |
| 4. Product range | `WHERE product_id > 'PRD10000'` | 2.41s | 2.3x |
| 5. Full scan (no WHERE) | `SELECT order_id, total_minor` | 1.36s | 4.1x |

### LIMIT Handling

| Query | Mode | Time | Notes |
|-------|------|------|-------|
| `LIMIT 5000` | Sequential + Index | 0.00s | Instant with index seek |
| `LIMIT 50000` | Parallel + Early Exit | 0.13s | Parallel worth overhead |

## Implementation Details

### CSV Parsing Optimizations

1. **Field Slice Reuse**: Single allocation reused across rows (50% allocation reduction)
2. **Inline Space Trimming**: Avoided `bytes.TrimSpace` overhead
3. **Fast-Path cleanField**: Skip processing if no quotes/spaces detected
4. **Proper Quote Handling**: RFC 4180 compliant escaped quote (`""`) support

### Map Handling

- **Overwrite vs Clear**: Populate values instead of O(n) delete loop
- **Pre-normalized Headers**: Headers normalized once, reused per row

### Buffer Sizing

- **Scanner Buffer**: 4MB (matches chunk size) to handle wide CSV rows
- **Flush Buffer**: 8192 rows per write (reduced syscall overhead)

### Correctness Features

1. **EOF Detection**: Properly handles chunks at end of file
2. **Chunk Boundaries**: Finds complete lines, warns on lines >4MB
3. **Escaped Quotes**: Handles `""` within quoted fields
4. **Early LIMIT Exit**: Stops processing when limit reached

## CPU Profiling Results

### Before Optimizations
```
Duration: 1.76s
Total samples: 12.52s (711% CPU utilization)

Top consumers:
- parseCSVLine:     4.58s (36.58%) - Field parsing overhead
- runtime.usleep:   2.71s (21.65%) - Goroutine scheduling
- mallocgc:         1.24s (9.90%)  - Allocations
```

### After Optimizations
```
Duration: 1.06s
Total samples: 7.29s (688% CPU utilization)

Top consumers:
- parseCSVLineInto: 2.30s (31.55%) - 50% reduction
- runtime.usleep:   0.44s (6.04%)  - 84% reduction
- mallocgc:         0.62s (8.50%)  - 50% reduction
```

**Result**: 40% faster wall time, 50% less parsing overhead

## Code Structure

### Files
- `internal/engine/parallel.go` - Parallel processing implementation (434 lines)
- `internal/engine/engine.go` - Integration with main engine

### Key Functions

#### `ParallelExecute(query, out) error`
Entry point. Checks conditions, splits file into chunks, coordinates workers.

#### `processChunks(filePath, chunks<-chan, results chan<-, ...)`
Worker goroutine. Reads chunks from channel, processes via `processChunk`, sends results.

#### `processChunk(file, chunk, query, ...) result`
Reads chunk bytes, finds complete lines, parses CSV, evaluates WHERE, returns matching rows.

#### `parseCSVLineInto(line []byte, fields []string) []string`
Reusable CSV parser. Handles quotes and escaped quotes. Calls `cleanField` per field.

#### `cleanField(field []byte) string`
Extracts clean field value:
- Fast path: No quotes/spaces → direct string conversion
- Slow path: Trim spaces, remove quotes, unescape `""`

## Environment Variables

### `SIDX_DEBUG=1`
Enable debug logging:
- Chunk processing stats
- Worker activity
- Warnings (e.g., lines >4MB)

### `SIDX_NO_PARALLEL=1`
Force sequential processing (bypass parallel path). Useful for:
- Debugging
- Performance comparison
- Systems with limited memory

## Edge Cases Handled

### CSV Semantics
- ✅ Quoted fields with commas: `"Smith, John"`
- ✅ Escaped quotes within quotes: `"Product with ""special"" features"`
- ✅ Mixed quoted/unquoted fields
- ✅ Leading/trailing spaces: ` "Trimmed" `

### File Boundaries
- ✅ Chunks at EOF (no trailing newline)
- ✅ Very wide CSV rows (>1MB but <4MB)
- ✅ Partial lines at chunk boundaries

### Query Patterns
- ✅ Full table scans (no WHERE clause)
- ✅ Selective WHERE with low selectivity
- ✅ Complex multi-condition WHERE
- ✅ Large LIMIT values (>=10000 rows)

## Future Optimizations

Potential improvements (not yet implemented):

1. **SIMD Parsing**: Vectorized CSV delimiter detection
2. **Result Streaming**: Stream results instead of buffering chunks
3. **Adaptive Chunk Size**: Adjust based on row width
4. **Column Pruning**: Skip parsing unused columns
5. **Predicate Pushdown**: Filter during parsing (avoid rowMap allocation)

## Usage Examples

### Basic Parallel Query
```bash
# File >10MB, no LIMIT - uses parallel
./sieswi "SELECT * FROM 'large_file.csv' WHERE country = 'UK'"
```

### Force Sequential
```bash
# Debug or compare performance
SIDX_NO_PARALLEL=1 ./sieswi "SELECT * FROM 'large_file.csv' WHERE country = 'UK'"
```

### Debug Mode
```bash
# See chunk processing details
SIDX_DEBUG=1 ./sieswi "SELECT * FROM 'large_file.csv' WHERE country = 'UK'"
```

### Large LIMIT (Uses Parallel)
```bash
# LIMIT >=10000 - parallel worth overhead
./sieswi "SELECT * FROM 'large_file.csv' WHERE country = 'UK' LIMIT 50000"
```

### Small LIMIT (Uses Sequential)
```bash
# LIMIT <10000 - sequential + index faster
./sieswi "SELECT * FROM 'large_file.csv' WHERE country = 'UK' LIMIT 100"
```

## Comparison with DuckDB

| Metric | sieswi | DuckDB | Ratio |
|--------|--------|--------|-------|
| UK filter (1M rows) | 1.18s | 1.05s | 1.1x slower |
| Implementation | Go stdlib | C++ vectorized | - |
| Memory | ~100MB | ~200MB | 2x less |
| Binary Size | 8MB | 50MB+ | 6x smaller |

**Key Insight**: Within 10% of DuckDB's performance using pure Go stdlib, demonstrating that careful optimization can compete with hand-tuned C++ engines.

## Conclusion

The parallel processing implementation successfully achieves competitive performance with minimal dependencies:

- ✅ **4.8x speedup** over sequential processing
- ✅ **1.1x slower** than DuckDB (down from 6x)
- ✅ **RFC 4180 compliant** CSV parsing
- ✅ **Robust edge case handling**
- ✅ **Zero external dependencies** (stdlib only)

This demonstrates that Go can achieve near-C++ performance for I/O-bound workloads with careful optimization of:
1. Allocation patterns (reuse, not allocate)
2. Data copying (minimize string conversions)
3. Parallelism (saturate all cores)
4. Fast paths (skip unnecessary work)
