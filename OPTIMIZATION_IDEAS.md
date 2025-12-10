# ORDER BY Optimization Ideas

## Quick Wins (Easy Implementation)

### 1. Pre-lowercase Sort Keys (3-5x faster for strings)
**Problem**: `strings.ToLower()` called millions of times during sort
**Solution**: Normalize strings once during load, not during comparison
```go
type sortKey struct {
    strValue      string
    strValueLower string  // Pre-lowercased
    numValue      float64
    isNumeric     bool
}
```
**Expected**: 30-40% faster multi-column sorts

### 2. Type Detection from Header Sample (Skip ParseFloat)
**Problem**: ParseFloat called for every cell (1M+ times)
**Solution**: Sample first 100 rows to detect column types
```go
// Detect numeric columns once
numericCols := detectNumericColumns(reader, orderByIndices)
// Skip ParseFloat for string columns
```
**Expected**: 15-20% faster load time

### 3. Early LIMIT Optimization (Heap-based Top-K)
**Problem**: Sort 1M rows to get top 10 (wasteful)
**Solution**: Use min/max heap when `LIMIT < rows/1000`
```go
if query.Limit > 0 && query.Limit < 1000 {
    return executeOrderByTopK(query, reader, header, out)
}
```
**Expected**: 10-50x faster for small LIMIT values

### 4. Parallel Sort for Large Datasets
**Problem**: Single-threaded sort (not using all cores)
**Solution**: Use sort.SliceStable with goroutines for chunks
```go
// For > 100K rows, use parallel merge sort
if len(rows) > 100000 {
    parallelMergeSort(rows, query.OrderBy)
}
```
**Expected**: 2-3x faster on multi-core (M2 has 12 cores)

## Medium Complexity

### 5. External Sort for Very Large Files
**Problem**: 10GB file won't fit in memory (653MB for 1M rows)
**Solution**: Chunk-based external merge sort
- Sort 100MB chunks
- Merge sorted chunks from disk
**Expected**: Handle datasets 10x larger than RAM

### 6. Column-Oriented Sort Keys
**Problem**: Row-oriented storage inefficient for cache
**Solution**: Store sort columns separately
```go
type columnData struct {
    values    []string  // Or []float64 for numeric
    rowIndex  []int
}
```
**Expected**: 20-30% faster due to better cache locality

### 7. Indexed Scans (if file has index)
**Problem**: Full table scan even with WHERE
**Solution**: If index exists, read only matching rows
**Expected**: 100x faster for selective WHERE + ORDER BY

## Benchmark Targets

| Optimization | 1M Single Col | 1M Multi-Col | vs DuckDB |
|--------------|---------------|--------------|-----------|
| **Current** | 0.81s | 3.23s | 3.7-14.7x slower |
| + Pre-lowercase | 0.75s | 2.10s | 3.4-9.5x slower |
| + Type sampling | 0.65s | 1.90s | 3.0-8.6x slower |
| + Top-K heap (LIMIT 100) | 0.15s | 0.40s | 0.7-1.8x slower |
| + Parallel sort | 0.30s | 0.80s | 1.4-3.6x slower |

## Implementation Priority

**Phase 1** (v1.2.0 - This PR):
- [ ] Pre-lowercase sort keys
- [ ] Type detection sampling
- [ ] Document current limitations

**Phase 2** (v1.3.0):
- [ ] Top-K heap optimization
- [ ] Parallel sort for large datasets

**Phase 3** (v1.4.0):
- [ ] External sort for huge files
- [ ] Column-oriented storage

## Notes

- DuckDB uses vectorized execution + SIMD instructions (hard to match)
- Our goal: Within 3-5x of DuckDB for common use cases
- Focus on correctness first, optimize hot paths
