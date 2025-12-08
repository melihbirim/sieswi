# Boolean Predicates Benchmark Results

## Test Environment
- **Machine**: Apple M2 Pro
- **sieswi**: Phase 4 implementation
- **Dataset**: ecommerce_1m.csv (1M rows, 77MB)
- **Date**: December 8, 2025

## DuckDB Baseline (Control)

Query performance on 1M row CSV:

| Query Type | DuckDB Time | Description |
|------------|-------------|-------------|
| Single predicate | 180ms | `WHERE country = 'UK' LIMIT 1000` |
| AND (two columns) | 185ms | `WHERE country = 'UK' AND amount > 100 LIMIT 1000` |
| OR (two columns) | 190ms | `WHERE country = 'UK' OR country = 'US' LIMIT 1000` |
| Complex nested | 195ms | `WHERE (country = 'UK' OR country = 'US') AND amount > 100 LIMIT 1000` |
| NOT operator | 188ms | `WHERE NOT country = 'UK' LIMIT 1000` |

## sieswi Results (with .sidx index)

| Query Type | sieswi Time | Speedup vs DuckDB | Notes |
|------------|-------------|-------------------|-------|
| Single predicate | 13.4ms | **13.4x faster** | Baseline comparison |
| AND (two columns) | 724ms | 0.26x (slower) | Non-selective, scans most of file |
| OR (two columns) | 4.7ms | **40.4x faster** | Early termination at LIMIT |
| Complex nested | 886ms | 0.22x (slower) | Non-selective AND requires full scan |
| NOT operator | 2.0ms | **94x faster** | Inverted logic, early termination |

## sieswi Results (no index)

| Query Type | sieswi Time | DuckDB Time | Comparison |
|------------|-------------|-------------|------------|
| Single predicate | 11.7ms | 180ms | **15.4x faster** |
| AND (two columns) | 766ms | 185ms | 0.24x (slower) |

## Key Findings

### When sieswi Dominates (10-100x Faster)

1. **Single predicates with LIMIT**
   - 13-15x faster than DuckDB
   - Index enables immediate block pruning
   - Early termination at LIMIT boundary

2. **OR predicates with selective conditions**
   - 40x faster than DuckDB
   - Short-circuit evaluation benefits
   - LIMIT reached quickly on distributed data

3. **NOT operator with LIMIT**
   - 94x faster than DuckDB
   - Inverse logic finds non-matching rows early
   - Streaming architecture advantage

### When DuckDB Performs Better

1. **AND predicates on non-selective conditions**
   - DuckDB 3-4x faster (185ms vs 724ms)
   - Requires scanning most of file
   - DuckDB's vectorized execution excels

2. **Complex nested AND expressions**
   - DuckDB 4x faster (195ms vs 886ms)
   - Multiple column checks require row-by-row evaluation
   - No pruning benefit from index

## Performance Characteristics

### SIDX Index Impact

- **OR queries**: Minimal pruning (checks all blocks), but early LIMIT termination
- **AND queries**: Conservative pruning (must satisfy both), often requires full scan
- **NOT queries**: No pruning (can't exclude blocks), relies on early LIMIT
- **Single predicates**: Maximum benefit from block statistics

### When to Use sieswi

✅ **Best for:**
- Single column filters with LIMIT
- OR predicates finding distributed matches
- NOT predicates with early results
- Streaming queries with immediate output
- Low memory constraints

❌ **Avoid for:**
- Complex AND predicates without LIMIT
- Non-selective multi-column conditions
- Queries requiring full table scans
- Aggregations (COUNT, SUM, GROUP BY)

### When to Use DuckDB

✅ **Best for:**
- Complex AND predicates
- Multi-column filters requiring full scans
- Aggregations and analytics
- JOINs and complex SQL
- Vectorized bulk operations

## Correctness Validation

All boolean expression tests pass:

- ✅ AND with two columns
- ✅ OR with two columns  
- ✅ NOT operator
- ✅ Complex nested expressions: `(col1 = 'A' OR col1 = 'B') AND col2 = 'C'`
- ✅ Operator precedence (NOT > AND > OR)

## Recommendations

1. **Use sieswi for interactive queries** with LIMIT on selective single predicates
2. **Use DuckDB for analytical queries** with complex AND conditions or aggregations
3. **Consider data distribution**: OR predicates benefit from distributed matches
4. **Leverage LIMIT**: sieswi's early termination is most effective with LIMIT
5. **Profile your workload**: AND vs OR predicates have dramatically different performance

## Next Steps

- [ ] Test boolean predicates on 10GB dataset (set TEST_10GB=1)
- [ ] Optimize AND predicate evaluation with better block pruning heuristics
- [ ] Consider query planner for reordering AND/OR operands
- [ ] Add query hints for forcing index usage or full scan
