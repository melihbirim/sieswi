# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.1] - 2025-12-09

### Fixed
- **Critical:** Fixed parallel processing data loss bug that was missing ~1,343 rows (0.13%)
  - Replaced byte-offset chunking with row-based batching architecture
  - Eliminated chunk boundary issues that caused row loss
  - Now returns exact same row counts as DuckDB (100% data accuracy)
- **Validation:** All queries now validated against DuckDB as ground truth
  - 1M rows: 1,000,000 (was 998,657) ✓
  - WHERE country='UK': 100,038 (was 99,903) ✓
  - 10M WHERE country='UK': 1,001,352 (matches DuckDB) ✓

### Changed
- **Architecture:** Complete rewrite of parallel processing engine
  - New: Row-based batching with 10,000 rows per batch
  - Old: Byte-offset chunking with 4MB chunks (had bugs)
  - Clean separation: 1 reader goroutine + N worker goroutines
  - Batch struct ensures complete rows, no boundary issues
- **Index:** Temporarily disabled index functionality
  - Index was returning incorrect row counts (25K instead of 100K)
  - Parallel processing is fast enough without index (~0.26s for 1M rows)
  - Index building code preserved for future re-implementation

### Performance
- Parallel processing: ~0.26s for 1M row WHERE queries
- Data correctness: 100% match with DuckDB
- Memory: Efficient batch processing (3-15 MB typical)

## [1.0.0] - 2025-12-08

### Added
- **Parallel chunk processing** for large CSV files (>10MB)
  - Automatic multi-core utilization (uses all CPU cores)
  - 4MB chunk size with intelligent boundary detection
  - 7.3x speedup on full table scans vs sequential
- **Sorted index support** (.sidx files)
  - Create with `sieswi index <file> <column>`
  - 85x faster selective queries (12ms vs 1050ms on 10M rows)
  - Type inference for numeric columns
- **Smart execution strategy**
  - Indexed path for fastest seeks
  - Parallel processing for large files
  - Sequential streaming for small files/LIMIT queries
- **RFC 4180 CSV compliance**
  - Proper quoted field handling
  - Escaped quote support (`""` → `"`)
  - Handles wide rows (up to 4MB per line)
- **LIMIT optimization**
  - Parallel for LIMIT ≥10K rows
  - Sequential + early exit for small limits
- **Debug mode** with `SIDX_DEBUG=1`
  - Shows execution strategy
  - Chunk processing stats
  - Performance diagnostics
- Comprehensive documentation
  - PARALLEL_PROCESSING.md with architecture details
  - INSTALL.md with installation options
  - docs/examples.md with real-world use cases

### Performance
- 10M rows (768MB): 0.77s (27% faster than DuckDB)
- 130M rows (10GB): 8.43s (14% slower than DuckDB)
- With index: 12ms (85x faster)
- Throughput: 0.91-1.15 GB/s on full scans

### Fixed
- Chunk boundary handling at EOF
- CSV parsing with escaped quotes
- Map allocation overhead in WHERE clause evaluation
- Scanner buffer sizing for wide CSV rows

## [0.1.0] - 2025-12-01

### Added
- Initial release
- Basic SQL support: SELECT, WHERE, LIMIT
- Streaming execution engine
- Single-predicate WHERE clauses
- Numeric type coercion
- Case-insensitive column names
- Stdin/stdout piping support
- CSV output format

### Performance
- 2-3x faster than DuckDB on small files (streaming)
- 25x less memory usage
- 50x smaller binary size

[Unreleased]: https://github.com/melihbirim/sieswi/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/melihbirim/sieswi/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/melihbirim/sieswi/releases/tag/v1.0.0
[0.1.0]: https://github.com/melihbirim/sieswi/releases/tag/v0.1.0
