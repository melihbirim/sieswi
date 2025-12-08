# `.sidx` Sidecar Index Architecture

The `.sidx` file is an optional sidecar that lets the engine skip large chunks of a CSV when evaluating selective predicates. It is designed to stay ≤ 1 % of the CSV size while allowing byte-level seeking into the source file.

---

## Goals & Constraints

- **Zero-copy CSV**: never rewrite the CSV; index must point into the original file.
- **Streaming-friendly**: block-level metadata so we can start outputting rows quickly.
- **Type-aware pruning**: numeric vs string comparisons behave correctly.
- **Cheap invalidation**: detect when the CSV changes (size/mtime) and ignore stale indexes.
- **No dependencies**: pure Go, stdlib only; binary format is simple and append-only.

---

## Binary Layout

```
Header:
  Magic      [4]byte  // "SIDX"
  Version    uint32   // format version (currently 3)
  BlockSize  uint32   // rows per block (default 65 536)
  NumBlocks  uint32
  FileSize   int64    // CSV size in bytes
  FileMtime  int64    // CSV mtime in Unix nanos
  ColumnsLen uint32
  Columns[]:
    NameLen  uint32
    Name     []byte
    Type     uint8    // 0=string, 1=numeric

Blocks[NumBlocks]:
  StartRow    uint64
  EndRow      uint64  // exclusive
  StartOffset uint64  // byte offset into CSV
  EndOffset   uint64
  ColumnStats[ColumnsLen]:
    MinLen uint32
    Min    []byte
    MaxLen uint32
    Max    []byte
    EmptyCount uint32  // v3+: number of empty values in this block

Footer (future): checksum or padding (not yet used)
```

**Why this layout?**

- Column names/types are stored once (dictionary) so each block only keeps min/max pairs, keeping the file small even for wide schemas.
- Offsets are absolute so the engine can `Seek` directly to the first row in a block without re-scanning the entire file.
- No compression/varints yet; everything is little-endian fixed-width for simplicity.

---

## Index Builder (`internal/sidx/builder.go`)

1. **Streaming parse**:
   - Uses a `bufio.Reader` to read the CSV once, line by line.
   - Runs each line through a tiny helper (`parseCSVLine`) that reuses Go’s `encoding/csv` so quoting rules remain correct.
2. **Byte-offset tracking**:
   - Tracks the absolute file position before and after each row.
   - `StartOffset` is captured at the first row in a block; `EndOffset` is captured after the last row.
3. **Per-column stats**:
   - Min/max values are updated as rows stream in (empty strings ignored).
   - **EmptyCount** (v3+): tracks the number of empty values per block for sparse column optimization.
   - **Limitation**: When a column is all-empty, Min/Max remain empty strings and `CanPruneBlock` conservatively returns false (can't prune safely). Future: consider adding an `AllEmpty` flag or sentinel bounds to enable pruning all-empty blocks.
   - Collects up to 256 samples per column to infer whether comparisons should be numeric or lexicographic.
   - **Limitation**: For large files (>256 rows) this sampling window is statistically thin and may misclassify column types. Consider increasing to first full block or N non-empty values for better accuracy.
4. **Block flushing**:
   - When `blockSize` rows accumulate (default 65 536) the builder writes a `BlockMeta` with row range, byte offsets, and column stats.
   - Final partial block flushes at EOF.
5. **Metadata**:
   - Header stores CSV `FileSize` and `FileMtime` for validation.
   - Column dictionary (`[]ColumnInfo`) holds the canonical name + type for each column.

### Build Performance

**Measured on 10GB CSV (130M rows, 10 columns):**

- **Build time:** ~200 seconds (~3.3 minutes)
- **Throughput:** ~50 MB/s
- **Peak memory:** ~118 MB
- **Index size:** 568 KB (0.006% of CSV size)

**Why it takes time:**

- Must read entire CSV file to track accurate byte offsets for seeking
- CSV parsing overhead (quotes, escaping, field splitting)
- Type inference sampling (256 rows per column)

**Trade-off:** One-time cost (~3 minutes for 10GB) enables 10-100x query speedup. Index build is typically done once and reused for many queries.

### Build Optimization Strategies

**Target:** Reduce 10GB build from 200s to 60-90s (3x speedup)

**High-Impact Optimizations:**

1. **Faster Stream Parsing** (~40% speedup)

   - Increase bufio.Reader from 512KB to 1-4MB
   - Use `ReadSlice('\n')` + manual stripping vs `ReadBytes('\n')` to eliminate allocations
   - Reuse single `[]byte` buffer across rows

2. **Parallel Column Stats** (~50% speedup)

   - Split each block into per-column workers (10 goroutines for 10 columns)
   - Or coarse-grained: one goroutine per CPU core processing chunks
   - Synchronize only at block boundaries

3. **Smarter Type Inference** (~15% speedup)

   - Single pass over first 65K-row block instead of 256-row sampling
   - Compute stats + type flags together (one loop instead of two)
   - Eliminates redundant parsing

4. **Optional Features** (~20% speedup for known schemas)

   - `--skip-type-inference` flag: assume all columns are strings
   - Eliminates repeated `ParseFloat` calls during type detection
   - User can specify column types via `--column-types=price:numeric,timestamp:numeric`

5. **Batch I/O** (~10% speedup)

   - Memory-map file or use `ReadAt` with large chunks (4-8MB)
   - Enables kernel prefetching and reduces syscalls

6. **Incremental Indexing** (10x speedup for appends)
   - Detect append-only scenarios (file size grew, mtime changed)
   - Only index new blocks starting from last indexed offset
   - Store last indexed position in index footer

**Combined Impact:** 200s → 60-90s for 10GB (~100 MB/s throughput)

**Profile-Driven Approach:**

```bash
# Profile index build
CPUPROFILE=/tmp/build.prof ./sieswi index huge.csv

# Identify hotspots
go tool pprof -top /tmp/build.prof
```

Target hottest functions first (likely: CSV parsing, string comparisons, type inference).

### Invalidation

`ValidateIndex` re-stat's the CSV and compares size + mtime. If either differs, the engine ignores the `.sidx` file and falls back to a full scan.

**Limitation**: Mtime is compared at nanosecond precision (`FileMtime int64` stores Unix nanos). On filesystems with coarse timestamp resolution (e.g., FAT32 with 2-second granularity), this can spuriously invalidate indexes even when the file hasn't changed. Future: consider rounding to second precision or using content hashes (e.g., first/last N bytes) for more robust validation.

---

## Pruning Logic (`sidx.CanPruneBlock`)

- Finds the target column in the dictionary (case-insensitive) to pull its `ColumnType`.
- Compares predicate values against min/max using numeric parsing when possible, falling back to lexicographic ordering.
- **Caution**: When numeric parsing fails, lexicographic comparison can misorder values (e.g., "9" > "10" lexicographically). This degrades pruning accuracy. Enable `SIDX_DEBUG=1` to log parse failures and identify columns that should be marked as string type.
- Operators handled: `=`, `!=`, `>`, `>=`, `<`, `<=`.
- Conservative rules: a block is pruned only when the predicate is _guaranteed_ to fail for the entire block. Empty stats, unknown columns, or parse failures all default to "keep".

---

## Engine Integration (`internal/engine/engine.go`)

1. Attempts to open `<csv>.sidx`; if found, `ReadIndex` loads the header, dictionary, and block metadata.
2. `ValidateIndex` ensures the sidecar matches the CSV on disk.
3. During query execution:
   - For single-column predicates, iterate blocks and mark any that can be skipped (`CanPruneBlock`).
   - **v3+**: Block-aware scanning seeks past multiple pruned regions. The engine tracks the current block index as it streams and performs a seek whenever it enters a pruned block, jumping directly to the next unpruned block's `StartOffset`.
   - **Note**: Earlier versions only seeked to the first non-pruned block at query start, but still streamed through subsequent pruned blocks. V3 fixes this with multiple seeks during execution.
   - As rows stream, normal predicate evaluation still runs to handle partial matches and LIMIT enforcement.
4. Debug mode (`SIDX_DEBUG=1`) logs how many blocks were pruned and which offsets were jumped to—useful while tuning block sizes or dataset distributions.

---

## Testing & Validation

- **Unit tests**: `internal/sidx/builder_comprehensive_test.go` covers numeric vs string comparisons, `!=` edge cases, index validation, and type inference.
- **Offset tracking**: `TestRealOffsetTracking` builds an index over a synthetic CSV and asserts that each block’s `StartOffset` matches the actual file position by seeking into the file.
- **Integration**: `internal/engine/engine_test.go` runs real queries to ensure the executor still streams the correct rows when filters and limits are applied.

---

## Future Enhancements

- **All-empty block pruning**: Add `AllEmpty` flag or sentinel bounds to `ColumnStats` so blocks with only empty values can be safely pruned.
- **Improved type inference**: Increase sampling window to first full block or until N non-empty values for more accurate numeric vs string detection.
- **Robust invalidation**: Round mtime to second precision or use content hashes to avoid spurious invalidation on coarse filesystems.
- **Parse failure logging**: Add debug logs when numeric parsing fails during pruning to help identify type inference issues.
- **Faster index building**: Current implementation reads entire file sequentially (~50 MB/s, 200s for 10GB). Target: 60-90s for 10GB via:
  - **Stream parse faster**: Tune bufio.Reader to 1-4 MB, use ReadSlice with manual newline stripping to avoid per-line allocations
  - **Parallelize column stats**: Split block scans into per-column workers or SIMD loops; coarse-grained parallelism (one goroutine per CPU chunk) could halve build time
  - **Smarter type inference**: Single pass over first block (65K rows) computing stats/type flags together instead of 256-row serial sampling
  - **Optional type detection**: Flag to skip numeric detection (eliminates repeated ParseFloat) for datasets with known types
  - **Batch I/O**: Memory-mapping or ReadAt with larger chunks for kernel prefetching
  - **Incremental indexing**: Append-only logs only need new blocks indexed, not full rebuild
  - **Profile-driven**: Target hottest functions (CSV parsing, stats aggregation, type inference) with optimized code
- Add checksum to detect silent corruption.
- Support multiple predicates / AND/OR pushdown (requires expression analysis).
- Persist small distinct sets for low-cardinality columns to accelerate `IN` queries.
- Compress min/max strings (e.g., delta + varint) to shrink indexes below 0.1 % for very wide tables.
- Build-time parallelization once we support multi-threaded scanning.
