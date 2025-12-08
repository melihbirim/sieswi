# sieswi

**Blazing-fast SQL queries on CSV files** • Parallel processing • Competitive with DuckDB • Pure Go

```bash
sieswi "SELECT price_minor, country FROM 'data.csv' WHERE country = 'US' LIMIT 100"
```

## Why sieswi?

**sieswi** combines the best of both worlds: instant streaming for small queries and parallel chunk processing for large files. Built in pure Go with zero dependencies.

### Performance vs DuckDB

| Dataset              | Query Type          | sieswi | DuckDB | Comparison        |
| -------------------- | ------------------- | ------ | ------ | ----------------- |
| **1M rows (77MB)**   | Selective (indexed) | 12ms   | 1050ms | **85x faster** ⚡ |
| **10M rows (768MB)** | Full scan           | 770ms  | 1050ms | **27% faster** ⚡ |
| **130M rows (10GB)** | Full scan           | 8.43s  | 7.41s  | 14% slower 🎯     |

**Key Features:**

- ⚡ **Parallel processing** - Auto-detects large files, uses all CPU cores
- 🎯 **Smart indexing** - `.sidx` sorted indexes for 85x speedup on selective queries
- 🚀 **Streaming first** - Results appear instantly for small queries
- 📦 **8MB binary** - Pure Go stdlib, no dependencies
- 🔧 **Production-ready** - RFC 4180 CSV compliant, robust edge case handling

## Quick Start

### Install

```bash
go install github.com/melihbirim/sieswi/cmd/sieswi@latest
```

Or build from source:

```bash
git clone https://github.com/melihbirim/sieswi
cd sieswi
go build -o sieswi ./cmd/sieswi
```

### First Query

```bash
# From command line
sieswi "SELECT * FROM 'data.csv' WHERE status = 'ACTIVE' LIMIT 10"

# From stdin (pipes!)
cat data.csv | sieswi "SELECT name, age FROM '-' WHERE age > 25"

# Write to file
sieswi "SELECT * FROM 'orders.csv' WHERE country = 'US'" > us_orders.csv
```

## SQL Support (Phase 1 - Baseline)

sieswi supports the subset of SQL that makes sense for streaming:

✅ **Supported:**

- `SELECT` with column projection (`SELECT name, age FROM ...`) or `SELECT *`
- `WHERE` comparisons: `=`, `!=`, `>`, `>=`, `<`, `<=`
- Boolean expressions: `AND`, `OR`, `NOT`, parentheses for grouping
- `LIMIT` for result capping
- Numeric coercion (`"123"` == `123`) and case-insensitive columns

❌ **Not Supported (by design):**

- `ORDER BY`, `GROUP BY`, `JOIN` – defeats streaming model
- `IN`, `LIKE`, `BETWEEN`, `IS NULL` – planned features

See [SQL_SUPPORT.md](SQL_SUPPORT.md) for full details.

## Use Cases

**Perfect for:**

- 🔍 Ad-hoc CSV exploration (`grep` for structured data)
- 🚰 Unix pipelines for streaming data:

  ```bash
  # Monitor live logs for errors (use '-' for stdin)
  tail -f logs.csv | sieswi "SELECT timestamp, message FROM '-' WHERE level = 'ERROR'"

  # Stream and filter API requests
  tail -f access.csv | sieswi "SELECT ip, endpoint FROM '-' WHERE status >= 400"

  # Pipe data through sieswi
  cat orders.csv | sieswi "SELECT country, total_minor FROM '-' WHERE total_minor > 10000" | wc -l

  # Chain multiple filters
  cat data.csv | sieswi "SELECT * FROM '-' WHERE country = 'US'" | sieswi "SELECT name, age FROM '-' WHERE age > 25"

  # Process multiple files
  for file in logs/*.csv; do
    cat "$file" | sieswi "SELECT * FROM '-' WHERE level = 'ERROR'" >> all_errors.csv
  done
  ```

- 📊 Log analysis without loading into a database
- ⚡ Quick data quality checks
- 🎯 Selective queries with `.sidx` indexes (100x+ speedup)

**Not ideal for:**

- Complex aggregations (use DuckDB/SQLite)
- JOINs across multiple files
- Analytics requiring ORDER BY

## How It Works

**Adaptive Execution Strategy:**

1. **Indexed queries** (fastest): Uses `.sidx` sorted index for instant seeks
2. **Parallel processing**: Large files (>10MB) use multi-core chunk processing
3. **Sequential streaming**: Small files or LIMIT queries stream row-by-row

```
                        ┌─ Has .sidx? ─→ Indexed Seek (12ms, 85x faster)
Input CSV → Parse Header┼─ File >10MB? ─→ Parallel Chunks (0.77s, 12 workers)
                        └─ Otherwise ──→ Sequential Stream (instant results)
```

**Parallel Processing:**

- Splits file into 4MB chunks
- Uses `runtime.GOMAXPROCS(0)` workers (all CPU cores)
- RFC 4180 compliant CSV parsing with escaped quotes
- Smart LIMIT handling (parallel for ≥10K rows, sequential for small limits)

## Roadmap

- **Phase 1 (✅ Done)**: Baseline streaming engine (current)
- **Phase 2**: CSV linter with strict RFC 4180 validation
- **Phase 3**: `.sidx` sorted index for 100x speedup on selective queries
- **Phase 4**: AND predicates, IN clauses, boolean expressions
- **Phase 5**: LIKE, BETWEEN, IS NULL
- **Phase 6**: Natural language to SQL

See [PLAN.md](PLAN.md) for detailed roadmap.

## Benchmarks

Run benchmarks yourself:

```bash
# Option 1: Use the prebuilt gencsv binary
./gencsv -rows 1000000 -out fixtures/ecommerce_1m.csv

# Option 2: Build and use from source
go run ./cmd/gencsv -rows 1000000 -out fixtures/ecommerce_1m.csv

# Generate larger datasets
./gencsv -rows 10000000 -out fixtures/ecommerce_10m.csv   # 10M rows (~768MB)
./gencsv -rows 130000000 -out fixtures/ecommerce_10gb.csv  # 130M rows (~10GB)

# Generate sorted data (for .sidx index testing)
./gencsv -rows 1000000 -sorted -out fixtures/sorted_1m.csv

# Quick scripts for common fixtures
./scripts/gen_ecommerce_fixture.sh      # 1M rows standard test data
./scripts/generate_10gb_fixtures.sh     # 10GB test fixtures (takes ~10 min)

# Run benchmark suite
./benchmarks/run_bench.sh
```

**gencsv options:**

- `-rows N` - Number of rows to generate (default: 1,000,000)
- `-out PATH` - Output file path (default: `fixtures/ecommerce_1m.csv`)
- `-seed N` - Random seed for reproducibility (default: 42)
- `-sorted` - Generate with sorted timestamps (useful for index testing)

Results stored in `benchmarks/results/` with detailed metrics.

## Development

```bash
# Run tests
go test ./...

# Run with coverage
go test -cover ./...

# Build binary
go build -o sieswi ./cmd/sieswi

# Benchmark against DuckDB
./benchmarks/run_bench.sh
```

**Test Coverage:** 85% engine, 71% parser

## Architecture

```bash
cmd/
  sieswi/          # CLI entrypoint
  gencsv/          # Test data generator
internal/
  sqlparser/       # SQL parsing (regex-based)
  engine/          # Streaming execution engine
benchmarks/        # Performance testing vs DuckDB
fixtures/          # Test data
```

## Contributing

sieswi is in active development. We welcome:

- Bug reports and feature requests (open an issue)
- Performance improvements
- SQL operator implementations
- Documentation improvements

## License

MIT

---

**Built with ❤️ in Go** | [Report Bug](https://github.com/melihbirim/sieswi/issues) | [Benchmark Methodology](benchmarks/STRATEGY.md)
