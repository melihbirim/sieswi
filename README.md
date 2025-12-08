# sieswi

**Zero-load streaming SQL queries on CSV files** • 2-3x faster than DuckDB • 25x less memory

```bash
sieswi "SELECT price_minor, country FROM 'data.csv' WHERE country = 'US' LIMIT 100"
```

## Why sieswi?

Traditional tools like DuckDB load entire CSV files into memory before querying. **sieswi streams row-by-row**, delivering results instantly with constant memory usage—perfect for ad-hoc queries, pipelines, and log analysis.

### Performance vs DuckDB (1M rows, 77MB CSV)

| Metric | sieswi | DuckDB | Winner |
|--------|--------|--------|---------|
| **Time-to-First-Row** | 90-110ms | 190-210ms | **2.1x faster** ⚡ |
| **Total Time** | 70-110ms | 190-210ms | **2-3x faster** ⚡ |
| **Memory (RSS)** | 4-9MB | 104-109MB | **25x less** 🚀 |
| **Binary Size** | ~4MB | ~200MB | **50x smaller** 📦 |

*Benchmark: Filtering 1M row e-commerce dataset, both engines writing to CSV files.*

## Quick Start

### Install

```bash
go install github.com/sieswi/sieswi/cmd/sieswi@latest
```

Or build from source:

```bash
git clone https://github.com/sieswi/sieswi
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
- `SELECT` with column projection: `SELECT name, age FROM ...`
- `SELECT *` for all columns
- `WHERE` with single predicates: `=`, `!=`, `>`, `>=`, `<`, `<=`
- `LIMIT` for result capping
- Numeric comparisons with type coercion
- Case-insensitive column names

❌ **Not Supported (by design):**
- `ORDER BY`, `GROUP BY`, `JOIN` - defeats streaming model
- Multiple predicates (coming in Phase 4)
- `IN`, `LIKE`, `BETWEEN` - planned features

See [SQL_SUPPORT.md](SQL_SUPPORT.md) for full details.

## Use Cases

**Perfect for:**
- 🔍 Ad-hoc CSV exploration (`grep` for structured data)
- 🚰 Unix pipelines: `tail -f logs.csv | sieswi "..." | ...`
- 📊 Log analysis without loading into a database
- ⚡ Quick data quality checks
- 🎯 Selective queries with `.sidx` indexes (100x+ speedup)

**Not ideal for:**
- Complex aggregations (use DuckDB/SQLite)
- JOINs across multiple files
- Analytics requiring ORDER BY

## How It Works

1. **Zero-load architecture**: No data loading phase, no indexes (unless you opt-in with `.sidx`)
2. **Streaming execution**: Row-by-row processing with constant memory
3. **Periodic flushing**: Results appear every 128 rows (configurable)
4. **Smart type coercion**: Automatic numeric conversion for comparisons

```
Input CSV → Parse Header → Stream Rows → Filter → Project → Flush → Output CSV
                                          ↓
                                   .sidx (optional)
                                   100x faster seeks
```

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
# Generate test data
go run ./cmd/gencsv > fixtures/ecommerce_1m.csv

# Run benchmark suite
./benchmarks/run_bench.sh
```

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

```
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

**Built with ❤️ in Go** | [Report Bug](https://github.com/sieswi/sieswi/issues) | [Benchmark Methodology](benchmarks/STRATEGY.md)
