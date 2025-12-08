# Benchmarks

Compare `sieswi` against DuckDB on shared CSV datasets. Every time a feature ships, re-run the suite to track time-to-first-row, total runtime, CPU, and memory deltas.

## Prerequisites

- DuckDB CLI available in `PATH` (e.g. `brew install duckdb`).
- Synthetic dataset (`fixtures/ecommerce_1m.csv`). Generate with:

```bash
./scripts/gen_ecommerce_fixture.sh
```

## Running the Benchmarks

```bash
./benchmarks/run_bench.sh [relative/path/to/dataset.csv]
```

The script:

1. Builds `sieswi` into `./bin/sieswi`.
2. Runs a set of representative queries (country equality, numeric range, high-selectivity) with both engines.
3. Uses `/usr/bin/time` to capture wall time, CPU, and peak memory.
4. Writes results to `benchmarks/results/latest.txt`.

Example snippet from the results file:

```
## sieswi - country_eq
        0.07 real         0.00 user         0.00 sys
             4587520  maximum resident set size

## duckdb - country_eq
        0.20 real         0.12 user         0.04 sys
            92127232  maximum resident set size
```

## Latest Results

See `benchmarks/results/summary.md` for a formatted analysis of the most recent run.

**Baseline (Phase 1):** sieswi is 2-3x faster and uses ~95% less memory than DuckDB on simple queries.

## Notes

- Queries use Phase 1 syntax only (single predicates: `=`, `>`, `<`, etc.)
- BETWEEN syntax not supported yet (use `>` or `<` instead)
- Expand the query list as new features (indexes, AND/OR, etc.) are introduced
