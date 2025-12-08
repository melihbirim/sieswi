.PHONY: build test bench clean install release

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

build:
	@echo "Building sieswi $(VERSION)..."
	@go build $(LDFLAGS) -o sieswi ./cmd/sieswi
	@echo "✓ Built sieswi binary"

test:
	@echo "Running tests..."
	@go test -v ./...

test-coverage:
	@echo "Running tests with coverage..."
	@go test -cover ./...
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report: coverage.html"

bench:
	@echo "Running benchmarks..."
	@./benchmarks/run_bench.sh

bench-10gb:
	@echo "Running 10GB benchmark..."
	@rm -f fixtures/ecommerce_10gb.csv.sidx
	@echo "sieswi:"
	@/usr/bin/time -p ./sieswi "SELECT * FROM 'fixtures/ecommerce_10gb.csv' WHERE country = 'UK'" > /tmp/sieswi_10gb.txt
	@wc -l /tmp/sieswi_10gb.txt
	@echo ""
	@echo "DuckDB:"
	@/usr/bin/time -p duckdb -c "COPY (SELECT * FROM read_csv_auto('fixtures/ecommerce_10gb.csv') WHERE country = 'UK') TO '/tmp/duckdb_10gb.csv' (HEADER, DELIMITER ',')"
	@wc -l /tmp/duckdb_10gb.csv

clean:
	@echo "Cleaning..."
	@rm -f sieswi
	@rm -f fixtures/*.sidx
	@rm -f coverage.out coverage.html
	@rm -rf dist/
	@echo "✓ Cleaned"

install: build
	@echo "Installing sieswi to $(GOPATH)/bin..."
	@cp sieswi $(GOPATH)/bin/
	@echo "✓ Installed"

release:
	@echo "Creating release $(VERSION)..."
	@goreleaser release --clean
	@echo "✓ Release complete"

release-snapshot:
	@echo "Creating snapshot release..."
	@goreleaser release --snapshot --clean
	@echo "✓ Snapshot complete"

# Development helpers
gen-test-data:
	@echo "Generating test data..."
	@go run ./cmd/gencsv -rows 1000000 > fixtures/ecommerce_1m.csv
	@echo "✓ Generated fixtures/ecommerce_1m.csv"

profile:
	@echo "Running profiler..."
	@go run cmd/profile_query/main.go > /dev/null
	@go tool pprof -top /tmp/sieswi_cpu.prof

help:
	@echo "sieswi Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  build              - Build sieswi binary"
	@echo "  test               - Run tests"
	@echo "  test-coverage      - Run tests with coverage report"
	@echo "  bench              - Run benchmark suite"
	@echo "  bench-10gb         - Run 10GB benchmark vs DuckDB"
	@echo "  clean              - Remove build artifacts"
	@echo "  install            - Install to GOPATH/bin"
	@echo "  release            - Create release with goreleaser"
	@echo "  release-snapshot   - Create snapshot release"
	@echo "  gen-test-data      - Generate test CSV data"
	@echo "  profile            - Run CPU profiler"
	@echo "  help               - Show this help"
