package engine_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/melihbirim/sieswi/internal/engine"
	"github.com/melihbirim/sieswi/internal/sidx"
	"github.com/melihbirim/sieswi/internal/sqlparser"
)

// Benchmark control: DuckDB results on 1M row CSV (77MB)
//
// Test queries run on fixtures/ecommerce_1m.csv with these DuckDB times:
//
// 1. Single predicate (baseline):
//    WHERE country = 'UK' LIMIT 1000
//    DuckDB: 180ms
//
// 2. AND with two columns:
//    WHERE country = 'UK' AND amount > 100 LIMIT 1000
//    DuckDB: 185ms
//
// 3. OR with two columns:
//    WHERE country = 'UK' OR country = 'US' LIMIT 1000
//    DuckDB: 190ms
//
// 4. Complex nested:
//    WHERE (country = 'UK' OR country = 'US') AND amount > 100 LIMIT 1000
//    DuckDB: 195ms
//
// 5. NOT operator:
//    WHERE NOT country = 'UK' LIMIT 1000
//    DuckDB: 188ms
//
// Target: sieswi should be within 2x of DuckDB without index,
//         and 10-30x faster with index on selective queries.

func BenchmarkBooleanPredicates(b *testing.B) {
	csvPath := "../../fixtures/ecommerce_1m.csv"
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		b.Skip("Benchmark CSV not found (run: go run cmd/gencsv/main.go)")
	}

	// Build index once
	indexPath := csvPath + ".sidx"
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		builder := sidx.NewBuilder(sidx.BlockSize)
		index, err := builder.BuildFromFile(csvPath)
		if err != nil {
			b.Fatalf("build index: %v", err)
		}
		indexFile, err := os.Create(indexPath)
		if err != nil {
			b.Fatalf("create index file: %v", err)
		}
		if err := sidx.WriteIndex(indexFile, index); err != nil {
			indexFile.Close()
			b.Fatalf("write index: %v", err)
		}
		indexFile.Close()
	}

	tests := []struct {
		name  string
		query sqlparser.Query
	}{
		{
			name: "SinglePredicate_Baseline",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: sqlparser.Comparison{
					Column:    "country",
					Operator:  "=",
					Value:     "UK",
					IsNumeric: false,
				},
				Limit: 1000,
			},
		},
		{
			name: "AND_TwoColumns",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "UK",
						IsNumeric: false,
					},
					Operator: "AND",
					Right: sqlparser.Comparison{
						Column:       "amount",
						Operator:     ">",
						Value:        "100",
						IsNumeric:    true,
						NumericValue: 100,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "OR_TwoCountries",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "UK",
						IsNumeric: false,
					},
					Operator: "OR",
					Right: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "US",
						IsNumeric: false,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "Complex_Nested",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: &sqlparser.BinaryExpr{
						Left: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "UK",
							IsNumeric: false,
						},
						Operator: "OR",
						Right: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "US",
							IsNumeric: false,
						},
					},
					Operator: "AND",
					Right: sqlparser.Comparison{
						Column:       "amount",
						Operator:     ">",
						Value:        "100",
						IsNumeric:    true,
						NumericValue: 100,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "NOT_Operator",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.UnaryExpr{
					Operator: "NOT",
					Expr: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "UK",
						IsNumeric: false,
					},
				},
				Limit: 1000,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bytes.Buffer
				if err := engine.Execute(tt.query, &out); err != nil {
					b.Fatalf("execute: %v", err)
				}
			}
		})
	}
}

func BenchmarkBooleanPredicatesNoIndex(b *testing.B) {
	csvPath := "../../fixtures/ecommerce_1m.csv"
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		b.Skip("Benchmark CSV not found")
	}

	// Remove index to force full scan
	indexPath := csvPath + ".sidx"
	os.Remove(indexPath)
	defer func() {
		// Rebuild index after benchmark
		builder := sidx.NewBuilder(sidx.BlockSize)
		if index, err := builder.BuildFromFile(csvPath); err == nil {
			if indexFile, err := os.Create(indexPath); err == nil {
				sidx.WriteIndex(indexFile, index)
				indexFile.Close()
			}
		}
	}()

	tests := []struct {
		name  string
		query sqlparser.Query
	}{
		{
			name: "SinglePredicate_NoIndex",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: sqlparser.Comparison{
					Column:    "country",
					Operator:  "=",
					Value:     "UK",
					IsNumeric: false,
				},
				Limit: 1000,
			},
		},
		{
			name: "AND_TwoColumns_NoIndex",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "UK",
						IsNumeric: false,
					},
					Operator: "AND",
					Right: sqlparser.Comparison{
						Column:       "amount",
						Operator:     ">",
						Value:        "100",
						IsNumeric:    true,
						NumericValue: 100,
					},
				},
				Limit: 1000,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bytes.Buffer
				if err := engine.Execute(tt.query, &out); err != nil {
					b.Fatalf("execute: %v", err)
				}
			}
		})
	}
}

func BenchmarkBooleanPredicates10GB(b *testing.B) {
	if os.Getenv("TEST_10GB") != "1" {
		b.Skip("Skipping 10GB benchmark (set TEST_10GB=1 to enable)")
	}

	csvPath := "../../fixtures/ecommerce_10gb.csv"
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		b.Skip("10GB CSV not found")
	}

	// Ensure index exists
	indexPath := csvPath + ".sidx"
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		b.Log("Building 10GB index (one-time setup)...")
		builder := sidx.NewBuilder(sidx.BlockSize)
		index, err := builder.BuildFromFile(csvPath)
		if err != nil {
			b.Fatalf("build index: %v", err)
		}
		indexFile, err := os.Create(indexPath)
		if err != nil {
			b.Fatalf("create index file: %v", err)
		}
		if err := sidx.WriteIndex(indexFile, index); err != nil {
			indexFile.Close()
			b.Fatalf("write index: %v", err)
		}
		indexFile.Close()
	}

	b.Run("10GB_ComplexQuery", func(b *testing.B) {
		query := sqlparser.Query{
			AllColumns: true,
			FilePath:   csvPath,
			Where: &sqlparser.BinaryExpr{
				Left: &sqlparser.BinaryExpr{
					Left: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "UK",
						IsNumeric: false,
					},
					Operator: "OR",
					Right: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "US",
						IsNumeric: false,
					},
				},
				Operator: "AND",
				Right: sqlparser.Comparison{
					Column:       "amount",
					Operator:     ">",
					Value:        "100",
					IsNumeric:    true,
					NumericValue: 100,
				},
			},
			Limit: 1000,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out bytes.Buffer
			if err := engine.Execute(query, &out); err != nil {
				b.Fatalf("execute: %v", err)
			}
		}
	})
}

// TestBooleanExpressionsCorrectness validates that boolean expressions produce correct results
func TestBooleanExpressionsCorrectness(t *testing.T) {
	// Create test CSV
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")
	content := `country,amount,status
UK,50,active
UK,150,active
US,120,active
US,80,inactive
FR,200,active
UK,30,inactive`

	if err := os.WriteFile(csvPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		where    sqlparser.Expression
		expected int // number of rows expected
	}{
		{
			name: "AND_both_true",
			where: &sqlparser.BinaryExpr{
				Left:     sqlparser.Comparison{Column: "country", Operator: "=", Value: "UK"},
				Operator: "AND",
				Right:    sqlparser.Comparison{Column: "amount", Operator: ">", Value: "100", IsNumeric: true, NumericValue: 100},
			},
			expected: 1, // UK,150,active
		},
		{
			name: "OR_either_true",
			where: &sqlparser.BinaryExpr{
				Left:     sqlparser.Comparison{Column: "country", Operator: "=", Value: "UK"},
				Operator: "OR",
				Right:    sqlparser.Comparison{Column: "country", Operator: "=", Value: "US"},
			},
			expected: 5, // 3 UK + 2 US = 5 rows total
		},
		{
			name: "NOT_negation",
			where: &sqlparser.UnaryExpr{
				Operator: "NOT",
				Expr:     sqlparser.Comparison{Column: "country", Operator: "=", Value: "UK"},
			},
			expected: 3, // US,US,FR
		},
		{
			name: "Complex_precedence",
			where: &sqlparser.BinaryExpr{
				Left: &sqlparser.BinaryExpr{
					Left:     sqlparser.Comparison{Column: "country", Operator: "=", Value: "UK"},
					Operator: "OR",
					Right:    sqlparser.Comparison{Column: "country", Operator: "=", Value: "US"},
				},
				Operator: "AND",
				Right:    sqlparser.Comparison{Column: "status", Operator: "=", Value: "active"},
			},
			expected: 3, // UK,150 + US,120 + UK,50 = 3 (UK,30 is inactive)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where:      tt.where,
				Limit:      -1,
			}

			var out bytes.Buffer
			if err := engine.Execute(query, &out); err != nil {
				t.Fatalf("execute: %v", err)
			}

			// Count lines (header + data rows)
			lines := bytes.Count(out.Bytes(), []byte("\n"))
			gotRows := lines - 1 // subtract header
			if gotRows != tt.expected {
				t.Errorf("got %d rows, want %d\nOutput:\n%s", gotRows, tt.expected, out.String())
			}
		})
	}
}

// Benchmark10M tests performance on 10M row dataset (~770MB)
func Benchmark10MRows(b *testing.B) {
	csvPath := "../../fixtures/ecommerce_10m.csv"
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		b.Skip("10M CSV not found (run: go run cmd/gencsv/main.go -rows 10000000 -out fixtures/ecommerce_10m.csv)")
	}

	// Build index once
	indexPath := csvPath + ".sidx"
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		b.Log("Building index for 10M rows (one-time setup)...")
		builder := sidx.NewBuilder(sidx.BlockSize)
		index, err := builder.BuildFromFile(csvPath)
		if err != nil {
			b.Fatalf("build index: %v", err)
		}
		indexFile, err := os.Create(indexPath)
		if err != nil {
			b.Fatalf("create index file: %v", err)
		}
		if err := sidx.WriteIndex(indexFile, index); err != nil {
			indexFile.Close()
			b.Fatalf("write index: %v", err)
		}
		indexFile.Close()
	}

	tests := []struct {
		name  string
		query sqlparser.Query
	}{
		{
			name: "SinglePredicate_10M",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: sqlparser.Comparison{
					Column:    "country",
					Operator:  "=",
					Value:     "UK",
					IsNumeric: false,
				},
				Limit: 1000,
			},
		},
		{
			name: "AND_TwoColumns_10M",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "UK",
						IsNumeric: false,
					},
					Operator: "AND",
					Right: sqlparser.Comparison{
						Column:       "total_minor",
						Operator:     ">",
						Value:        "10000",
						IsNumeric:    true,
						NumericValue: 10000,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "OR_ThreeCountries_10M",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: &sqlparser.BinaryExpr{
						Left: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "UK",
							IsNumeric: false,
						},
						Operator: "OR",
						Right: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "US",
							IsNumeric: false,
						},
					},
					Operator: "OR",
					Right: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "FR",
						IsNumeric: false,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "ComplexNested_10M",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: &sqlparser.BinaryExpr{
						Left: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "UK",
							IsNumeric: false,
						},
						Operator: "OR",
						Right: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "US",
							IsNumeric: false,
						},
					},
					Operator: "AND",
					Right: &sqlparser.BinaryExpr{
						Left: sqlparser.Comparison{
							Column:       "total_minor",
							Operator:     ">",
							Value:        "5000",
							IsNumeric:    true,
							NumericValue: 5000,
						},
						Operator: "AND",
						Right: sqlparser.Comparison{
							Column:    "status",
							Operator:  "=",
							Value:     "shipped",
							IsNumeric: false,
						},
					},
				},
				Limit: 100,
			},
		},
		{
			name: "NOT_HighValue_10M",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.UnaryExpr{
					Operator: "NOT",
					Expr: sqlparser.Comparison{
						Column:       "total_minor",
						Operator:     ">",
						Value:        "50000",
						IsNumeric:    true,
						NumericValue: 50000,
					},
				},
				Limit: 1000,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bytes.Buffer
				if err := engine.Execute(tt.query, &out); err != nil {
					b.Fatalf("execute: %v", err)
				}
			}
		})
	}
}

// BenchmarkTrickySQL tests edge cases and complex expressions
func BenchmarkTrickySQL(b *testing.B) {
	csvPath := "../../fixtures/ecommerce_1m.csv"
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		b.Skip("Benchmark CSV not found")
	}

	// Build index
	indexPath := csvPath + ".sidx"
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		builder := sidx.NewBuilder(sidx.BlockSize)
		index, err := builder.BuildFromFile(csvPath)
		if err != nil {
			b.Fatalf("build index: %v", err)
		}
		indexFile, err := os.Create(indexPath)
		if err != nil {
			b.Fatalf("create index file: %v", err)
		}
		if err := sidx.WriteIndex(indexFile, index); err != nil {
			indexFile.Close()
			b.Fatalf("write index: %v", err)
		}
		indexFile.Close()
	}

	tests := []struct {
		name  string
		query sqlparser.Query
	}{
		{
			name: "DeepNesting_4Levels",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: &sqlparser.BinaryExpr{
						Left: &sqlparser.BinaryExpr{
							Left: sqlparser.Comparison{
								Column:    "country",
								Operator:  "=",
								Value:     "UK",
								IsNumeric: false,
							},
							Operator: "OR",
							Right: sqlparser.Comparison{
								Column:    "country",
								Operator:  "=",
								Value:     "US",
								IsNumeric: false,
							},
						},
						Operator: "OR",
						Right: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "FR",
							IsNumeric: false,
						},
					},
					Operator: "AND",
					Right: sqlparser.Comparison{
						Column:    "status",
						Operator:  "!=",
						Value:     "cancelled",
						IsNumeric: false,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "DoubleNOT",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.UnaryExpr{
					Operator: "NOT",
					Expr: &sqlparser.UnaryExpr{
						Operator: "NOT",
						Expr: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "UK",
							IsNumeric: false,
						},
					},
				},
				Limit: 500,
			},
		},
		{
			name: "MixedOperators_AllThree",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: &sqlparser.UnaryExpr{
						Operator: "NOT",
						Expr: sqlparser.Comparison{
							Column:    "status",
							Operator:  "=",
							Value:     "cancelled",
							IsNumeric: false,
						},
					},
					Operator: "AND",
					Right: &sqlparser.BinaryExpr{
						Left: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "UK",
							IsNumeric: false,
						},
						Operator: "OR",
						Right: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "US",
							IsNumeric: false,
						},
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "NumericRange_AND",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: sqlparser.Comparison{
						Column:       "total_minor",
						Operator:     ">",
						Value:        "5000",
						IsNumeric:    true,
						NumericValue: 5000,
					},
					Operator: "AND",
					Right: sqlparser.Comparison{
						Column:       "total_minor",
						Operator:     "<",
						Value:        "10000",
						IsNumeric:    true,
						NumericValue: 10000,
					},
				},
				Limit: 1000,
			},
		},
		{
			name: "MultipleOR_5Countries",
			query: sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: &sqlparser.BinaryExpr{
					Left: &sqlparser.BinaryExpr{
						Left: &sqlparser.BinaryExpr{
							Left: &sqlparser.BinaryExpr{
								Left: sqlparser.Comparison{
									Column:    "country",
									Operator:  "=",
									Value:     "UK",
									IsNumeric: false,
								},
								Operator: "OR",
								Right: sqlparser.Comparison{
									Column:    "country",
									Operator:  "=",
									Value:     "US",
									IsNumeric: false,
								},
							},
							Operator: "OR",
							Right: sqlparser.Comparison{
								Column:    "country",
								Operator:  "=",
								Value:     "FR",
								IsNumeric: false,
							},
						},
						Operator: "OR",
						Right: sqlparser.Comparison{
							Column:    "country",
							Operator:  "=",
							Value:     "DE",
							IsNumeric: false,
						},
					},
					Operator: "OR",
					Right: sqlparser.Comparison{
						Column:    "country",
						Operator:  "=",
						Value:     "JP",
						IsNumeric: false,
					},
				},
				Limit: 1000,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bytes.Buffer
				if err := engine.Execute(tt.query, &out); err != nil {
					b.Fatalf("execute: %v", err)
				}
			}
		})
	}
}
