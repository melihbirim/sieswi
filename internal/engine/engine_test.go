package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}
	return path
}

func TestExecuteStreamsProjectedRows(t *testing.T) {
	csvPath := writeTempCSV(t, "id,name,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n")

	q := sqlparser.Query{
		Columns:  []string{"name", "amount"},
		FilePath: csvPath,
		Limit:    -1,
	}

	var out bytes.Buffer
	if err := Execute(q, &out); err != nil {
		t.Fatalf("execute query: %v", err)
	}

	want := "name,amount\nalpha,10\nbeta,20\ngamma,30\n"
	if got := out.String(); got != want {
		t.Fatalf("unexpected output.\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestExecuteRespectsPredicateAndLimit(t *testing.T) {
	csvPath := writeTempCSV(t, "id,amount\n1,5\n2,15\n3,25\n")

	q := sqlparser.Query{
		AllColumns: true,
		FilePath:   csvPath,
		Where: sqlparser.Comparison{
			Column:       "amount",
			Operator:     ">",
			Value:        "10",
			IsNumeric:    true,
			NumericValue: 10,
		},
		Limit: 1,
	}

	var out bytes.Buffer
	if err := Execute(q, &out); err != nil {
		t.Fatalf("execute query: %v", err)
	}

	want := "id,amount\n2,15\n"
	if got := out.String(); got != want {
		t.Fatalf("unexpected output.\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestExecuteEmptyCSV(t *testing.T) {
	csvPath := writeTempCSV(t, "name,age,city\n")

	q := sqlparser.Query{
		AllColumns: true,
		FilePath:   csvPath,
		Limit:      -1,
	}

	var out bytes.Buffer
	if err := Execute(q, &out); err != nil {
		t.Fatalf("execute query: %v", err)
	}

	want := "name,age,city\n"
	if got := out.String(); got != want {
		t.Errorf("expected header only, got: %s", got)
	}
}

func TestExecuteZeroMatches(t *testing.T) {
	csvPath := writeTempCSV(t, "name,age\nAlice,30\nBob,25\n")

	q := sqlparser.Query{
		AllColumns: true,
		FilePath:   csvPath,
		Where: sqlparser.Comparison{
			Column:   "name",
			Operator: "=",
			Value:    "Charlie",
		},
		Limit: -1,
	}

	var out bytes.Buffer
	if err := Execute(q, &out); err != nil {
		t.Fatalf("execute query: %v", err)
	}

	want := "name,age\n"
	if got := out.String(); got != want {
		t.Errorf("expected header only, got: %s", got)
	}
}

func TestExecuteMissingColumn(t *testing.T) {
	csvPath := writeTempCSV(t, "name,age\nAlice,30\n")

	q := sqlparser.Query{
		AllColumns: true,
		FilePath:   csvPath,
		Where: sqlparser.Comparison{
			Column:   "city",
			Operator: "=",
			Value:    "NYC",
		},
		Limit: -1,
	}

	var out bytes.Buffer
	err := Execute(q, &out)
	if err == nil {
		t.Fatal("expected error for missing column, got nil")
	}

	if !contains(err.Error(), "city") {
		t.Errorf("expected error mentioning 'city', got: %v", err)
	}
}

func TestExecuteAllOperators(t *testing.T) {
	csvPath := writeTempCSV(t, "id,value\n1,10\n2,20\n3,30\n4,40\n")

	tests := []struct {
		operator     string
		value        float64
		expectedRows string
	}{
		{"=", 20, "id,value\n2,20\n"},
		{"!=", 20, "id,value\n1,10\n3,30\n4,40\n"},
		{">", 20, "id,value\n3,30\n4,40\n"},
		{">=", 20, "id,value\n2,20\n3,30\n4,40\n"},
		{"<", 30, "id,value\n1,10\n2,20\n"},
		{"<=", 30, "id,value\n1,10\n2,20\n3,30\n"},
	}

	for _, tt := range tests {
		t.Run(tt.operator, func(t *testing.T) {
			q := sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: sqlparser.Comparison{
					Column:       "value",
					Operator:     tt.operator,
					Value:        "",
					NumericValue: tt.value,
					IsNumeric:    true,
				},
				Limit: -1,
			}

			var out bytes.Buffer
			if err := Execute(q, &out); err != nil {
				t.Fatalf("execute query: %v", err)
			}

			if got := out.String(); got != tt.expectedRows {
				t.Errorf("operator %s with value %.0f:\nwant:\n%s\ngot:\n%s",
					tt.operator, tt.value, tt.expectedRows, got)
			}
		})
	}
}

func TestExecuteCaseInsensitiveColumns(t *testing.T) {
	csvPath := writeTempCSV(t, "Name,AGE,CiTy\nAlice,30,NYC\n")

	q := sqlparser.Query{
		Columns:  []string{"name", "age"},
		FilePath: csvPath,
		Limit:    -1,
	}

	var out bytes.Buffer
	if err := Execute(q, &out); err != nil {
		t.Fatalf("execute query: %v", err)
	}

	want := "Name,AGE\nAlice,30\n"
	if got := out.String(); got != want {
		t.Errorf("case-insensitive match failed:\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestExecuteStringComparisons(t *testing.T) {
	csvPath := writeTempCSV(t, "name,status\nAlice,ACTIVE\nBob,INACTIVE\nCharlie,ACTIVE\n")

	tests := []struct {
		operator string
		value    string
		expected string
	}{
		{"=", "ACTIVE", "name,status\nAlice,ACTIVE\nCharlie,ACTIVE\n"},
		{"!=", "ACTIVE", "name,status\nBob,INACTIVE\n"},
	}

	for _, tt := range tests {
		t.Run(tt.operator+"_"+tt.value, func(t *testing.T) {
			q := sqlparser.Query{
				AllColumns: true,
				FilePath:   csvPath,
				Where: sqlparser.Comparison{
					Column:    "status",
					Operator:  tt.operator,
					Value:     tt.value,
					IsNumeric: false,
				},
				Limit: -1,
			}

			var out bytes.Buffer
			if err := Execute(q, &out); err != nil {
				t.Fatalf("execute query: %v", err)
			}

			if got := out.String(); got != tt.expected {
				t.Errorf("string comparison failed:\nwant:\n%s\ngot:\n%s", tt.expected, got)
			}
		})
	}
}

func TestExecuteFileNotFound(t *testing.T) {
	q := sqlparser.Query{
		AllColumns: true,
		FilePath:   "/nonexistent/file.csv",
		Limit:      -1,
	}

	var out bytes.Buffer
	err := Execute(q, &out)
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			contains(s[1:], substr)))
}
