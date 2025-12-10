package engine

import (
	"bytes"
	"encoding/csv"
	"os"
	"strings"
	"testing"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

func TestOrderByNumeric(t *testing.T) {
	// Create test CSV
	csvData := `name,age
Alice,30
Bob,25
Charlie,35
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name, age FROM '" + tmpfile.Name() + "' ORDER BY age")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	reader := csv.NewReader(&buf)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	// Should be header + 3 rows sorted by age (25, 30, 35)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows (header + 3 data), got %d", len(rows))
	}

	if rows[1][0] != "Bob" || rows[1][1] != "25" {
		t.Errorf("expected Bob,25 first, got %v", rows[1])
	}

	if rows[2][0] != "Alice" || rows[2][1] != "30" {
		t.Errorf("expected Alice,30 second, got %v", rows[2])
	}

	if rows[3][0] != "Charlie" || rows[3][1] != "35" {
		t.Errorf("expected Charlie,35 third, got %v", rows[3])
	}
}

func TestOrderByDescending(t *testing.T) {
	csvData := `name,score
Alice,85
Bob,92
Charlie,78
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name, score FROM '" + tmpfile.Name() + "' ORDER BY score DESC")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	reader := csv.NewReader(&buf)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	// Should be sorted DESC: 92, 85, 78
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	if rows[1][0] != "Bob" {
		t.Errorf("expected Bob first (92), got %v", rows[1])
	}

	if rows[2][0] != "Alice" {
		t.Errorf("expected Alice second (85), got %v", rows[2])
	}

	if rows[3][0] != "Charlie" {
		t.Errorf("expected Charlie third (78), got %v", rows[3])
	}
}

func TestOrderByString(t *testing.T) {
	csvData := `name,country
Alice,USA
Bob,Canada
Charlie,Australia
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name, country FROM '" + tmpfile.Name() + "' ORDER BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	reader := csv.NewReader(&buf)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	// Should be sorted alphabetically: Australia, Canada, USA
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	if rows[1][1] != "Australia" {
		t.Errorf("expected Australia first, got %v", rows[1])
	}

	if rows[2][1] != "Canada" {
		t.Errorf("expected Canada second, got %v", rows[2])
	}

	if rows[3][1] != "USA" {
		t.Errorf("expected USA third, got %v", rows[3])
	}
}

func TestOrderByMultipleColumns(t *testing.T) {
	csvData := `name,country,age
Alice,USA,30
Bob,USA,25
Charlie,Canada,30
Diana,Canada,25
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name, country, age FROM '" + tmpfile.Name() + "' ORDER BY country ASC, age DESC")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	reader := csv.NewReader(&buf)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	// Should be sorted by country ASC, then age DESC within each country
	// Canada (Charlie 30, Diana 25), USA (Alice 30, Bob 25)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	if rows[1][0] != "Charlie" {
		t.Errorf("expected Charlie first, got %v", rows[1])
	}

	if rows[2][0] != "Diana" {
		t.Errorf("expected Diana second, got %v", rows[2])
	}

	if rows[3][0] != "Alice" {
		t.Errorf("expected Alice third, got %v", rows[3])
	}

	if rows[4][0] != "Bob" {
		t.Errorf("expected Bob fourth, got %v", rows[4])
	}
}

func TestOrderByWithWhere(t *testing.T) {
	csvData := `name,age,country
Alice,30,USA
Bob,25,USA
Charlie,35,Canada
Diana,40,USA
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name, age FROM '" + tmpfile.Name() + "' WHERE country = 'USA' ORDER BY age DESC")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	reader := csv.NewReader(&buf)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	// Should filter to USA only, then sort by age DESC: Diana 40, Alice 30, Bob 25
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows (header + 3 USA rows), got %d", len(rows))
	}

	if rows[1][0] != "Diana" {
		t.Errorf("expected Diana first (40), got %v", rows[1])
	}

	if rows[2][0] != "Alice" {
		t.Errorf("expected Alice second (30), got %v", rows[2])
	}

	if rows[3][0] != "Bob" {
		t.Errorf("expected Bob third (25), got %v", rows[3])
	}
}

func TestOrderByWithLimit(t *testing.T) {
	csvData := `name,score
Alice,85
Bob,92
Charlie,78
Diana,95
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name, score FROM '" + tmpfile.Name() + "' ORDER BY score DESC LIMIT 2")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	reader := csv.NewReader(&buf)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	// Should be top 2: Diana 95, Bob 92
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (header + 2 data), got %d", len(rows))
	}

	if rows[1][0] != "Diana" {
		t.Errorf("expected Diana first (95), got %v", rows[1])
	}

	if rows[2][0] != "Bob" {
		t.Errorf("expected Bob second (92), got %v", rows[2])
	}
}

func TestOrderByInvalidColumn(t *testing.T) {
	csvData := `name,age
Alice,30
`
	tmpfile, err := os.CreateTemp("", "test_orderby_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(csvData)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	query, err := sqlparser.Parse("SELECT name FROM '" + tmpfile.Name() + "' ORDER BY invalid_column")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	err = Execute(query, &buf)
	if err == nil {
		t.Fatal("expected error for invalid ORDER BY column")
	}

	if !strings.Contains(err.Error(), "ORDER BY column not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}
