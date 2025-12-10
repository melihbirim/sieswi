package engine

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

// createTestCSV creates a temporary CSV file for testing
func createTestCSV(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	return tmpFile
}

// parseCSVOutput parses CSV output into rows
func parseCSVOutput(t *testing.T, output string) [][]string {
	t.Helper()
	reader := csv.NewReader(strings.NewReader(output))
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse CSV output: %v", err)
	}
	return rows
}

func TestGroupByCount(t *testing.T) {
	csvContent := `country,status,amount
US,completed,100
US,pending,200
UK,completed,150
UK,completed,250
US,completed,300`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, COUNT(*) FROM '" + tmpFile + "' GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	if len(rows) != 3 { // header + 2 groups
		t.Fatalf("expected 3 rows (header + 2 groups), got %d", len(rows))
	}

	// Check header
	if rows[0][0] != "country" || rows[0][1] != "COUNT(*)" {
		t.Errorf("unexpected header: %v", rows[0])
	}

	// Verify counts (US: 3, UK: 2)
	counts := make(map[string]string)
	for i := 1; i < len(rows); i++ {
		counts[rows[i][0]] = rows[i][1]
	}

	if counts["US"] != "3" {
		t.Errorf("expected US count=3, got %s", counts["US"])
	}
	if counts["UK"] != "2" {
		t.Errorf("expected UK count=2, got %s", counts["UK"])
	}
}

func TestGroupBySum(t *testing.T) {
	csvContent := `country,amount
US,100
US,200
UK,150
UK,250`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, SUM(amount) FROM '" + tmpFile + "' GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	sums := make(map[string]string)
	for i := 1; i < len(rows); i++ {
		sums[rows[i][0]] = rows[i][1]
	}

	if sums["US"] != "300.00" {
		t.Errorf("expected US sum=300.00, got %s", sums["US"])
	}
	if sums["UK"] != "400.00" {
		t.Errorf("expected UK sum=400.00, got %s", sums["UK"])
	}
}

func TestGroupByAvg(t *testing.T) {
	csvContent := `country,amount
US,100
US,200
UK,100
UK,300`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, AVG(amount) FROM '" + tmpFile + "' GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	avgs := make(map[string]string)
	for i := 1; i < len(rows); i++ {
		avgs[rows[i][0]] = rows[i][1]
	}

	if avgs["US"] != "150.00" {
		t.Errorf("expected US avg=150.00, got %s", avgs["US"])
	}
	if avgs["UK"] != "200.00" {
		t.Errorf("expected UK avg=200.00, got %s", avgs["UK"])
	}
}

func TestGroupByMinMax(t *testing.T) {
	csvContent := `country,amount
US,100
US,300
UK,150
UK,250`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, MIN(amount), MAX(amount) FROM '" + tmpFile + "' GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	results := make(map[string][]string)
	for i := 1; i < len(rows); i++ {
		results[rows[i][0]] = []string{rows[i][1], rows[i][2]}
	}

	// US: min=100, max=300
	if results["US"][0] != "100.00" || results["US"][1] != "300.00" {
		t.Errorf("expected US min=100.00, max=300.00, got min=%s, max=%s", results["US"][0], results["US"][1])
	}

	// UK: min=150, max=250
	if results["UK"][0] != "150.00" || results["UK"][1] != "250.00" {
		t.Errorf("expected UK min=150.00, max=250.00, got min=%s, max=%s", results["UK"][0], results["UK"][1])
	}
}

func TestGroupByMultipleColumns(t *testing.T) {
	csvContent := `country,status,amount
US,completed,100
US,pending,200
US,completed,300
UK,completed,150
UK,pending,250`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, status, COUNT(*) FROM '" + tmpFile + "' GROUP BY country, status")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	if len(rows) != 5 { // header + 4 groups
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	// Verify we have 4 distinct groups
	groups := make(map[string]string)
	for i := 1; i < len(rows); i++ {
		key := rows[i][0] + ":" + rows[i][1]
		groups[key] = rows[i][2]
	}

	if len(groups) != 4 {
		t.Errorf("expected 4 groups, got %d: %v", len(groups), groups)
	}

	if groups["US:completed"] != "2" {
		t.Errorf("expected US:completed count=2, got %s", groups["US:completed"])
	}
}

func TestGroupByWithWhere(t *testing.T) {
	csvContent := `country,status,amount
US,completed,100
US,pending,200
UK,completed,150
UK,completed,250
US,completed,50`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, COUNT(*) FROM '" + tmpFile + "' WHERE amount > 100 GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	counts := make(map[string]string)
	for i := 1; i < len(rows); i++ {
		counts[rows[i][0]] = rows[i][1]
	}

	// US: 1 row (amount=200), UK: 2 rows (amount=150,250)
	if counts["US"] != "1" {
		t.Errorf("expected US count=1 (filtered), got %s", counts["US"])
	}
	if counts["UK"] != "2" {
		t.Errorf("expected UK count=2, got %s", counts["UK"])
	}
}

func TestGroupByWithLimit(t *testing.T) {
	csvContent := `country,amount
US,100
UK,200
CA,300
FR,400`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, COUNT(*) FROM '" + tmpFile + "' GROUP BY country LIMIT 2")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	if len(rows) != 3 { // header + 2 limited rows
		t.Fatalf("expected 3 rows (header + 2 limited), got %d", len(rows))
	}
}

func TestGroupByMultipleAggregates(t *testing.T) {
	csvContent := `country,amount
US,100
US,200
UK,150`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM '" + tmpFile + "' GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())

	// Check header
	if len(rows[0]) != 6 {
		t.Errorf("expected 6 columns in header, got %d", len(rows[0]))
	}

	// Find US row
	var usRow []string
	for i := 1; i < len(rows); i++ {
		if rows[i][0] == "US" {
			usRow = rows[i]
			break
		}
	}

	if usRow == nil {
		t.Fatal("US row not found")
	}

	// US: count=2, sum=300, avg=150, min=100, max=200
	if usRow[1] != "2" {
		t.Errorf("expected count=2, got %s", usRow[1])
	}
	if usRow[2] != "300.00" {
		t.Errorf("expected sum=300.00, got %s", usRow[2])
	}
	if usRow[3] != "150.00" {
		t.Errorf("expected avg=150.00, got %s", usRow[3])
	}
	if usRow[4] != "100.00" {
		t.Errorf("expected min=100.00, got %s", usRow[4])
	}
	if usRow[5] != "200.00" {
		t.Errorf("expected max=200.00, got %s", usRow[5])
	}
}

func TestGroupByEmptyResult(t *testing.T) {
	csvContent := `country,amount
US,100
UK,200`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, COUNT(*) FROM '" + tmpFile + "' WHERE amount > 1000 GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	if err := Execute(query, &buf); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	rows := parseCSVOutput(t, buf.String())
	if len(rows) != 1 { // only header
		t.Fatalf("expected 1 row (header only), got %d", len(rows))
	}
}

func TestGroupByInvalidColumn(t *testing.T) {
	csvContent := `country,amount
US,100`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT country, COUNT(*) FROM '" + tmpFile + "' GROUP BY nonexistent")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	err = Execute(query, &buf)
	if err == nil {
		t.Fatal("expected error for nonexistent GROUP BY column")
	}
	if !strings.Contains(err.Error(), "GROUP BY column not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGroupBySelectStarError(t *testing.T) {
	csvContent := `country,amount
US,100`

	tmpFile := createTestCSV(t, csvContent)

	query, err := sqlparser.Parse("SELECT * FROM '" + tmpFile + "' GROUP BY country")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	var buf bytes.Buffer
	err = Execute(query, &buf)
	if err == nil {
		t.Fatal("expected error for SELECT * with GROUP BY")
	}
	if !strings.Contains(err.Error(), "SELECT * not supported with GROUP BY") {
		t.Errorf("unexpected error message: %v", err)
	}
}
