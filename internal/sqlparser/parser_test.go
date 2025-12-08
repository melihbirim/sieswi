package sqlparser

import "testing"

func TestParseBasicQuery(t *testing.T) {
	q, err := Parse("SELECT col1, col2 FROM data.csv WHERE col1 = '42' LIMIT 10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if q.FilePath != "data.csv" {
		t.Fatalf("unexpected file path: %s", q.FilePath)
	}

	if len(q.Columns) != 2 || q.Columns[0] != "col1" || q.Columns[1] != "col2" {
		t.Fatalf("unexpected columns: %#v", q.Columns)
	}

	if q.Limit != 10 {
		t.Fatalf("unexpected limit: %d", q.Limit)
	}

	if q.Where == nil {
		t.Fatalf("expected WHERE expression")
	}

	// Check it's a simple comparison
	if _, ok := q.Where.(Comparison); !ok {
		t.Fatalf("expected WHERE to be a Comparison, got %T", q.Where)
	}
}

func TestParseQuotedFilePath(t *testing.T) {
	q, err := Parse(`SELECT * FROM "my data.csv" WHERE amount >= 10`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if q.FilePath != "my data.csv" {
		t.Fatalf("expected quoted path to be trimmed, got %q", q.FilePath)
	}
}

func TestParseHandlesWhitespace(t *testing.T) {
	q, err := Parse("  SELECT   col1  ,  col2    FROM   ./data.csv   LIMIT   5  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.Columns) != 2 || q.Columns[0] != "col1" || q.Columns[1] != "col2" {
		t.Fatalf("unexpected columns: %#v", q.Columns)
	}

	if q.Limit != 5 {
		t.Fatalf("unexpected limit: %d", q.Limit)
	}
}

func TestParseRejectsInvalidQueries(t *testing.T) {
	if _, err := Parse("DELETE FROM data.csv"); err == nil {
		t.Fatalf("expected error for unsupported verb")
	}

	if _, err := Parse("SELECT col1,, col2 FROM data.csv"); err == nil {
		t.Fatalf("expected error for empty column")
	}

	if _, err := Parse("SELECT * FROM ''"); err == nil {
		t.Fatalf("expected error for missing file path")
	}
}

func TestPredicateCompare(t *testing.T) {
	pred := Predicate{Column: "col", Operator: ">", Value: "10", NumericValue: 10, IsNumeric: true}
	if !pred.Compare("11") {
		t.Fatalf("expected predicate to match")
	}
	if pred.Compare("10") {
		t.Fatalf("did not expect predicate to match")
	}
}
