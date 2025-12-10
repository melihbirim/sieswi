package sqlparser

import "testing"

func TestParseOrderBy(t *testing.T) {
	q, err := Parse("SELECT name, age FROM data.csv ORDER BY age")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(q.OrderBy))
	}

	if q.OrderBy[0].Column != "age" {
		t.Fatalf("expected ORDER BY age, got %s", q.OrderBy[0].Column)
	}

	if q.OrderBy[0].Descending {
		t.Fatalf("expected ASC (default), got DESC")
	}
}

func TestParseOrderByDesc(t *testing.T) {
	q, err := Parse("SELECT name, age FROM data.csv ORDER BY age DESC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(q.OrderBy))
	}

	if q.OrderBy[0].Column != "age" {
		t.Fatalf("expected ORDER BY age, got %s", q.OrderBy[0].Column)
	}

	if !q.OrderBy[0].Descending {
		t.Fatalf("expected DESC, got ASC")
	}
}

func TestParseOrderByAsc(t *testing.T) {
	q, err := Parse("SELECT name, age FROM data.csv ORDER BY age ASC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(q.OrderBy))
	}

	if q.OrderBy[0].Column != "age" {
		t.Fatalf("expected ORDER BY age, got %s", q.OrderBy[0].Column)
	}

	if q.OrderBy[0].Descending {
		t.Fatalf("expected ASC, got DESC")
	}
}

func TestParseOrderByMultiple(t *testing.T) {
	q, err := Parse("SELECT name, age, country FROM data.csv ORDER BY country ASC, age DESC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY columns, got %d", len(q.OrderBy))
	}

	if q.OrderBy[0].Column != "country" || q.OrderBy[0].Descending {
		t.Fatalf("expected ORDER BY country ASC, got %s %v", q.OrderBy[0].Column, q.OrderBy[0].Descending)
	}

	if q.OrderBy[1].Column != "age" || !q.OrderBy[1].Descending {
		t.Fatalf("expected ORDER BY age DESC, got %s %v", q.OrderBy[1].Column, q.OrderBy[1].Descending)
	}
}

func TestParseOrderByWithWhere(t *testing.T) {
	q, err := Parse("SELECT name, age FROM data.csv WHERE age > 18 ORDER BY age DESC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(q.OrderBy))
	}

	if q.Where == nil {
		t.Fatalf("expected WHERE expression")
	}

	if q.OrderBy[0].Column != "age" || !q.OrderBy[0].Descending {
		t.Fatalf("expected ORDER BY age DESC, got %s %v", q.OrderBy[0].Column, q.OrderBy[0].Descending)
	}
}

func TestParseOrderByWithLimit(t *testing.T) {
	q, err := Parse("SELECT name, age FROM data.csv ORDER BY age DESC LIMIT 10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(q.OrderBy))
	}

	if q.Limit != 10 {
		t.Fatalf("expected LIMIT 10, got %d", q.Limit)
	}
}

func TestParseOrderByWithGroupBy(t *testing.T) {
	q, err := Parse("SELECT country, COUNT(*) FROM data.csv GROUP BY country ORDER BY COUNT(*) DESC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.GroupBy) != 1 || q.GroupBy[0] != "country" {
		t.Fatalf("expected GROUP BY country, got %#v", q.GroupBy)
	}

	if len(q.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(q.OrderBy))
	}

	if q.OrderBy[0].Column != "COUNT(*)" || !q.OrderBy[0].Descending {
		t.Fatalf("expected ORDER BY COUNT(*) DESC, got %s %v", q.OrderBy[0].Column, q.OrderBy[0].Descending)
	}
}

func TestParseOrderByInvalidDirection(t *testing.T) {
	_, err := Parse("SELECT name FROM data.csv ORDER BY name ASCENDING")
	if err == nil {
		t.Fatalf("expected error for invalid direction")
	}
}
