package sqlparser

import (
	"testing"
)

// TestParenthesesMatching tests that parsePrimary correctly identifies outer parentheses
func TestParenthesesMatching(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		shouldParse bool
		description string
	}{
		{
			name:        "simple_outer_parens",
			input:       "SELECT * FROM data.csv WHERE (col1 = 'A')",
			shouldParse: true,
			description: "Simple comparison with outer parentheses",
		},
		{
			name:        "or_with_separate_parens",
			input:       "SELECT * FROM data.csv WHERE (col1 = 'A') OR (col2 = 'B')",
			shouldParse: true,
			description: "OR with two separate parenthesized comparisons - should NOT strip parens",
		},
		{
			name:        "nested_parens",
			input:       "SELECT * FROM data.csv WHERE ((col1 = 'A' OR col2 = 'B') AND col3 = 'C')",
			shouldParse: true,
			description: "Properly nested parentheses",
		},
		{
			name:        "and_no_parens",
			input:       "SELECT * FROM data.csv WHERE col1 = 'A' AND col2 = 'B'",
			shouldParse: true,
			description: "AND without any parentheses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if tt.shouldParse {
				if err != nil {
					t.Errorf("%s: unexpected error: %v", tt.description, err)
				}
				if q.Where == nil {
					t.Errorf("%s: expected WHERE clause", tt.description)
				}
			} else {
				if err == nil {
					t.Errorf("%s: expected error but got none", tt.description)
				}
			}
		})
	}
}

// TestWhitespaceInOperators tests that operators work with various whitespace
func TestWhitespaceInOperators(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		shouldParse bool
		description string
	}{
		{
			name:        "and_with_tabs",
			input:       "SELECT * FROM data.csv WHERE col1 = 'A'\tAND\tcol2 = 'B'",
			shouldParse: true,
			description: "AND operator with tabs",
		},
		{
			name:        "and_with_newline",
			input:       "SELECT * FROM data.csv WHERE col1 = 'A'\nAND col2 = 'B'",
			shouldParse: false, // Known limitation: main query regex doesn't handle newlines
			description: "AND operator with newline (not supported - known limitation)",
		},
		{
			name:        "and_no_space_before_paren",
			input:       "SELECT * FROM data.csv WHERE col1 = 'A' AND(col2 = 'B')",
			shouldParse: true,
			description: "AND followed immediately by opening parenthesis",
		},
		{
			name:        "or_with_mixed_whitespace",
			input:       "SELECT * FROM data.csv WHERE col1 = 'A'  \t\n  OR   col2 = 'B'",
			shouldParse: false, // Known limitation: main query regex doesn't handle newlines
			description: "OR with mixed whitespace including newline (not supported)",
		},
		{
			name:        "not_with_paren",
			input:       "SELECT * FROM data.csv WHERE NOT(col1 = 'A')",
			shouldParse: true,
			description: "NOT followed immediately by opening parenthesis",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if tt.shouldParse {
				if err != nil {
					t.Errorf("%s: unexpected error: %v", tt.description, err)
				}
				if q.Where == nil {
					t.Errorf("%s: expected WHERE clause", tt.description)
				}
			} else {
				if err == nil {
					t.Errorf("%s: expected error but got none", tt.description)
				}
			}
		})
	}
}

// TestComplexExpressions tests complex boolean expressions
func TestComplexExpressions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		validate func(*testing.T, Expression)
	}{
		{
			name:  "or_with_separate_parens",
			input: "SELECT * FROM data.csv WHERE (col1 = 'A') OR (col2 = 'B')",
			validate: func(t *testing.T, expr Expression) {
				// Should be a BinaryExpr with OR
				switch e := expr.(type) {
				case BinaryExpr:
					if e.Operator != "OR" {
						t.Errorf("expected OR operator, got %s", e.Operator)
					}
				case *BinaryExpr:
					if e.Operator != "OR" {
						t.Errorf("expected OR operator, got %s", e.Operator)
					}
				default:
					t.Errorf("expected BinaryExpr, got %T", expr)
				}
			},
		},
		{
			name:  "precedence_or_and",
			input: "SELECT * FROM data.csv WHERE col1 = 'A' OR col2 = 'B' AND col3 = 'C'",
			validate: func(t *testing.T, expr Expression) {
				// Should be: col1 = 'A' OR (col2 = 'B' AND col3 = 'C')
				// Top level should be OR
				switch e := expr.(type) {
				case BinaryExpr:
					if e.Operator != "OR" {
						t.Errorf("expected top-level OR, got %s", e.Operator)
					}
					// Right side should be AND
					if right, ok := e.Right.(BinaryExpr); ok {
						if right.Operator != "AND" {
							t.Errorf("expected right side to be AND, got %s", right.Operator)
						}
					} else if right, ok := e.Right.(*BinaryExpr); ok {
						if right.Operator != "AND" {
							t.Errorf("expected right side to be AND, got %s", right.Operator)
						}
					}
				case *BinaryExpr:
					if e.Operator != "OR" {
						t.Errorf("expected top-level OR, got %s", e.Operator)
					}
				default:
					t.Errorf("expected BinaryExpr, got %T", expr)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.Where == nil {
				t.Fatal("expected WHERE clause")
			}
			tt.validate(t, q.Where)
		})
	}
}
