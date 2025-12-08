package sqlparser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Query captures the minimal information required to execute a CSV query.
type Query struct {
	Columns    []string
	AllColumns bool
	FilePath   string
	Predicate  *Predicate
	Limit      int
}

// Predicate describes a single WHERE clause comparison.
type Predicate struct {
	Column       string
	Operator     string
	Value        string
	NumericValue float64
	IsNumeric    bool
}

var (
	queryRe = regexp.MustCompile(`(?i)^\s*select\s+(.+?)\s+from\s+((?:'[^']+'|"[^"]+"|\S+))(?:\s+where\s+(.+?))?(?:\s+limit\s+(\d+))?\s*$`)

	predicateRe = regexp.MustCompile(`(?i)^\s*([a-zA-Z0-9_]+)\s*(=|!=|>=|<=|>|<)\s*(.+?)\s*$`)
)

// Parse turns a limited SQL string into a Query structure.
func Parse(input string) (Query, error) {
	matches := queryRe.FindStringSubmatch(input)
	if len(matches) == 0 {
		return Query{}, fmt.Errorf("unsupported query; expected SELECT ... FROM file [WHERE ...] [LIMIT ...]")
	}

	columnsPart := strings.TrimSpace(matches[1])
	filePart := trimQuotes(strings.TrimSpace(matches[2]))
	wherePart := strings.TrimSpace(matches[3])
	limitPart := strings.TrimSpace(matches[4])

	q := Query{FilePath: filePart, Limit: -1}

	if q.FilePath == "" {
		return Query{}, fmt.Errorf("missing file path in FROM clause")
	}

	if columnsPart == "*" {
		q.AllColumns = true
	} else {
		cols := strings.Split(columnsPart, ",")
		for _, col := range cols {
			cleaned := strings.TrimSpace(col)
			if cleaned == "" {
				return Query{}, fmt.Errorf("empty column name in SELECT clause")
			}
			q.Columns = append(q.Columns, cleaned)
		}
	}

	if wherePart != "" {
		pred, err := parsePredicate(wherePart)
		if err != nil {
			return Query{}, err
		}
		q.Predicate = &pred
	}

	if limitPart != "" {
		limit, err := strconv.Atoi(limitPart)
		if err != nil || limit < 0 {
			return Query{}, fmt.Errorf("invalid LIMIT value: %s", limitPart)
		}
		q.Limit = limit
	}

	return q, nil
}

func parsePredicate(input string) (Predicate, error) {
	matches := predicateRe.FindStringSubmatch(input)
	if len(matches) == 0 {
		return Predicate{}, fmt.Errorf("unsupported WHERE clause; expected column OP value")
	}

	column := matches[1]
	operator := matches[2]
	value := strings.TrimSpace(matches[3])
	value = trimQuotes(value)

	pred := Predicate{Column: column, Operator: operator, Value: value}

	if numeric, err := strconv.ParseFloat(value, 64); err == nil {
		pred.IsNumeric = true
		pred.NumericValue = numeric
	}

	return pred, nil
}

func trimQuotes(input string) string {
	if len(input) >= 2 {
		if (input[0] == '\'' && input[len(input)-1] == '\'') || (input[0] == '"' && input[len(input)-1] == '"') {
			return input[1 : len(input)-1]
		}
	}
	return input
}

// Compare evaluates the predicate against the provided value.
func (p Predicate) Compare(candidate string) bool {
	if p.IsNumeric {
		candidateNum, err := strconv.ParseFloat(candidate, 64)
		if err != nil {
			return false
		}
		switch p.Operator {
		case "=":
			return candidateNum == p.NumericValue
		case "!=":
			return candidateNum != p.NumericValue
		case ">":
			return candidateNum > p.NumericValue
		case ">=":
			return candidateNum >= p.NumericValue
		case "<":
			return candidateNum < p.NumericValue
		case "<=":
			return candidateNum <= p.NumericValue
		}
		return false
	}

	cmp := strings.Compare(candidate, p.Value)
	switch p.Operator {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	default:
		return false
	}
}
