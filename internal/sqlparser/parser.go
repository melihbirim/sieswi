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
	Where      Expression
	Limit      int
}

// Expression represents a boolean expression in the WHERE clause
type Expression interface {
	isExpression()
}

// BinaryExpr represents AND/OR operations
type BinaryExpr struct {
	Left     Expression
	Operator string // "AND" or "OR"
	Right    Expression
}

func (BinaryExpr) isExpression() {}

// UnaryExpr represents NOT operation
type UnaryExpr struct {
	Operator string // "NOT"
	Expr     Expression
}

func (UnaryExpr) isExpression() {}

// Comparison represents a single column comparison
type Comparison struct {
	Column       string
	Operator     string // "=", "!=", ">", ">=", "<", "<="
	Value        string
	NumericValue float64
	IsNumeric    bool
}

func (Comparison) isExpression() {}

// Predicate is kept for backward compatibility (deprecated)
type Predicate = Comparison

var (
	queryRe = regexp.MustCompile(`(?i)^\s*select\s+(.+?)\s+from\s+((?:'[^']+'|"[^"]+"|\S+))(?:\s+where\s+(.+?))?(?:\s+limit\s+(\d+))?\s*$`)

	predicateRe = regexp.MustCompile(`(?i)^\s*([a-zA-Z0-9_]+)\s*(=|!=|>=|<=|>|<)\s*(.+?)\s*$`)
)

// isWordBoundary returns true if the character is a word boundary (whitespace or paren)
func isWordBoundary(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' || c == ')'
}

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
		expr, err := parseExpression(wherePart)
		if err != nil {
			return Query{}, err
		}
		q.Where = expr
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

// parseExpression parses OR expressions (lowest precedence)
func parseExpression(input string) (Expression, error) {
	return parseOrExpr(input)
}

// parseOrExpr handles OR operations
func parseOrExpr(input string) (Expression, error) {
	// Split on OR (case insensitive, outside parentheses)
	parts := splitOnOperator(input, "OR")
	if len(parts) == 1 {
		return parseAndExpr(parts[0])
	}

	left, err := parseAndExpr(parts[0])
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(parts); i++ {
		right, err := parseAndExpr(parts[i])
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Operator: "OR", Right: right}
	}

	return left, nil
}

// parseAndExpr handles AND operations
func parseAndExpr(input string) (Expression, error) {
	parts := splitOnOperator(input, "AND")
	if len(parts) == 1 {
		return parseNotExpr(parts[0])
	}

	left, err := parseNotExpr(parts[0])
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(parts); i++ {
		right, err := parseNotExpr(parts[i])
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Operator: "AND", Right: right}
	}

	return left, nil
}

// parseNotExpr handles NOT operations
func parseNotExpr(input string) (Expression, error) {
	input = strings.TrimSpace(input)

	// Check for NOT prefix (with space or paren after)
	inputUpper := strings.ToUpper(input)
	if strings.HasPrefix(inputUpper, "NOT ") || strings.HasPrefix(inputUpper, "NOT(") {
		var inner string
		if strings.HasPrefix(inputUpper, "NOT ") {
			inner = strings.TrimSpace(input[4:])
		} else {
			inner = strings.TrimSpace(input[3:])
		}
		expr, err := parseNotExpr(inner)
		if err != nil {
			return nil, err
		}
		return UnaryExpr{Operator: "NOT", Expr: expr}, nil
	}

	return parsePrimary(input)
}

// parsePrimary handles parentheses and comparisons
func parsePrimary(input string) (Expression, error) {
	input = strings.TrimSpace(input)

	// Handle parentheses - verify outer parens match
	if strings.HasPrefix(input, "(") && strings.HasSuffix(input, ")") {
		// Verify the opening ( at position 0 matches the final )
		depth := 0
		matchesOuter := true
		for i := 0; i < len(input)-1; i++ {
			if input[i] == '(' {
				depth++
			} else if input[i] == ')' {
				depth--
				if depth == 0 {
					// Closed before the end, not outer parens
					matchesOuter = false
					break
				}
			}
		}
		if matchesOuter && depth == 1 {
			// Valid outer parentheses, strip them
			inner := input[1 : len(input)-1]
			return parseExpression(inner)
		}
	}

	// Parse as comparison
	return parseComparison(input)
}

// parseComparison parses a single column comparison
func parseComparison(input string) (Comparison, error) {
	matches := predicateRe.FindStringSubmatch(input)
	if len(matches) == 0 {
		return Comparison{}, fmt.Errorf("unsupported WHERE clause; expected column OP value")
	}

	column := matches[1]
	operator := matches[2]
	value := strings.TrimSpace(matches[3])
	value = trimQuotes(value)

	comp := Comparison{Column: column, Operator: operator, Value: value}

	if numeric, err := strconv.ParseFloat(value, 64); err == nil {
		comp.IsNumeric = true
		comp.NumericValue = numeric
	}

	return comp, nil
}

// splitOnOperator splits input on operator (AND/OR) respecting parentheses
func splitOnOperator(input string, op string) []string {
	input = strings.TrimSpace(input)
	opUpper := strings.ToUpper(op)
	opLen := len(op)

	var parts []string
	var current strings.Builder
	parenDepth := 0

	i := 0
	for i < len(input) {
		// Track parentheses depth
		if input[i] == '(' {
			parenDepth++
			current.WriteByte(input[i])
			i++
			continue
		}
		if input[i] == ')' {
			parenDepth--
			current.WriteByte(input[i])
			i++
			continue
		}

		// Check if we're at the operator (outside parentheses)
		if parenDepth == 0 && i+opLen <= len(input) {
			substr := strings.ToUpper(input[i : i+opLen])
			// Ensure it's a word boundary (whitespace, paren, or start/end)
			beforeOk := i == 0 || isWordBoundary(input[i-1])
			afterOk := i+opLen >= len(input) || isWordBoundary(input[i+opLen])

			if substr == opUpper && beforeOk && afterOk {
				// Found operator, save current part
				parts = append(parts, current.String())
				current.Reset()
				i += opLen
				// Skip trailing spaces
				for i < len(input) && input[i] == ' ' {
					i++
				}
				continue
			}
		}

		current.WriteByte(input[i])
		i++
	}

	// Add final part
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	if len(parts) == 0 {
		return []string{input}
	}

	return parts
}

// parsePredicate is kept for backward compatibility (deprecated)
func parsePredicate(input string) (Predicate, error) {
	return parseComparison(input)
}

func trimQuotes(input string) string {
	if len(input) >= 2 {
		if (input[0] == '\'' && input[len(input)-1] == '\'') || (input[0] == '"' && input[len(input)-1] == '"') {
			return input[1 : len(input)-1]
		}
	}
	return input
}

// Evaluate evaluates an expression tree against a row (map of column -> value)
func Evaluate(expr Expression, row map[string]string) bool {
	switch e := expr.(type) {
	case BinaryExpr:
		if e.Operator == "AND" {
			// Short-circuit: if left is false, return false without evaluating right
			if !Evaluate(e.Left, row) {
				return false
			}
			return Evaluate(e.Right, row)
		} else if e.Operator == "OR" {
			// Short-circuit: if left is true, return true without evaluating right
			if Evaluate(e.Left, row) {
				return true
			}
			return Evaluate(e.Right, row)
		}
		return false

	case UnaryExpr:
		if e.Operator == "NOT" {
			return !Evaluate(e.Expr, row)
		}
		return false

	case Comparison:
		value, exists := row[e.Column]
		if !exists {
			return false
		}
		return e.Compare(value)

	default:
		return false
	}
}

// EvaluateNormalized evaluates expression with normalized (lowercase) column names
func EvaluateNormalized(expr Expression, row map[string]string) bool {
	switch e := expr.(type) {
	case *BinaryExpr:
		if e.Operator == "AND" {
			if !EvaluateNormalized(e.Left, row) {
				return false
			}
			return EvaluateNormalized(e.Right, row)
		} else if e.Operator == "OR" {
			if EvaluateNormalized(e.Left, row) {
				return true
			}
			return EvaluateNormalized(e.Right, row)
		}
		return false

	case BinaryExpr:
		if e.Operator == "AND" {
			if !EvaluateNormalized(e.Left, row) {
				return false
			}
			return EvaluateNormalized(e.Right, row)
		} else if e.Operator == "OR" {
			if EvaluateNormalized(e.Left, row) {
				return true
			}
			return EvaluateNormalized(e.Right, row)
		}
		return false

	case *UnaryExpr:
		if e.Operator == "NOT" {
			return !EvaluateNormalized(e.Expr, row)
		}
		return false

	case UnaryExpr:
		if e.Operator == "NOT" {
			return !EvaluateNormalized(e.Expr, row)
		}
		return false

	case Comparison:
		// Normalize column name for lookup
		normalized := strings.ToLower(strings.TrimSpace(e.Column))
		value, exists := row[normalized]
		if !exists {
			return false
		}
		return e.Compare(value)

	default:
		return false
	}
}

// Compare evaluates a comparison against the provided value.
func (c Comparison) Compare(candidate string) bool {
	if c.IsNumeric {
		candidateNum, err := strconv.ParseFloat(candidate, 64)
		if err != nil {
			return false
		}
		switch c.Operator {
		case "=":
			return candidateNum == c.NumericValue
		case "!=":
			return candidateNum != c.NumericValue
		case ">":
			return candidateNum > c.NumericValue
		case ">=":
			return candidateNum >= c.NumericValue
		case "<":
			return candidateNum < c.NumericValue
		case "<=":
			return candidateNum <= c.NumericValue
		}
		return false
	}

	cmp := strings.Compare(candidate, c.Value)
	switch c.Operator {
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
