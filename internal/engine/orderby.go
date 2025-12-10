package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

// rowWithKey stores a CSV row with its sort key values
type rowWithKey struct {
	row      []string
	sortKeys []sortKey
}

// columnType represents the detected type of a column
type columnType int

const (
	columnUnknown columnType = iota
	columnNumeric
	columnString
)

// compileWhereClause creates a fast evaluator that works directly on row slices
// This avoids allocating a map[string]string for every row
func compileWhereClause(expr sqlparser.Expression, header []string, normalizedHeaders map[string]int) func([]string) bool {
	return func(row []string) bool {
		return evaluateExpressionDirect(expr, row, normalizedHeaders)
	}
}

// evaluateExpressionDirect evaluates expressions directly on row slices without map allocation
func evaluateExpressionDirect(expr sqlparser.Expression, row []string, colIndices map[string]int) bool {
	switch e := expr.(type) {
	case *sqlparser.BinaryExpr:
		switch e.Operator {
		case "AND":
			if !evaluateExpressionDirect(e.Left, row, colIndices) {
				return false
			}
			return evaluateExpressionDirect(e.Right, row, colIndices)
		case "OR":
			if evaluateExpressionDirect(e.Left, row, colIndices) {
				return true
			}
			return evaluateExpressionDirect(e.Right, row, colIndices)
		}
		return false

	case sqlparser.BinaryExpr:
		switch e.Operator {
		case "AND":
			if !evaluateExpressionDirect(e.Left, row, colIndices) {
				return false
			}
			return evaluateExpressionDirect(e.Right, row, colIndices)
		case "OR":
			if evaluateExpressionDirect(e.Left, row, colIndices) {
				return true
			}
			return evaluateExpressionDirect(e.Right, row, colIndices)
		}
		return false

	case *sqlparser.UnaryExpr:
		if e.Operator == "NOT" {
			return !evaluateExpressionDirect(e.Expr, row, colIndices)
		}
		return false

	case sqlparser.UnaryExpr:
		if e.Operator == "NOT" {
			return !evaluateExpressionDirect(e.Expr, row, colIndices)
		}
		return false

	case *sqlparser.Comparison:
		colIdx, ok := colIndices[strings.ToLower(e.Column)]
		if !ok || colIdx >= len(row) {
			return false
		}
		value := row[colIdx]

		switch e.Operator {
		case "=":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num == e.NumericValue
			}
			return value == e.Value
		case "!=", "<>":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err != nil || num != e.NumericValue
			}
			return value != e.Value
		case ">":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num > e.NumericValue
			}
			return value > e.Value
		case ">=":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num >= e.NumericValue
			}
			return value >= e.Value
		case "<":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num < e.NumericValue
			}
			return value < e.Value
		case "<=":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num <= e.NumericValue
			}
			return value <= e.Value
		}
		return false

	case sqlparser.Comparison:
		colIdx, ok := colIndices[strings.ToLower(e.Column)]
		if !ok || colIdx >= len(row) {
			return false
		}
		value := row[colIdx]

		switch e.Operator {
		case "=":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num == e.NumericValue
			}
			return value == e.Value
		case "!=", "<>":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err != nil || num != e.NumericValue
			}
			return value != e.Value
		case ">":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num > e.NumericValue
			}
			return value > e.Value
		case ">=":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num >= e.NumericValue
			}
			return value >= e.Value
		case "<":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num < e.NumericValue
			}
			return value < e.Value
		case "<=":
			if e.IsNumeric {
				num, err := strconv.ParseFloat(value, 64)
				return err == nil && num <= e.NumericValue
			}
			return value <= e.Value
		}
		return false

	default:
		return false
	}
}

// sortKey represents a single sort value (string or numeric)
type sortKey struct {
	strValue      string
	strValueLower string // Pre-lowercased for strings only (not for numeric)
	numValue      float64
	isNumeric     bool
}

// executeOrderBy handles ORDER BY queries by loading all rows into memory and sorting
func executeOrderBy(query sqlparser.Query, reader *csv.Reader, header []string, out io.Writer) error {
	// Normalize headers for case-insensitive matching
	normalizedHeaders := make(map[string]int)
	for i, h := range header {
		normalizedHeaders[strings.ToLower(strings.TrimSpace(h))] = i
	}

	// Resolve column indices for ORDER BY
	orderByIndices := make([]int, len(query.OrderBy))
	for i, orderCol := range query.OrderBy {
		idx, ok := normalizedHeaders[strings.ToLower(orderCol.Column)]
		if !ok {
			return fmt.Errorf("ORDER BY column not found: %s", orderCol.Column)
		}
		orderByIndices[i] = idx
	}

	// Determine which columns to output
	selectedIdxs, outputHeader, err := resolveProjection(query, header, normalizedHeaders)
	if err != nil {
		return err
	}

	// Compile WHERE clause to avoid per-row map allocations
	var whereEvaluator func([]string) bool
	if query.Where != nil {
		if err := validateWhereColumns(query.Where, normalizedHeaders); err != nil {
			return err
		}
		// Pre-compile predicate with column indices
		whereEvaluator = compileWhereClause(query.Where, header, normalizedHeaders)
	}

	// Quick Win #3: Use heap-based top-K when LIMIT is small (< 1000)
	if query.Limit > 0 && query.Limit < 1000 {
		return executeOrderByTopK(query, reader, header, orderByIndices, selectedIdxs, outputHeader, normalizedHeaders, out)
	}

	// 3-state type detection: unknown -> numeric/string (stop ParseFloat once decided)
	columnTypes := make([]columnType, len(orderByIndices))
	for i := range columnTypes {
		columnTypes[i] = columnUnknown
	}
	sampleCount := 0
	const sampleSize = 100

	// Load all rows into memory
	var rows []rowWithKey
	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1

	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read row %d: %w", rowCount+1, err)
		}
		rowCount++

		// Apply compiled WHERE filter (no map allocation)
		if whereEvaluator != nil && !whereEvaluator(record) {
			continue
		}

		// Copy the row since reader reuses the slice
		rowCopy := make([]string, len(record))
		copy(rowCopy, record)

		// 3-state type detection: stop testing once column type is determined
		if sampleCount < sampleSize {
			for i, idx := range orderByIndices {
				if columnTypes[i] != columnUnknown {
					continue // Already determined
				}
				if idx < len(rowCopy) {
					if _, err := strconv.ParseFloat(rowCopy[idx], 64); err == nil {
						if sampleCount >= 5 { // Need a few samples to be confident
							columnTypes[i] = columnNumeric
						}
					} else {
						// Found non-numeric value - column is string
						columnTypes[i] = columnString
					}
				}
			}
			sampleCount++
		}

		// Extract sort keys using determined types
		sortKeys := make([]sortKey, len(orderByIndices))
		for i, idx := range orderByIndices {
			if idx >= len(rowCopy) {
				sortKeys[i] = sortKey{strValue: "", strValueLower: "", isNumeric: false}
				continue
			}

			value := rowCopy[idx]

			// Use 3-state type detection
			if columnTypes[i] == columnString {
				// Known string column - skip ParseFloat entirely
				sortKeys[i] = sortKey{
					strValue:      value,
					strValueLower: strings.ToLower(value), // Only lowercase for strings
					isNumeric:     false,
				}
			} else if columnTypes[i] == columnNumeric {
				// Known numeric column - skip ToLower entirely
				if numVal, err := strconv.ParseFloat(value, 64); err == nil {
					sortKeys[i] = sortKey{
						strValue:  value,
						numValue:  numVal,
						isNumeric: true,
						// No strValueLower for numeric columns
					}
				} else {
					// Type changed to string
					sortKeys[i] = sortKey{
						strValue:      value,
						strValueLower: strings.ToLower(value),
						isNumeric:     false,
					}
				}
			} else {
				// Unknown - try to parse
				if numVal, err := strconv.ParseFloat(value, 64); err == nil {
					sortKeys[i] = sortKey{
						strValue:  value,
						numValue:  numVal,
						isNumeric: true,
					}
				} else {
					sortKeys[i] = sortKey{
						strValue:      value,
						strValueLower: strings.ToLower(value),
						isNumeric:     false,
					}
				}
			}
		}

		rows = append(rows, rowWithKey{
			row:      rowCopy,
			sortKeys: sortKeys,
		})
	}

	// Sort the rows
	sort.Slice(rows, func(i, j int) bool {
		for k, orderCol := range query.OrderBy {
			keyI := rows[i].sortKeys[k]
			keyJ := rows[j].sortKeys[k]

			var cmp int
			if keyI.isNumeric && keyJ.isNumeric {
				// Numeric comparison
				if keyI.numValue < keyJ.numValue {
					cmp = -1
				} else if keyI.numValue > keyJ.numValue {
					cmp = 1
				} else {
					cmp = 0
				}
			} else {
				// String comparison using pre-lowercased values (Quick Win #1)
				cmp = strings.Compare(keyI.strValueLower, keyJ.strValueLower)
			}

			if cmp != 0 {
				if orderCol.Descending {
					return cmp > 0
				}
				return cmp < 0
			}
			// Equal, continue to next sort column
		}
		return false // All sort keys equal
	})

	// Write header
	writer := csv.NewWriter(out)
	if err := writer.Write(outputHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write sorted rows with projection
	outputCount := 0
	for _, rowWithKey := range rows {
		if query.Limit >= 0 && outputCount >= query.Limit {
			break
		}

		outputRow := make([]string, len(selectedIdxs))
		for i, idx := range selectedIdxs {
			if idx < len(rowWithKey.row) {
				outputRow[i] = rowWithKey.row[idx]
			}
		}

		if err := writer.Write(outputRow); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
		outputCount++
	}

	writer.Flush()
	return writer.Error()
}

// executeOrderByFromFile handles ORDER BY queries by opening the file
func executeOrderByFromFile(query sqlparser.Query, out io.Writer) error {
	file, err := os.Open(query.FilePath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer file.Close()

	buffered := bufio.NewReaderSize(file, ioBufferSize)
	reader := csv.NewReader(buffered)
	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Copy header because ReuseRecord=true will overwrite the slice
	headerCopy := make([]string, len(header))
	copy(headerCopy, header)

	return executeOrderBy(query, reader, headerCopy, out)
}
