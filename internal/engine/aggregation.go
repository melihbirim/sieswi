package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

// AggregateFunc represents an aggregate function in SELECT
type AggregateFunc struct {
	FuncName string // COUNT, SUM, AVG, MIN, MAX
	Column   string // Column name, or "*" for COUNT(*)
	Alias    string // Original expression (e.g., "COUNT(*)")
}

// Aggregator accumulates values for aggregation
type Aggregator struct {
	Count  int64
	Sum    float64
	Min    float64
	Max    float64
	HasMin bool
	HasMax bool
}

var aggregateFuncRe = regexp.MustCompile(`(?i)^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*([*a-zA-Z0-9_]+)\s*\)$`)

// parseAggregateFunc checks if a column expression is an aggregate function
func parseAggregateFunc(expr string) (*AggregateFunc, bool) {
	expr = strings.TrimSpace(expr)
	matches := aggregateFuncRe.FindStringSubmatch(expr)
	if len(matches) == 0 {
		return nil, false
	}

	return &AggregateFunc{
		FuncName: strings.ToUpper(matches[1]),
		Column:   strings.TrimSpace(matches[2]),
		Alias:    expr,
	}, true
}

// executeGroupBy handles GROUP BY queries with aggregations
func executeGroupBy(query sqlparser.Query, reader *csv.Reader, header []string, out io.Writer) error {
	// Parse SELECT columns to identify group columns and aggregate functions
	var groupCols []string
	var aggregates []*AggregateFunc
	
	if query.AllColumns {
		return fmt.Errorf("SELECT * not supported with GROUP BY, please specify columns")
	}

	for _, col := range query.Columns {
		if agg, isAgg := parseAggregateFunc(col); isAgg {
			aggregates = append(aggregates, agg)
		} else {
			groupCols = append(groupCols, strings.TrimSpace(col))
		}
	}

	// Validate that all non-aggregate columns are in GROUP BY
	if len(groupCols) != len(query.GroupBy) {
		return fmt.Errorf("all non-aggregate columns in SELECT must appear in GROUP BY")
	}

	// Normalize headers for case-insensitive matching
	normalizedHeaders := make(map[string]int)
	for i, h := range header {
		normalizedHeaders[strings.ToLower(strings.TrimSpace(h))] = i
	}

	// Find indices for GROUP BY columns
	groupByIndices := make([]int, len(query.GroupBy))
	for i, col := range query.GroupBy {
		idx, ok := normalizedHeaders[strings.ToLower(col)]
		if !ok {
			return fmt.Errorf("GROUP BY column not found: %s", col)
		}
		groupByIndices[i] = idx
	}

	// Find indices for aggregate columns
	aggregateIndices := make([]int, len(aggregates))
	for i, agg := range aggregates {
		if agg.FuncName == "COUNT" && agg.Column == "*" {
			aggregateIndices[i] = -1 // Special case for COUNT(*)
			continue
		}
		idx, ok := normalizedHeaders[strings.ToLower(agg.Column)]
		if !ok {
			return fmt.Errorf("aggregate column not found: %s", agg.Column)
		}
		aggregateIndices[i] = idx
	}

	// Accumulate groups in memory
	groups := make(map[string]*Aggregator)
	groupKeys := []string{} // Preserve insertion order

	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1

	rowCount := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read row %d: %w", rowCount+1, err)
		}
		rowCount++

		// Apply WHERE filter if present
		if query.Where != nil {
			rowMap := make(map[string]string)
			for idx, val := range row {
				if idx < len(header) {
					rowMap[strings.ToLower(header[idx])] = val
				}
			}
			if !sqlparser.EvaluateNormalized(query.Where, rowMap) {
				continue
			}
		}

		// Build group key from GROUP BY columns
		keyParts := make([]string, len(groupByIndices))
		for i, idx := range groupByIndices {
			if idx >= len(row) {
				keyParts[i] = ""
			} else {
				keyParts[i] = row[idx]
			}
		}
		groupKey := strings.Join(keyParts, "\x00") // Use null byte as separator

		// Get or create aggregator for this group
		agg, exists := groups[groupKey]
		if !exists {
			agg = &Aggregator{}
			groups[groupKey] = agg
			groupKeys = append(groupKeys, groupKey)
		}

		// Update aggregates
		for i, aggFunc := range aggregates {
			switch aggFunc.FuncName {
			case "COUNT":
				agg.Count++
			case "SUM", "AVG":
				if aggregateIndices[i] >= 0 && aggregateIndices[i] < len(row) {
					if val, err := strconv.ParseFloat(row[aggregateIndices[i]], 64); err == nil {
						agg.Sum += val
						agg.Count++
					}
				}
			case "MIN":
				if aggregateIndices[i] >= 0 && aggregateIndices[i] < len(row) {
					if val, err := strconv.ParseFloat(row[aggregateIndices[i]], 64); err == nil {
						if !agg.HasMin || val < agg.Min {
							agg.Min = val
							agg.HasMin = true
						}
						agg.Count++
					}
				}
			case "MAX":
				if aggregateIndices[i] >= 0 && aggregateIndices[i] < len(row) {
					if val, err := strconv.ParseFloat(row[aggregateIndices[i]], 64); err == nil {
						if !agg.HasMax || val > agg.Max {
							agg.Max = val
							agg.HasMax = true
						}
						agg.Count++
					}
				}
			}
		}
	}

	// Write output header
	writer := csv.NewWriter(out)
	outputHeader := make([]string, 0, len(groupCols)+len(aggregates))
	outputHeader = append(outputHeader, query.GroupBy...)
	for _, agg := range aggregates {
		outputHeader = append(outputHeader, agg.Alias)
	}
	if err := writer.Write(outputHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write aggregated results (in order of first appearance)
	outputCount := 0
	for _, groupKey := range groupKeys {
		if query.Limit >= 0 && outputCount >= query.Limit {
			break
		}

		agg := groups[groupKey]
		keyParts := strings.Split(groupKey, "\x00")

		outputRow := make([]string, 0, len(groupCols)+len(aggregates))
		outputRow = append(outputRow, keyParts...)

		for _, aggFunc := range aggregates {
			var value string
			switch aggFunc.FuncName {
			case "COUNT":
				value = fmt.Sprintf("%d", agg.Count)
			case "SUM":
				value = fmt.Sprintf("%.2f", agg.Sum)
			case "AVG":
				if agg.Count > 0 {
					value = fmt.Sprintf("%.2f", agg.Sum/float64(agg.Count))
				} else {
					value = "0"
				}
			case "MIN":
				if agg.HasMin {
					value = fmt.Sprintf("%.2f", agg.Min)
				} else {
					value = ""
				}
			case "MAX":
				if agg.HasMax {
					value = fmt.Sprintf("%.2f", agg.Max)
				} else {
					value = ""
				}
			}
			outputRow = append(outputRow, value)
		}

		if err := writer.Write(outputRow); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
		outputCount++
	}

	writer.Flush()
	return writer.Error()
}

// executeGroupByFromFile handles GROUP BY queries by opening the file and calling executeGroupBy
func executeGroupByFromFile(query sqlparser.Query, out io.Writer) error {
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

	return executeGroupBy(query, reader, headerCopy, out)
}
