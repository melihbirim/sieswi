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
	"sync"

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
	RowCount int64           // COUNT(*) - number of rows in group
	Sums     map[int]float64 // SUM/AVG per aggregate index
	Counts   map[int]int64   // COUNT per aggregate index (for AVG)
	Mins     map[int]float64 // MIN per aggregate index
	Maxs     map[int]float64 // MAX per aggregate index
	HasMin   map[int]bool    // Track if MIN has been set
	HasMax   map[int]bool    // Track if MAX has been set
}

func newAggregator() *Aggregator {
	return &Aggregator{
		Sums:   make(map[int]float64),
		Counts: make(map[int]int64),
		Mins:   make(map[int]float64),
		Maxs:   make(map[int]float64),
		HasMin: make(map[int]bool),
		HasMax: make(map[int]bool),
	}
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
			agg = newAggregator()
			groups[groupKey] = agg
			groupKeys = append(groupKeys, groupKey)
		}

		// Increment row count for this group (for COUNT(*))
		agg.RowCount++

		// Update aggregates
		for i, aggFunc := range aggregates {
			switch aggFunc.FuncName {
			case "COUNT":
				// COUNT(*) already handled by RowCount
				// COUNT(column) would be the same in our case
			case "SUM", "AVG":
				if aggregateIndices[i] >= 0 && aggregateIndices[i] < len(row) {
					if val, err := strconv.ParseFloat(row[aggregateIndices[i]], 64); err == nil {
						agg.Sums[i] += val
						agg.Counts[i]++
					}
				}
			case "MIN":
				if aggregateIndices[i] >= 0 && aggregateIndices[i] < len(row) {
					if val, err := strconv.ParseFloat(row[aggregateIndices[i]], 64); err == nil {
						if !agg.HasMin[i] || val < agg.Mins[i] {
							agg.Mins[i] = val
							agg.HasMin[i] = true
						}
					}
				}
			case "MAX":
				if aggregateIndices[i] >= 0 && aggregateIndices[i] < len(row) {
					if val, err := strconv.ParseFloat(row[aggregateIndices[i]], 64); err == nil {
						if !agg.HasMax[i] || val > agg.Maxs[i] {
							agg.Maxs[i] = val
							agg.HasMax[i] = true
						}
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

		for i, aggFunc := range aggregates {
			var value string
			switch aggFunc.FuncName {
			case "COUNT":
				value = fmt.Sprintf("%d", agg.RowCount)
			case "SUM":
				value = fmt.Sprintf("%.2f", agg.Sums[i])
			case "AVG":
				if agg.Counts[i] > 0 {
					value = fmt.Sprintf("%.2f", agg.Sums[i]/float64(agg.Counts[i]))
				} else {
					value = "0"
				}
			case "MIN":
				if agg.HasMin[i] {
					value = fmt.Sprintf("%.2f", agg.Mins[i])
				} else {
					value = ""
				}
			case "MAX":
				if agg.HasMax[i] {
					value = fmt.Sprintf("%.2f", agg.Maxs[i])
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
