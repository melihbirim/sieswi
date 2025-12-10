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

// sortKey represents a single sort value (string or numeric)
type sortKey struct {
	strValue      string
	strValueLower string // Pre-lowercased for fast case-insensitive comparison
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

	// Validate WHERE clause columns if present
	if query.Where != nil {
		if err := validateWhereColumns(query.Where, normalizedHeaders); err != nil {
			return err
		}
	}

	// Quick Win #3: Use heap-based top-K when LIMIT is small (< 1000)
	if query.Limit > 0 && query.Limit < 1000 {
		return executeOrderByTopK(query, reader, header, orderByIndices, selectedIdxs, outputHeader, normalizedHeaders, out)
	}

	// Quick Win #2: Type detection state for sampling
	columnTypes := make([]bool, len(orderByIndices))
	typesSampled := false
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

		// Apply WHERE filter if present
		if query.Where != nil {
			rowMap := make(map[string]string)
			for idx, val := range record {
				if idx < len(header) {
					rowMap[strings.ToLower(header[idx])] = val
				}
			}
			if !sqlparser.EvaluateNormalized(query.Where, rowMap) {
				continue
			}
		}

		// Copy the row since reader reuses the slice
		rowCopy := make([]string, len(record))
		copy(rowCopy, record)

		// Sample first N rows to detect column types (Quick Win #2)
		if !typesSampled && sampleCount < sampleSize {
			for i, idx := range orderByIndices {
				if idx < len(rowCopy) {
					if _, err := strconv.ParseFloat(rowCopy[idx], 64); err == nil {
						columnTypes[i] = true
					}
				}
			}
			sampleCount++
			if sampleCount >= sampleSize {
				typesSampled = true
			}
		}

		// Extract sort keys
		sortKeys := make([]sortKey, len(orderByIndices))
		for i, idx := range orderByIndices {
			if idx >= len(rowCopy) {
				sortKeys[i] = sortKey{strValue: "", strValueLower: "", isNumeric: false}
				continue
			}

			value := rowCopy[idx]

			// Use type detection to skip ParseFloat for known string columns
			if typesSampled && !columnTypes[i] {
				// Known string column - skip ParseFloat (Quick Win #2)
				sortKeys[i] = sortKey{
					strValue:      value,
					strValueLower: strings.ToLower(value), // Quick Win #1: pre-lowercase
					isNumeric:     false,
				}
			} else {
				// Try to parse as number
				if numVal, err := strconv.ParseFloat(value, 64); err == nil {
					sortKeys[i] = sortKey{
						strValue:      value,
						strValueLower: strings.ToLower(value),
						numValue:      numVal,
						isNumeric:     true,
					}
				} else {
					sortKeys[i] = sortKey{
						strValue:      value,
						strValueLower: strings.ToLower(value),
						isNumeric:     false,
					}
					if !typesSampled {
						columnTypes[i] = false
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
