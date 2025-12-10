package engine

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

// rowHeap implements heap.Interface for top-K selection
type rowHeap struct {
	rows    []rowWithKey
	orderBy []sqlparser.OrderByColumn
}

func (h rowHeap) Len() int { return len(h.rows) }

func (h rowHeap) Less(i, j int) bool {
	// Max-heap: we want to pop the worst elements to keep best K
	for k, orderCol := range h.orderBy {
		keyI := h.rows[i].sortKeys[k]
		keyJ := h.rows[j].sortKeys[k]

		var cmp int
		if keyI.isNumeric && keyJ.isNumeric {
			if keyI.numValue < keyJ.numValue {
				cmp = -1
			} else if keyI.numValue > keyJ.numValue {
				cmp = 1
			} else {
				cmp = 0
			}
		} else {
			cmp = strings.Compare(keyI.strValueLower, keyJ.strValueLower)
		}

		if cmp != 0 {
			// For ASC order: max-heap (pop largest) - reverse comparison
			// For DESC order: min-heap (pop smallest) - normal comparison
			if orderCol.Descending {
				return cmp < 0
			}
			return cmp > 0
		}
	}
	return false
}

func (h rowHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *rowHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(rowWithKey))
}

func (h *rowHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[0 : n-1]
	return x
}

// executeOrderByTopK uses a heap for efficient top-K selection when LIMIT is small
func executeOrderByTopK(query sqlparser.Query, reader *csv.Reader, header []string, orderByIndices []int,
	selectedIdxs []int, outputHeader []string, normalizedHeaders map[string]int, out io.Writer) error {

	// Compile WHERE clause once
	var whereEvaluator func([]string) bool
	if query.Where != nil {
		whereEvaluator = compileWhereClause(query.Where, header, normalizedHeaders)
	}

	// Initialize heap
	h := &rowHeap{
		rows:    make([]rowWithKey, 0, query.Limit),
		orderBy: query.OrderBy,
	}
	heap.Init(h)

	// 3-state type detection
	columnTypes := make([]columnType, len(orderByIndices))
	for i := range columnTypes {
		columnTypes[i] = columnUnknown
	}
	sampleCount := 0
	const sampleSize = 100

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

		// Apply compiled WHERE filter
		if whereEvaluator != nil && !whereEvaluator(record) {
			continue
		}

		// Copy the row
		rowCopy := make([]string, len(record))
		copy(rowCopy, record)

		// 3-state type detection with early termination
		if sampleCount < sampleSize {
			for i, idx := range orderByIndices {
				if columnTypes[i] != columnUnknown {
					continue
				}
				if idx < len(rowCopy) {
					if _, err := strconv.ParseFloat(rowCopy[idx], 64); err == nil {
						if sampleCount >= 5 {
							columnTypes[i] = columnNumeric
						}
					} else {
						columnTypes[i] = columnString
					}
				}
			}
			sampleCount++
		}

		// Extract sort keys
		sortKeys := make([]sortKey, len(orderByIndices))
		for i, idx := range orderByIndices {
			if idx >= len(rowCopy) {
				sortKeys[i] = sortKey{strValue: "", strValueLower: "", isNumeric: false}
				continue
			}

			value := rowCopy[idx]

			// Use 3-state type detection
			if columnTypes[i] == columnString {
				// Known string - skip ParseFloat
				sortKeys[i] = sortKey{
					strValue:      value,
					strValueLower: strings.ToLower(value),
					isNumeric:     false,
				}
			} else if columnTypes[i] == columnNumeric {
				// Known numeric - skip ToLower
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
			} else {
				// Unknown - try parse
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

		newRow := rowWithKey{
			row:      rowCopy,
			sortKeys: sortKeys,
		}

		// Maintain heap of size K
		if h.Len() < query.Limit {
			heap.Push(h, newRow)
		} else {
			// Check if new row should replace worst in heap
			if isRowBetter(newRow, h.rows[0], query.OrderBy) {
				heap.Pop(h)
				heap.Push(h, newRow)
			}
		}
	}

	// Sort final results
	results := h.rows
	sort.Slice(results, func(i, j int) bool {
		for k, orderCol := range query.OrderBy {
			keyI := results[i].sortKeys[k]
			keyJ := results[j].sortKeys[k]

			var cmp int
			if keyI.isNumeric && keyJ.isNumeric {
				if keyI.numValue < keyJ.numValue {
					cmp = -1
				} else if keyI.numValue > keyJ.numValue {
					cmp = 1
				} else {
					cmp = 0
				}
			} else {
				cmp = strings.Compare(keyI.strValueLower, keyJ.strValueLower)
			}

			if cmp != 0 {
				if orderCol.Descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	// Write output
	writer := csv.NewWriter(out)
	if err := writer.Write(outputHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, rowWithKey := range results {
		outputRow := make([]string, len(selectedIdxs))
		for i, idx := range selectedIdxs {
			if idx < len(rowWithKey.row) {
				outputRow[i] = rowWithKey.row[idx]
			}
		}
		if err := writer.Write(outputRow); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}

	writer.Flush()
	return writer.Error()
}

// isRowBetter returns true if rowA is better than rowB according to ORDER BY
func isRowBetter(rowA, rowB rowWithKey, orderBy []sqlparser.OrderByColumn) bool {
	for k, orderCol := range orderBy {
		keyA := rowA.sortKeys[k]
		keyB := rowB.sortKeys[k]

		var cmp int
		if keyA.isNumeric && keyB.isNumeric {
			if keyA.numValue < keyB.numValue {
				cmp = -1
			} else if keyA.numValue > keyB.numValue {
				cmp = 1
			} else {
				cmp = 0
			}
		} else {
			cmp = strings.Compare(keyA.strValueLower, keyB.strValueLower)
		}

		if cmp != 0 {
			if orderCol.Descending {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	return false
}
