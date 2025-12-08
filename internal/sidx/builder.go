package sidx

import (
	"bytes"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type countingReader struct {
	r      io.Reader
	offset int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.offset += int64(n)
	return n, err
}

func (c *countingReader) Offset() int64 {
	return c.offset
}

// Builder collects statistics while scanning a CSV file
type Builder struct {
	blockSize  uint32
	currentRow uint64
	blocks     []BlockMeta

	// Current block state
	blockStartRow    uint64
	blockStartOffset uint64
	lastRowEndOffset uint64
	columnMins       []string
	columnMaxs       []string
	columnTypes      []ColumnType
	headers          []string
}

func NewBuilder(blockSize uint32) *Builder {
	return &Builder{
		blockSize: blockSize,
	}
}

// inferColumnType attempts to detect if a column is numeric
func inferColumnType(values []string) ColumnType {
	numericCount := 0
	nonEmptyCount := 0
	for _, v := range values {
		if v == "" {
			continue
		}
		nonEmptyCount++
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			numericCount++
		}
	}
	// If >80% of non-empty values are numeric, treat as numeric
	if nonEmptyCount > 0 && numericCount*5 >= nonEmptyCount*4 {
		return ColumnTypeNumeric
	}
	return ColumnTypeString
}

func (b *Builder) BuildFromFile(csvPath string) (*Index, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	fileSize := stat.Size()
	fileMtime := stat.ModTime().UnixNano()

	reader := bufio.NewReaderSize(f, 512*1024)
	offset := int64(0)

	// Read header line
	headerLine, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read header: %w", err)
	}
	headerRecord, perr := parseCSVLine(headerLine)
	if perr != nil {
		return nil, fmt.Errorf("parse header: %w", perr)
	}

	b.headers = make([]string, len(headerRecord))
	copy(b.headers, headerRecord)

	numCols := len(b.headers)
	b.columnMins = make([]string, numCols)
	b.columnMaxs = make([]string, numCols)
	b.columnTypes = make([]ColumnType, numCols)

	const sampleSize = 256
	samples := make([][]string, numCols)
	for i := range samples {
		samples[i] = make([]string, 0, sampleSize)
	}

	offset += int64(len(headerLine))
	b.blockStartRow = 0
	b.blockStartOffset = uint64(offset)
	b.lastRowEndOffset = b.blockStartOffset

	rowInBlock := uint32(0)

	for {
		rowStart := uint64(offset)
		rawLine, err := reader.ReadBytes('\n')
		if err == io.EOF && len(rawLine) == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read row %d: %w", b.currentRow, err)
		}

		trimmed := bytes.TrimRight(rawLine, "\r\n")
		if len(trimmed) == 0 {
			if err == io.EOF {
				break
			}
			offset += int64(len(rawLine))
			continue
		}

		record, perr := parseCSVLine(trimmed)
		if perr != nil {
			return nil, fmt.Errorf("parse row %d: %w", b.currentRow, perr)
		}

		if rowInBlock == 0 {
			b.blockStartOffset = rowStart
		}

		for i := 0; i < numCols && i < len(record); i++ {
			value := record[i]
			if value == "" {
				continue
			}

			if b.columnMins[i] == "" || value < b.columnMins[i] {
				b.columnMins[i] = value
			}
			if b.columnMaxs[i] == "" || value > b.columnMaxs[i] {
				b.columnMaxs[i] = value
			}
			if len(samples[i]) < sampleSize {
				samples[i] = append(samples[i], value)
			}
		}

		b.currentRow++
		rowInBlock++
		offset += int64(len(rawLine))
		b.lastRowEndOffset = uint64(offset)

		if rowInBlock >= b.blockSize {
			b.flushBlock()
			rowInBlock = 0
		}

		if err == io.EOF {
			break
		}
	}

	if b.currentRow > b.blockStartRow {
		b.flushBlock()
	}

	for i := 0; i < numCols; i++ {
		b.columnTypes[i] = inferColumnType(samples[i])
	}

	columns := make([]ColumnInfo, numCols)
	for i := range columns {
		columns[i] = ColumnInfo{
			Name: b.headers[i],
			Type: b.columnTypes[i],
		}
	}

	return &Index{
		Header: Header{
			Version:   Version,
			BlockSize: b.blockSize,
			NumBlocks: uint32(len(b.blocks)),
			FileSize:  fileSize,
			FileMtime: fileMtime,
			Columns:   columns,
		},
		Blocks: b.blocks,
	}, nil
}

func (b *Builder) flushBlock() {
	if b.currentRow == b.blockStartRow {
		return
	}

	cols := make([]ColumnStats, len(b.headers))
	for i := range b.headers {
		cols[i] = ColumnStats{
			Min: b.columnMins[i],
			Max: b.columnMaxs[i],
		}
	}

	block := BlockMeta{
		StartRow:    b.blockStartRow,
		EndRow:      b.currentRow,
		StartOffset: b.blockStartOffset,
		EndOffset:   b.lastRowEndOffset,
		Columns:     cols,
	}
	b.blocks = append(b.blocks, block)

	b.blockStartRow = b.currentRow
	b.blockStartOffset = b.lastRowEndOffset
	for i := range b.columnMins {
		b.columnMins[i] = ""
		b.columnMaxs[i] = ""
	}
}

// CanPruneBlock determines if a block can be skipped based on predicate
// Requires index with column dictionary for type information
func CanPruneBlock(index *Index, block *BlockMeta, colName, operator, value string) bool {
	colName = strings.ToLower(colName)

	// Find column index in dictionary
	colIdx := -1
	var colType ColumnType
	for i, col := range index.Header.Columns {
		if strings.ToLower(col.Name) == colName {
			colIdx = i
			colType = col.Type
			break
		}
	}

	if colIdx == -1 || colIdx >= len(block.Columns) {
		return false // Column not found, can't prune
	}

	stats := &block.Columns[colIdx]
	min := stats.Min
	max := stats.Max

	// Empty stats mean we can't prune safely
	if min == "" && max == "" {
		return false
	}

	// Use type-aware comparison
	compare := func(a, b string) int {
		if colType == ColumnTypeNumeric {
			aNum, aErr := strconv.ParseFloat(a, 64)
			bNum, bErr := strconv.ParseFloat(b, 64)
			if aErr == nil && bErr == nil {
				if aNum < bNum {
					return -1
				} else if aNum > bNum {
					return 1
				}
				return 0
			}
		}
		// Fall back to lexicographic
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}

	switch operator {
	case "=":
		// Can prune if value is outside [min, max] range
		return compare(value, min) < 0 || compare(value, max) > 0
	case "!=":
		// Can only prune if min == max == value (entire block is that value)
		return compare(min, max) == 0 && compare(min, value) == 0
	case ">":
		// Can prune if value >= max (all values are <= max)
		return compare(value, max) >= 0
	case ">=":
		// Can prune if value > max
		return compare(value, max) > 0
	case "<":
		// Can prune if value <= min
		return compare(value, min) <= 0
	case "<=":
		// Can prune if value < min
		return compare(value, min) < 0
	default:
		return false
	}
}

// ValidateIndex checks if index is still valid for the given CSV file
func ValidateIndex(index *Index, csvPath string) error {
	stat, err := os.Stat(csvPath)
	if err != nil {
		return fmt.Errorf("stat CSV: %w", err)
	}

	if stat.Size() != index.Header.FileSize {
		return fmt.Errorf("file size mismatch: index has %d, file has %d",
			index.Header.FileSize, stat.Size())
	}

	if stat.ModTime().UnixNano() != index.Header.FileMtime {
		return fmt.Errorf("file modified since index built")
	}

	return nil
}

func parseCSVLine(raw []byte) ([]string, error) {
	r := csv.NewReader(bytes.NewReader(raw))
	r.FieldsPerRecord = -1
	return r.Read()
}
