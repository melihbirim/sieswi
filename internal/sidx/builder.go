package sidx

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Builder collects statistics while scanning a CSV file
type Builder struct {
	blockSize  uint32
	currentRow uint64
	blocks     []BlockMeta

	// Current block state
	blockStartRow     uint64
	blockStartOffset  uint64
	lastRowEndOffset  uint64
	columnMins        []string
	columnMaxs        []string
	columnEmptyCounts []uint32
	columnTypes       []ColumnType
	headers           []string

	// Type inference state (computed during first block)
	typeInferenceActive bool
	skipTypeInference   bool
	numericCounts       []int
	nonEmptyCounts      []int

	// Reusable CSV parsing buffer
	csvReader *csv.Reader
	csvBuffer *bytes.Reader
}

func NewBuilder(blockSize uint32) *Builder {
	return &Builder{
		blockSize: blockSize,
	}
}

// SetSkipTypeInference configures whether to skip type detection
// When true, all columns are assumed to be strings (faster indexing)
func (b *Builder) SetSkipTypeInference(skip bool) {
	b.skipTypeInference = skip
}

// finalizeTypeInference determines column types based on collected statistics
func (b *Builder) finalizeTypeInference() {
	for i := range b.columnTypes {
		// If >80% of non-empty values are numeric, treat as numeric
		if b.nonEmptyCounts[i] > 0 && b.numericCounts[i]*5 >= b.nonEmptyCounts[i]*4 {
			b.columnTypes[i] = ColumnTypeNumeric
		} else {
			b.columnTypes[i] = ColumnTypeString
		}
	}
}

// inferColumnType is a helper for testing type inference logic
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
	defer func() {
		if err := f.Close(); err != nil && os.Getenv("SIDX_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "[sidx] Failed to close CSV file: %v\n", err)
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	fileSize := stat.Size()
	fileMtime := stat.ModTime().UnixNano()

	reader := bufio.NewReaderSize(f, 2*1024*1024) // 2MB buffer for better throughput
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
	b.columnEmptyCounts = make([]uint32, numCols)
	b.columnTypes = make([]ColumnType, numCols)

	// Type inference during first block (unless skipped)
	if !b.skipTypeInference {
		b.typeInferenceActive = true
		b.numericCounts = make([]int, numCols)
		b.nonEmptyCounts = make([]int, numCols)
	}

	// Initialize reusable CSV parser
	b.csvBuffer = bytes.NewReader(nil)
	b.csvReader = csv.NewReader(b.csvBuffer)
	b.csvReader.FieldsPerRecord = -1

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

		// Reuse CSV reader to avoid allocations
		b.csvBuffer.Reset(trimmed)
		record, perr := b.csvReader.Read()
		if perr != nil {
			return nil, fmt.Errorf("parse row %d: %w", b.currentRow, perr)
		}

		if rowInBlock == 0 {
			b.blockStartOffset = rowStart
		}

		for i := 0; i < numCols && i < len(record); i++ {
			value := record[i]
			if value == "" {
				b.columnEmptyCounts[i]++
				continue
			}

			if b.columnMins[i] == "" || value < b.columnMins[i] {
				b.columnMins[i] = value
			}
			if b.columnMaxs[i] == "" || value > b.columnMaxs[i] {
				b.columnMaxs[i] = value
			}

			// Type inference during first block
			if b.typeInferenceActive {
				b.nonEmptyCounts[i]++
				if _, err := strconv.ParseFloat(value, 64); err == nil {
					b.numericCounts[i]++
				}
			}
		}

		b.currentRow++
		rowInBlock++
		offset += int64(len(rawLine))
		b.lastRowEndOffset = uint64(offset)

		if rowInBlock >= b.blockSize {
			b.flushBlock()
			rowInBlock = 0

			// Type inference complete after first block
			if b.typeInferenceActive {
				b.finalizeTypeInference()
				b.typeInferenceActive = false
			}
		}

		if err == io.EOF {
			break
		}
	}

	if b.currentRow > b.blockStartRow {
		b.flushBlock()
	}

	// Finalize type inference if we never hit a full block
	if b.typeInferenceActive {
		b.finalizeTypeInference()
		b.typeInferenceActive = false
	} else if b.skipTypeInference {
		// Set all columns to string type
		for i := range b.columnTypes {
			b.columnTypes[i] = ColumnTypeString
		}
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

	// Validate block invariants
	if b.currentRow <= b.blockStartRow {
		panic(fmt.Sprintf("invalid block: EndRow (%d) <= StartRow (%d)", b.currentRow, b.blockStartRow))
	}
	if b.lastRowEndOffset < b.blockStartOffset {
		panic(fmt.Sprintf("invalid block: EndOffset (%d) < StartOffset (%d)", b.lastRowEndOffset, b.blockStartOffset))
	}

	cols := make([]ColumnStats, len(b.headers))
	for i := range b.headers {
		// Validate min <= max when both present
		if b.columnMins[i] != "" && b.columnMaxs[i] != "" && b.columnMins[i] > b.columnMaxs[i] {
			panic(fmt.Sprintf("invalid block: column %q has min > max (%q > %q)", b.headers[i], b.columnMins[i], b.columnMaxs[i]))
		}
		cols[i] = ColumnStats{
			Min:        b.columnMins[i],
			Max:        b.columnMaxs[i],
			EmptyCount: b.columnEmptyCounts[i],
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
		b.columnEmptyCounts[i] = 0
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

	// If stats are empty but we have non-empty count info, check if block is all-empty
	if min == "" && max == "" {
		// If EmptyCount equals block size, all values are empty
		blockSize := block.EndRow - block.StartRow
		// Only use EmptyCount if it's meaningful (blockSize > 0 and EmptyCount > 0)
		if blockSize > 0 && stats.EmptyCount > 0 && stats.EmptyCount == uint32(blockSize) {
			// All empty: can prune for any operator except != empty
			if operator == "=" && value != "" {
				return true // Looking for non-empty value in all-empty column
			}
		}
		return false // Can't prune safely otherwise
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

	// Validate that CSV header matches index columns (only if columns are defined)
	if len(index.Header.Columns) > 0 {
		f, err := os.Open(csvPath)
		if err != nil {
			return fmt.Errorf("open CSV for header check: %w", err)
		}
		defer func() {
			if err := f.Close(); err != nil && os.Getenv("SIDX_DEBUG") == "1" {
				fmt.Fprintf(os.Stderr, "[sidx] Failed to close CSV file: %v\n", err)
			}
		}()

		reader := bufio.NewReader(f)
		headerLine, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("read CSV header: %w", err)
		}

		headerRecord, err := parseCSVLine(bytes.TrimRight(headerLine, "\r\n"))
		if err != nil {
			return fmt.Errorf("parse CSV header: %w", err)
		}

		if len(headerRecord) != len(index.Header.Columns) {
			return fmt.Errorf("column count mismatch: CSV has %d columns, index has %d",
				len(headerRecord), len(index.Header.Columns))
		}

		for i, csvCol := range headerRecord {
			if !strings.EqualFold(strings.TrimSpace(csvCol), index.Header.Columns[i].Name) {
				return fmt.Errorf("column %d mismatch: CSV has %q, index has %q",
					i, csvCol, index.Header.Columns[i].Name)
			}
		}
	}

	return nil
}

func parseCSVLine(raw []byte) ([]string, error) {
	r := csv.NewReader(bytes.NewReader(raw))
	r.FieldsPerRecord = -1
	return r.Read()
}
