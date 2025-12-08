package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/melihbirim/sieswi/internal/sidx"
	"github.com/melihbirim/sieswi/internal/sqlparser"
)

const (
	ioBufferSize       = 256 * 1024 // 256KB keeps syscalls low without huge RSS.
	defaultFlushEveryN = 8192       // Flush every N rows - higher for bulk throughput.
)

// tryParallelExecute attempts parallel execution and returns (handled, error).
// If handled=false, caller should fall back to sequential.
// If handled=true, the error indicates success (nil) or failure.
func tryParallelExecute(query sqlparser.Query, out io.Writer) (bool, error) {
	err := ParallelExecute(query, out)
	if err == errSkipParallel {
		// Parallel processing was skipped, use sequential
		return false, nil
	}
	// Parallel was attempted (either succeeded with nil or failed with error)
	return true, err
}

// Execute streams query results to the provided writer.
func Execute(query sqlparser.Query, out io.Writer) error {
	// Try to load and validate index
	var index *sidx.Index
	indexPath := query.FilePath + ".sidx"
	if indexFile, err := os.Open(indexPath); err == nil {
		if loadedIdx, err := sidx.ReadIndex(indexFile); err == nil {
			// Validate index against CSV file
			if err := sidx.ValidateIndex(loadedIdx, query.FilePath); err == nil {
				index = loadedIdx
			} else if os.Getenv("SIDX_DEBUG") == "1" {
				fmt.Fprintf(os.Stderr, "[sidx] Index invalid: %v\n", err)
			}
		}
		indexFile.Close()
	}

	// Check if reading from stdin
	isStdin := query.FilePath == "-" || query.FilePath == "stdin"

	if isStdin {
		// Stdin: cannot use parallel, index, or seeking - direct sequential stream
		return executeFromStdin(query, out)
	}

	// Try parallel execution for large files without index
	// ParallelExecute returns nil if it should be skipped (file too small, small LIMIT, etc.)
	// It returns a real error only if parallel processing failed
	if index == nil && os.Getenv("SIDX_NO_PARALLEL") != "1" {
		parallelHandled, err := tryParallelExecute(query, out)
		if parallelHandled {
			return err // Parallel execution was attempted, return its result
		}
		// Fall through to sequential execution
	}

	file, err := os.Open(query.FilePath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer file.Close()

	// Note: We need file handle for seeking, can't use buffered reader until after seeks
	var reader *csv.Reader
	var fastReader *FastCSVReader
	var bufferedFile *bufio.Reader
	useFastPath := (index == nil) // Use fast parser when no index (no seeking needed)

	if index != nil {
		// Use unbuffered for seeking, will add buffer after seeks
		reader = csv.NewReader(file)
		reader.ReuseRecord = true
		reader.FieldsPerRecord = -1
	} else {
		// No index, use fast CSV parser (3-5x faster than encoding/csv)
		fastReader = NewFastCSVReader(file)
	}

	var headerRecord []string
	if useFastPath {
		headerRecord, err = fastReader.Read()
	} else {
		headerRecord, err = reader.Read()
	}
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	// IMPORTANT: Copy header because ReuseRecord=true will overwrite the slice
	header := make([]string, len(headerRecord))
	copy(header, headerRecord)

	// Pre-normalize headers once for fast WHERE evaluation
	normalizedHeaders := make([]string, len(header))
	normalisedIndex := make(map[string]int, len(header))
	for idx, name := range header {
		normalized := strings.ToLower(strings.TrimSpace(name))
		normalizedHeaders[idx] = normalized
		normalisedIndex[normalized] = idx
	}

	selectedIdxs, outputHeader, err := resolveProjection(query, header, normalisedIndex)
	if err != nil {
		return err
	}

	// Validate all columns referenced in WHERE clause exist
	if query.Where != nil {
		if err := validateWhereColumns(query.Where, normalisedIndex); err != nil {
			return err
		}
	}

	// Determine which blocks can be pruned and seek to first non-pruned block
	var pruneBlocks map[int]bool
	if index != nil && query.Where != nil {
		pruneBlocks = make(map[int]bool)
		prunedCount := 0
		for i := range index.Blocks {
			block := &index.Blocks[i]
			if canPruneBlockExpr(index, block, query.Where) {
				pruneBlocks[i] = true
				prunedCount++
			}
		}
		if os.Getenv("SIDX_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "[sidx] Loaded index with %d blocks, pruned %d (%.1f%%)\n",
				len(index.Blocks), prunedCount, 100.0*float64(prunedCount)/float64(len(index.Blocks)))
		}

		// Seek to first non-pruned block if possible
		for i := range index.Blocks {
			if !pruneBlocks[i] {
				block := &index.Blocks[i]
				if _, err := file.Seek(int64(block.StartOffset), io.SeekStart); err == nil {
					// Successfully seeked, now add buffering
					bufferedFile = bufio.NewReaderSize(file, ioBufferSize)
					reader = csv.NewReader(bufferedFile)
					reader.ReuseRecord = true
					reader.FieldsPerRecord = -1
					useFastPath = false // Disable fast path after seeking
					if os.Getenv("SIDX_DEBUG") == "1" {
						fmt.Fprintf(os.Stderr, "[sidx] Seeked to block %d offset %d\n", i, block.StartOffset)
					}
				}
				break
			}
		}
	}

	writer := csv.NewWriter(out)
	if err := writer.Write(outputHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	writer.Flush() // push header immediately for better time-to-first-row
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush header: %w", err)
	}

	written := 0
	rowsSinceFlush := 0
	currentRow := uint64(0)
	currentBlockIdx := 0

	// Find which block we started in (if we seeked)
	if index != nil && len(index.Blocks) > 0 {
		for i := range index.Blocks {
			if !pruneBlocks[i] {
				currentBlockIdx = i
				currentRow = index.Blocks[i].StartRow
				break
			}
		}
	}

	// Pre-allocate rowMap for WHERE evaluation to avoid repeated allocations
	var rowMap map[string]string
	if query.Where != nil {
		rowMap = make(map[string]string, len(header))
	}

	for {
		// Check if we've entered a pruned block and should skip ahead
		if index != nil && currentBlockIdx < len(index.Blocks) {
			block := &index.Blocks[currentBlockIdx]

			// If current row is beyond this block, move to next block
			if currentRow >= block.EndRow {
				currentBlockIdx++
				if currentBlockIdx < len(index.Blocks) {
					block = &index.Blocks[currentBlockIdx]
				}
			}

			// If we're in a pruned block, seek to next unpruned block
			if currentBlockIdx < len(index.Blocks) && pruneBlocks[currentBlockIdx] {
				// Find next unpruned block
				nextBlockIdx := currentBlockIdx + 1
				for nextBlockIdx < len(index.Blocks) && pruneBlocks[nextBlockIdx] {
					nextBlockIdx++
				}

				if nextBlockIdx >= len(index.Blocks) {
					break // No more unpruned blocks
				}

				nextBlock := &index.Blocks[nextBlockIdx]
				if _, err := file.Seek(int64(nextBlock.StartOffset), io.SeekStart); err == nil {
					// Successfully seeked, recreate buffered reader
					bufferedFile = bufio.NewReaderSize(file, ioBufferSize)
					reader = csv.NewReader(bufferedFile)
					reader.ReuseRecord = true
					reader.FieldsPerRecord = -1
					useFastPath = false // Disable fast path after seeking
					currentBlockIdx = nextBlockIdx
					currentRow = nextBlock.StartRow

					if os.Getenv("SIDX_DEBUG") == "1" {
						fmt.Fprintf(os.Stderr, "[sidx] Skipped pruned blocks, seeked to block %d offset %d\n",
							nextBlockIdx, nextBlock.StartOffset)
					}
				}
			}
		}

		var record []string
		if useFastPath {
			record, err = fastReader.Read()
		} else {
			record, err = reader.Read()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}

		currentRow++

		// Evaluate WHERE clause if present
		if query.Where != nil {
			// Populate rowMap with pre-normalized headers (reuses map allocation)
			for k := range rowMap {
				delete(rowMap, k) // Clear previous row's data
			}
			for i := range normalizedHeaders {
				if i < len(record) {
					rowMap[normalizedHeaders[i]] = record[i]
				}
			}
			if !sqlparser.EvaluateNormalized(query.Where, rowMap) {
				continue
			}
		}

		row := project(record, selectedIdxs)
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}

		written++
		rowsSinceFlush++
		if rowsSinceFlush >= defaultFlushEveryN {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("flush rows: %w", err)
			}
			rowsSinceFlush = 0
		}

		if query.Limit >= 0 && written >= query.Limit {
			break
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush rows: %w", err)
	}

	return nil
}

func resolveProjection(query sqlparser.Query, header []string, index map[string]int) ([]int, []string, error) {
	if query.AllColumns {
		idxs := make([]int, len(header))
		for i := range header {
			idxs[i] = i
		}
		return idxs, header, nil
	}

	idxs := make([]int, len(query.Columns))
	names := make([]string, len(query.Columns))

	for i, col := range query.Columns {
		normalized := strings.ToLower(col)
		idx, ok := index[normalized]
		if !ok {
			return nil, nil, fmt.Errorf("column %q not found in CSV header", col)
		}
		idxs[i] = idx
		names[i] = header[idx]
	}

	return idxs, names, nil
}

func project(record []string, columns []int) []string {
	projected := make([]string, len(columns))
	for i, idx := range columns {
		if idx < len(record) {
			projected[i] = record[idx]
		}
	}
	return projected
}

// validateWhereColumns checks that all columns in expression exist
func validateWhereColumns(expr sqlparser.Expression, index map[string]int) error {
	switch e := expr.(type) {
	case sqlparser.BinaryExpr:
		if err := validateWhereColumns(e.Left, index); err != nil {
			return err
		}
		return validateWhereColumns(e.Right, index)
	case sqlparser.UnaryExpr:
		return validateWhereColumns(e.Expr, index)
	case sqlparser.Comparison:
		_, ok := index[strings.ToLower(e.Column)]
		if !ok {
			return fmt.Errorf("column %q not found in CSV header", e.Column)
		}
		return nil
	}
	return nil
}

// canPruneBlockExpr determines if a block can be pruned based on expression
func canPruneBlockExpr(index *sidx.Index, block *sidx.BlockMeta, expr sqlparser.Expression) bool {
	switch e := expr.(type) {
	case sqlparser.BinaryExpr:
		if e.Operator == "AND" {
			// Can prune if either side allows pruning
			return canPruneBlockExpr(index, block, e.Left) || canPruneBlockExpr(index, block, e.Right)
		} else if e.Operator == "OR" {
			// Can only prune if BOTH sides allow pruning
			return canPruneBlockExpr(index, block, e.Left) && canPruneBlockExpr(index, block, e.Right)
		}
		return false
	case sqlparser.UnaryExpr:
		// NOT: conservative, don't prune
		return false
	case sqlparser.Comparison:
		return sidx.CanPruneBlock(index, block, e.Column, e.Operator, e.Value)
	}
	return false
}

// executeFromStdin handles queries reading from stdin (piped data)
func executeFromStdin(query sqlparser.Query, out io.Writer) error {
	reader := csv.NewReader(bufio.NewReader(os.Stdin))
	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1

	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Build column map
	colMap := make(map[string]int, len(header))
	for i, col := range header {
		colMap[strings.ToLower(col)] = i
	}

	// Determine output columns
	outCols := header
	outIndices := make([]int, len(header))
	for i := range outIndices {
		outIndices[i] = i
	}

	if !query.AllColumns {
		outCols = query.Columns
		outIndices = make([]int, len(query.Columns))
		for i, col := range query.Columns {
			idx, ok := colMap[strings.ToLower(col)]
			if !ok {
				return fmt.Errorf("column not found: %s", col)
			}
			outIndices[i] = idx
		}
	}

	// Write output header
	writer := csv.NewWriter(out)
	defer writer.Flush()

	if err := writer.Write(outCols); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Stream rows
	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}

		// Apply WHERE filter
		if query.Where != nil {
			// Build row map for evaluation
			rowMap := make(map[string]string, len(record))
			for col, idx := range colMap {
				if idx < len(record) {
					rowMap[col] = record[idx]
				}
			}

			if !sqlparser.Evaluate(query.Where, rowMap) {
				continue
			}
		}

		// Build output row
		outRow := make([]string, len(outIndices))
		for i, idx := range outIndices {
			if idx < len(record) {
				outRow[i] = record[idx]
			}
		}

		if err := writer.Write(outRow); err != nil {
			return fmt.Errorf("write row: %w", err)
		}

		rowCount++
		if query.Limit > 0 && rowCount >= query.Limit {
			break
		}
	}

	return nil
}
