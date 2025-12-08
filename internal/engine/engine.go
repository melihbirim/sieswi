package engine

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sieswi/sieswi/internal/sidx"
	"github.com/sieswi/sieswi/internal/sqlparser"
)

const (
	ioBufferSize       = 256 * 1024 // 256KB keeps syscalls low without huge RSS.
	defaultFlushEveryN = 128        // Flush every N rows to keep streaming latency low.
)

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

	file, err := os.Open(query.FilePath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer file.Close()

	// Note: We need file handle for seeking, can't use buffered reader until after seeks
	var reader *csv.Reader
	var bufferedFile *bufio.Reader

	if index != nil {
		// Use unbuffered for seeking, will add buffer after seeks
		reader = csv.NewReader(file)
	} else {
		// No index, use buffered from start
		bufferedFile = bufio.NewReaderSize(file, ioBufferSize)
		reader = csv.NewReader(bufferedFile)
	}

	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	normalisedIndex := make(map[string]int, len(header))
	for idx, name := range header {
		normalisedIndex[strings.ToLower(strings.TrimSpace(name))] = idx
	}

	selectedIdxs, outputHeader, err := resolveProjection(query, header, normalisedIndex)
	if err != nil {
		return err
	}

	var predicateIdx int
	var predicate *sqlparser.Predicate
	if query.Predicate != nil {
		predicate = query.Predicate
		idx, ok := normalisedIndex[strings.ToLower(predicate.Column)]
		if !ok {
			return fmt.Errorf("column %q not found in CSV header", predicate.Column)
		}
		predicateIdx = idx
	}

	// Determine which blocks can be pruned and seek to first non-pruned block
	var pruneBlocks map[int]bool
	if index != nil && predicate != nil {
		pruneBlocks = make(map[int]bool)
		prunedCount := 0
		for i := range index.Blocks {
			block := &index.Blocks[i]
			if sidx.CanPruneBlock(index, block, predicate.Column, predicate.Operator, predicate.Value) {
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
					currentBlockIdx = nextBlockIdx
					currentRow = nextBlock.StartRow

					if os.Getenv("SIDX_DEBUG") == "1" {
						fmt.Fprintf(os.Stderr, "[sidx] Skipped pruned blocks, seeked to block %d offset %d\n",
							nextBlockIdx, nextBlock.StartOffset)
					}
				}
			}
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}

		currentRow++

		if predicate != nil {
			value := ""
			if predicateIdx < len(record) {
				value = record[predicateIdx]
			}
			if !predicate.Compare(value) {
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
