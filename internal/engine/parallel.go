package engine

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

var errSkipParallel = errors.New("parallel processing skipped")

// rowBatch represents a batch of CSV rows to process
type rowBatch struct {
	id   int
	rows [][]string // Pre-parsed CSV rows
}

// batchResult represents processed rows from a batch
type batchResult struct {
	id   int
	rows [][]string
	err  error
}

// ParallelExecute processes CSV in parallel by having one goroutine read/parse rows
// and multiple worker goroutines filter and project them. This avoids chunk boundary issues.
func ParallelExecute(query sqlparser.Query, out io.Writer) error {
	// Get file size to decide if parallel processing is worth it
	fileInfo, err := os.Stat(query.FilePath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	// Only use parallel processing for large files (>10MB)
	// Skip for small LIMIT queries (< 10000 rows) where sequential is faster
	if fileInfo.Size() < 10*1024*1024 {
		return errSkipParallel // File too small, use sequential
	}
	if query.Limit >= 0 && query.Limit < 10000 {
		return errSkipParallel // Small LIMIT, sequential is faster
	}

	file, err := os.Open(query.FilePath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil && os.Getenv("SIDX_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "[sidx] Failed to close CSV file: %v\n", err)
		}
	}()

	// Read header first (sequential)
	reader := csv.NewReader(bufio.NewReaderSize(file, ioBufferSize))
	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1

	headerRecord, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	header := make([]string, len(headerRecord))
	copy(header, headerRecord)

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

	if query.Where != nil {
		if err := validateWhereColumns(query.Where, normalisedIndex); err != nil {
			return err
		}
	}

	// Write header
	writer := csv.NewWriter(out)
	if err := writer.Write(outputHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush header: %w", err)
	}

	// Use all available CPU cores as workers
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}

	// Create channels
	const batchSize = 10000 // Rows per batch
	batches := make(chan rowBatch, workers*2)
	results := make(chan batchResult, workers*2)

	// Start worker goroutines to process batches
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processBatches(batches, results, query, normalizedHeaders, selectedIdxs)
		}()
	}

	// Start reader goroutine to read CSV and create batches
	readErr := make(chan error, 1)
	go func() {
		batchID := 0
		batch := make([][]string, 0, batchSize)

		for {
			record, err := reader.Read()
			if err == io.EOF {
				// Send final batch if any
				if len(batch) > 0 {
					batches <- rowBatch{id: batchID, rows: batch}
				}
				close(batches)
				readErr <- nil
				return
			}
			if err != nil {
				close(batches)
				readErr <- fmt.Errorf("read row: %w", err)
				return
			}

			// Copy record since reader reuses the slice
			row := make([]string, len(record))
			copy(row, record)
			batch = append(batch, row)

			if len(batch) >= batchSize {
				batches <- rowBatch{id: batchID, rows: batch}
				batchID++
				batch = make([][]string, 0, batchSize)
			}
		}
	}()

	// Close results when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and write results in order
	resultMap := make(map[int][][]string)
	nextID := 0
	rowCount := 0
	batchesProcessed := 0

	for res := range results {
		if res.err != nil {
			return fmt.Errorf("batch %d: %w", res.id, res.err)
		}

		resultMap[res.id] = res.rows
		batchesProcessed++

		// Write results in order
		for {
			rows, ok := resultMap[nextID]
			if !ok {
				break
			}

			for _, row := range rows {
				// Check LIMIT before writing
				if query.Limit >= 0 && rowCount >= query.Limit {
					goto done // Exit both loops
				}

				if err := writer.Write(row); err != nil {
					return fmt.Errorf("write row: %w", err)
				}
				rowCount++
				if rowCount%defaultFlushEveryN == 0 {
					writer.Flush()
					if err := writer.Error(); err != nil {
						return fmt.Errorf("flush rows: %w", err)
					}
				}
			}

			delete(resultMap, nextID)
			nextID++
		}
	}

done:
	// Check for read errors
	if err := <-readErr; err != nil {
		return err
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}

	if os.Getenv("SIDX_DEBUG") == "1" {
		fmt.Fprintf(os.Stderr, "[parallel] Processed %d batches with %d workers, wrote %d rows\n",
			batchesProcessed, workers, rowCount)
	}

	return nil
}

// processBatches processes row batches from the channel
func processBatches(
	batches <-chan rowBatch,
	results chan<- batchResult,
	query sqlparser.Query,
	normalizedHeaders []string,
	selectedIdxs []int,
) {
	// Pre-allocate rowMap for WHERE evaluation
	var rowMap map[string]string
	if query.Where != nil {
		rowMap = make(map[string]string, len(normalizedHeaders))
	}

	for batch := range batches {
		var filteredRows [][]string

		for _, record := range batch.rows {
			// Evaluate WHERE clause if present
			if query.Where != nil {
				// Clear and populate rowMap
				for k := range rowMap {
					delete(rowMap, k)
				}
				for i := range normalizedHeaders {
					if i < len(record) {
						rowMap[normalizedHeaders[i]] = record[i]
					} else {
						rowMap[normalizedHeaders[i]] = ""
					}
				}
				if !sqlparser.EvaluateNormalized(query.Where, rowMap) {
					continue
				}
			}

			// Project columns
			row := make([]string, len(selectedIdxs))
			for i, idx := range selectedIdxs {
				if idx < len(record) {
					row[i] = record[idx]
				}
			}
			filteredRows = append(filteredRows, row)
		}

		results <- batchResult{
			id:   batch.id,
			rows: filteredRows,
			err:  nil,
		}
	}
}
