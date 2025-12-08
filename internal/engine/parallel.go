package engine

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/sieswi/sieswi/internal/sqlparser"
)

const (
	// Size of chunks for parallel processing
	parallelChunkSize = 4 * 1024 * 1024 // 4MB chunks
)

// chunk represents a portion of the CSV file to process.
type chunk struct {
	id     int
	offset int64
	size   int64
}

// result represents the processed rows from a chunk.
type result struct {
	id   int
	rows [][]string
	err  error
}

// ParallelExecute processes CSV in parallel chunks when no index is available.
// This is significantly faster for full table scans on multi-core systems.
func ParallelExecute(query sqlparser.Query, out io.Writer) error {
	// Get file size to decide if parallel processing is worth it
	fileInfo, err := os.Stat(query.FilePath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	// Only use parallel processing for large files (>10MB)
	// Skip for small LIMIT queries (< 10000 rows) where sequential is faster
	if fileInfo.Size() < 10*1024*1024 {
		return nil // File too small, use sequential
	}
	if query.Limit >= 0 && query.Limit < 10000 {
		return nil // Small LIMIT, sequential is faster
	}

	file, err := os.Open(query.FilePath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer file.Close()

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

	// Get current file position (after header)
	headerEnd, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("get position: %w", err)
	}

	dataSize := fileInfo.Size() - headerEnd
	numChunks := int((dataSize + parallelChunkSize - 1) / parallelChunkSize)

	// Use all available CPU cores as workers for maximum throughput
	workers := runtime.GOMAXPROCS(0)
	if workers > numChunks {
		workers = numChunks
	}
	if workers < 1 {
		workers = 1
	}

	// Create channels for work distribution
	chunks := make(chan chunk, numChunks)
	results := make(chan result, numChunks)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processChunks(query.FilePath, chunks, results, query, normalizedHeaders, selectedIdxs, header)
		}()
	}

	// Send chunks to workers
	go func() {
		for i := 0; i < numChunks; i++ {
			offset := headerEnd + int64(i*parallelChunkSize)
			size := int64(parallelChunkSize)
			if offset+size > fileInfo.Size() {
				size = fileInfo.Size() - offset
			}
			chunks <- chunk{id: i, offset: offset, size: size}
		}
		close(chunks)
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

	for res := range results {
		if res.err != nil {
			return fmt.Errorf("chunk %d: %w", res.id, res.err)
		}

		resultMap[res.id] = res.rows

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

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}

	if os.Getenv("SIDX_DEBUG") == "1" {
		fmt.Fprintf(os.Stderr, "[parallel] Processed %d chunks with %d workers, wrote %d rows\n",
			numChunks, workers, rowCount)
	}

	return nil
}

// processChunks is the worker function that processes chunks from the channel.
func processChunks(
	filePath string,
	chunks <-chan chunk,
	results chan<- result,
	query sqlparser.Query,
	normalizedHeaders []string,
	selectedIdxs []int,
	header []string,
) {
	// Each worker opens its own file handle
	file, err := os.Open(filePath)
	if err != nil {
		results <- result{err: err}
		return
	}
	defer file.Close()

	for ch := range chunks {
		rows, err := processChunk(file, ch, query, normalizedHeaders, selectedIdxs, header)
		results <- result{id: ch.id, rows: rows, err: err}
	}
}

// processChunk processes a single chunk of the CSV file.
func processChunk(
	file *os.File,
	ch chunk,
	query sqlparser.Query,
	normalizedHeaders []string,
	selectedIdxs []int,
	header []string,
) ([][]string, error) {
	// Seek to chunk start
	if _, err := file.Seek(ch.offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	// Read chunk data with extra space for partial line
	buf := make([]byte, ch.size+8192)
	n, readErr := io.ReadFull(file, buf[:ch.size])
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read chunk: %w", readErr)
	}

	// Try to read extra bytes to find complete line
	if readErr == nil {
		extra, _ := file.Read(buf[n:]) // Best effort, ignore errors
		n += extra
	}
	buf = buf[:n]
	atEOF := (readErr == io.EOF || readErr == io.ErrUnexpectedEOF)

	// If not first chunk, skip to next newline (avoid partial line)
	start := 0
	if ch.id > 0 {
		idx := bytes.IndexByte(buf, '\n')
		if idx == -1 {
			return nil, nil // No complete lines in chunk
		}
		start = idx + 1
	}

	// Find last complete line (only trim if not at EOF)
	end := len(buf)
	if !atEOF {
		// Not at EOF, find last newline to avoid splitting a row
		for end > start && buf[end-1] != '\n' {
			end--
		}
		if end == start {
			// No newline found in chunk - line too long or data issue
			if os.Getenv("SIDX_DEBUG") == "1" {
				fmt.Fprintf(os.Stderr, "[parallel] Warning: chunk %d has no newline, possibly line > 4MB\n", ch.id)
			}
			return nil, nil
		}
	}

	chunkData := buf[start:end]
	if len(chunkData) == 0 {
		return nil, nil
	}

	// Parse lines in chunk
	var rows [][]string
	scanner := bufio.NewScanner(bytes.NewReader(chunkData))
	// Use large buffer to handle very long CSV lines (up to chunk size)
	scanBuf := make([]byte, parallelChunkSize)
	scanner.Buffer(scanBuf, parallelChunkSize)

	// Pre-allocate rowMap for WHERE evaluation
	var rowMap map[string]string
	if query.Where != nil {
		rowMap = make(map[string]string, len(header))
	}

	// Reusable fields slice to reduce allocations
	fields := make([]string, 0, len(header))

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse CSV line (reuses fields slice)
		fields = parseCSVLineInto(line, fields[:0])
		if len(fields) == 0 {
			continue
		}

		// Evaluate WHERE clause (overwrite values, don't clear map)
		if query.Where != nil {
			// Populate rowMap by overwriting (faster than clearing)
			for i := range normalizedHeaders {
				if i < len(fields) {
					rowMap[normalizedHeaders[i]] = fields[i]
				} else {
					rowMap[normalizedHeaders[i]] = "" // Empty for missing columns
				}
			}
			if !sqlparser.EvaluateNormalized(query.Where, rowMap) {
				continue
			}
		}

		// Project columns
		row := make([]string, len(selectedIdxs))
		for i, idx := range selectedIdxs {
			if idx < len(fields) {
				row[i] = fields[idx]
			}
		}
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan chunk: %w", err)
	}

	return rows, nil
}

// parseCSVLineInto parses a CSV line into the provided fields slice (reused).
// Handles quoted fields with embedded commas and escaped quotes ("").
// This reduces allocations by reusing the slice across calls.
func parseCSVLineInto(line []byte, fields []string) []string {
	start := 0
	inQuote := false

	for i := 0; i < len(line); i++ {
		c := line[i]

		if c == '"' {
			// Check for escaped quote ("")
			if inQuote && i+1 < len(line) && line[i+1] == '"' {
				i++ // Skip the escaped quote
				continue
			}
			inQuote = !inQuote
		} else if c == ',' && !inQuote {
			// Field boundary - extract and clean
			fields = append(fields, cleanField(line[start:i]))
			start = i + 1
		}
	}

	// Last field
	if start <= len(line) {
		fields = append(fields, cleanField(line[start:]))
	}

	return fields
}

// cleanField trims spaces and removes surrounding quotes from a CSV field.
func cleanField(field []byte) string {
	if len(field) == 0 {
		return ""
	}

	// Fast path: no quotes and no spaces - just convert to string
	hasQuote := field[0] == '"'
	hasSpace := field[0] == ' ' || field[len(field)-1] == ' '

	if !hasQuote && !hasSpace {
		return string(field)
	}

	// Trim leading/trailing spaces
	fieldStart := 0
	fieldEnd := len(field)

	for fieldStart < fieldEnd && field[fieldStart] == ' ' {
		fieldStart++
	}
	for fieldEnd > fieldStart && field[fieldEnd-1] == ' ' {
		fieldEnd--
	}

	if fieldStart >= fieldEnd {
		return ""
	}

	// Remove surrounding quotes if present
	if field[fieldStart] == '"' && field[fieldEnd-1] == '"' {
		fieldStart++
		fieldEnd--

		// Check if ANY escaped quotes exist
		hasEscapedQuotes := false
		for i := fieldStart; i < fieldEnd-1; i++ {
			if field[i] == '"' && field[i+1] == '"' {
				hasEscapedQuotes = true
				break
			}
		}

		if hasEscapedQuotes {
			// Build unescaped string in one pass
			var result strings.Builder
			result.Grow(fieldEnd - fieldStart)
			for i := fieldStart; i < fieldEnd; i++ {
				if field[i] == '"' && i+1 < fieldEnd && field[i+1] == '"' {
					result.WriteByte('"')
					i++ // Skip the second quote
				} else {
					result.WriteByte(field[i])
				}
			}
			return result.String()
		}
	}

	return string(field[fieldStart:fieldEnd])
}
