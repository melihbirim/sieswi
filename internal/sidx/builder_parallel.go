package sidx

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
)

// ChunkResult represents the result of processing a chunk of the CSV file
type ChunkResult struct {
	StartRow       uint64
	EndRow         uint64
	StartOffset    uint64
	EndOffset      uint64
	ColumnMins     []string
	ColumnMaxs     []string
	EmptyCounts    []uint32
	NumericCounts  []int // For type inference
	NonEmptyCounts []int // For type inference
	Err            error
}

// ParallelBuilder builds indexes using multiple goroutines
type ParallelBuilder struct {
	blockSize         uint32
	skipTypeInference bool
	numWorkers        int
}

// NewParallelBuilder creates a new parallel index builder
func NewParallelBuilder(blockSize uint32, numWorkers int) *ParallelBuilder {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	return &ParallelBuilder{
		blockSize:  blockSize,
		numWorkers: numWorkers,
	}
}

// SetSkipTypeInference configures whether to skip type detection
func (pb *ParallelBuilder) SetSkipTypeInference(skip bool) {
	pb.skipTypeInference = skip
}

// BuildFromFile builds an index using parallel processing
func (pb *ParallelBuilder) BuildFromFile(csvPath string) (*Index, error) {
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

	// Read header
	reader := bufio.NewReaderSize(f, 2*1024*1024)
	headerLine, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read header: %w", err)
	}

	headers, err := parseCSVLine(headerLine)
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	numCols := len(headers)
	headerSize := int64(len(headerLine))

	// Divide file into chunks for parallel processing
	chunks := pb.divideIntoChunks(fileSize, headerSize)

	// Process chunks in parallel
	results := make(chan ChunkResult, len(chunks))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, pb.numWorkers)

	for _, chunk := range chunks {
		wg.Add(1)
		go func(c chunkInfo) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			result := pb.processChunk(csvPath, c, numCols, headerSize)
			results <- result
		}(chunk)
	}

	// Wait for all workers and close results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and merge results
	var allResults []ChunkResult
	for result := range results {
		if result.Err != nil {
			return nil, fmt.Errorf("chunk processing error: %w", result.Err)
		}
		allResults = append(allResults, result)
	}

	// Sort results by StartRow to maintain order
	// (Results may arrive out of order)
	for i := 0; i < len(allResults)-1; i++ {
		for j := i + 1; j < len(allResults); j++ {
			if allResults[j].StartRow < allResults[i].StartRow {
				allResults[i], allResults[j] = allResults[j], allResults[i]
			}
		}
	}

	// Merge results into blocks
	blocks := pb.mergeResultsIntoBlocks(allResults, numCols)

	// Determine column types
	columnTypes := make([]ColumnType, numCols)
	if pb.skipTypeInference {
		for i := range columnTypes {
			columnTypes[i] = ColumnTypeString
		}
	} else {
		// Use first result for type inference
		if len(allResults) > 0 {
			for i := 0; i < numCols; i++ {
				if allResults[0].NonEmptyCounts[i] > 0 &&
					allResults[0].NumericCounts[i]*5 >= allResults[0].NonEmptyCounts[i]*4 {
					columnTypes[i] = ColumnTypeNumeric
				} else {
					columnTypes[i] = ColumnTypeString
				}
			}
		}
	}

	columns := make([]ColumnInfo, numCols)
	for i := range columns {
		columns[i] = ColumnInfo{
			Name: headers[i],
			Type: columnTypes[i],
		}
	}

	return &Index{
		Header: Header{
			Version:   Version,
			BlockSize: pb.blockSize,
			NumBlocks: uint32(len(blocks)),
			FileSize:  fileSize,
			FileMtime: fileMtime,
			Columns:   columns,
		},
		Blocks: blocks,
	}, nil
}

type chunkInfo struct {
	StartOffset uint64
	EndOffset   uint64
	StartRow    uint64
}

// divideIntoChunks divides the file into roughly equal chunks for parallel processing
func (pb *ParallelBuilder) divideIntoChunks(fileSize, headerSize int64) []chunkInfo {
	dataSize := fileSize - headerSize
	if dataSize <= 0 {
		return []chunkInfo{}
	}

	// Aim for chunks that are multiples of block size
	rowsPerChunk := pb.blockSize * 4 // Process 4 blocks worth at a time
	avgBytesPerRow := int64(80)      // Estimate ~80 bytes per row
	chunkSize := int64(rowsPerChunk) * avgBytesPerRow

	numChunks := (dataSize + chunkSize - 1) / chunkSize
	if numChunks > int64(pb.numWorkers*2) {
		numChunks = int64(pb.numWorkers * 2) // Cap at 2x workers
	}
	if numChunks < 1 {
		numChunks = 1
	}

	actualChunkSize := (dataSize + numChunks - 1) / numChunks
	chunks := make([]chunkInfo, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		startOffset := headerSize + i*actualChunkSize
		endOffset := startOffset + actualChunkSize
		if endOffset > fileSize {
			endOffset = fileSize
		}

		chunks = append(chunks, chunkInfo{
			StartOffset: uint64(startOffset),
			EndOffset:   uint64(endOffset),
			StartRow:    0, // Will be computed during processing
		})
	}

	return chunks
}

// processChunk processes a chunk of the CSV file
func (pb *ParallelBuilder) processChunk(csvPath string, chunk chunkInfo, numCols int, headerSize int64) ChunkResult {
	f, err := os.Open(csvPath)
	if err != nil {
		return ChunkResult{Err: err}
	}
	defer f.Close()

	// Seek to chunk start
	if chunk.StartOffset > uint64(headerSize) {
		// Need to find line boundary - seek back and find newline
		seekPos := int64(chunk.StartOffset) - 1024 // Look back up to 1KB
		if seekPos < headerSize {
			seekPos = headerSize
		}
		if _, err := f.Seek(seekPos, 0); err != nil {
			return ChunkResult{Err: err}
		}

		reader := bufio.NewReader(f)
		// Skip to next newline
		if seekPos > headerSize {
			_, err := reader.ReadBytes('\n')
			if err != nil {
				return ChunkResult{Err: err}
			}
		}
		chunk.StartOffset = uint64(seekPos) + uint64(1024) // Approximate
	} else {
		if _, err := f.Seek(int64(chunk.StartOffset), 0); err != nil {
			return ChunkResult{Err: err}
		}
	}

	reader := bufio.NewReaderSize(f, 1*1024*1024)

	result := ChunkResult{
		StartOffset:    chunk.StartOffset,
		ColumnMins:     make([]string, numCols),
		ColumnMaxs:     make([]string, numCols),
		EmptyCounts:    make([]uint32, numCols),
		NumericCounts:  make([]int, numCols),
		NonEmptyCounts: make([]int, numCols),
	}

	csvBuffer := bytes.NewReader(nil)
	csvReader := csv.NewReader(csvBuffer)
	csvReader.FieldsPerRecord = -1

	rowCount := uint64(0)
	offset := chunk.StartOffset

	for offset < chunk.EndOffset {
		rawLine, err := reader.ReadBytes('\n')
		if err == io.EOF && len(rawLine) == 0 {
			break
		}
		if err != nil && err != io.EOF {
			result.Err = fmt.Errorf("read row: %w", err)
			return result
		}

		trimmed := bytes.TrimRight(rawLine, "\r\n")
		if len(trimmed) == 0 {
			if err == io.EOF {
				break
			}
			offset += uint64(len(rawLine))
			continue
		}

		csvBuffer.Reset(trimmed)
		record, perr := csvReader.Read()
		if perr != nil {
			// Skip malformed rows
			offset += uint64(len(rawLine))
			continue
		}

		if rowCount == 0 {
			result.StartRow = chunk.StartRow
		}

		// Update statistics
		for i := 0; i < numCols && i < len(record); i++ {
			value := record[i]
			if value == "" {
				result.EmptyCounts[i]++
				continue
			}

			if result.ColumnMins[i] == "" || value < result.ColumnMins[i] {
				result.ColumnMins[i] = value
			}
			if result.ColumnMaxs[i] == "" || value > result.ColumnMaxs[i] {
				result.ColumnMaxs[i] = value
			}

			// Type inference (only for first chunk)
			if !pb.skipTypeInference && chunk.StartOffset == uint64(headerSize) {
				result.NonEmptyCounts[i]++
				if _, err := strconv.ParseFloat(value, 64); err == nil {
					result.NumericCounts[i]++
				}
			}
		}

		rowCount++
		offset += uint64(len(rawLine))

		if err == io.EOF {
			break
		}
	}

	result.EndRow = result.StartRow + rowCount - 1
	result.EndOffset = offset

	return result
}

// mergeResultsIntoBlocks combines chunk results into index blocks
func (pb *ParallelBuilder) mergeResultsIntoBlocks(results []ChunkResult, numCols int) []BlockMeta {
	if len(results) == 0 {
		return nil
	}

	var blocks []BlockMeta

	// For simplicity, treat each result as potential blocks
	// In a production system, we'd merge adjacent results into blockSize chunks
	currentBlock := BlockMeta{
		StartRow:    results[0].StartRow,
		StartOffset: results[0].StartOffset,
		Columns:     make([]ColumnStats, numCols),
	}

	for i := range currentBlock.Columns {
		currentBlock.Columns[i].Min = results[0].ColumnMins[i]
		currentBlock.Columns[i].Max = results[0].ColumnMaxs[i]
		currentBlock.Columns[i].EmptyCount = results[0].EmptyCounts[i]
	}

	rowsInBlock := results[0].EndRow - results[0].StartRow + 1

	for _, result := range results[1:] {
		if rowsInBlock >= uint64(pb.blockSize) {
			// Finalize current block
			currentBlock.EndRow = currentBlock.StartRow + rowsInBlock - 1
			currentBlock.EndOffset = result.StartOffset
			blocks = append(blocks, currentBlock)

			// Start new block
			currentBlock = BlockMeta{
				StartRow:    result.StartRow,
				StartOffset: result.StartOffset,
				Columns:     make([]ColumnStats, numCols),
			}
			rowsInBlock = 0
		}

		// Merge statistics
		for i := 0; i < numCols; i++ {
			if result.ColumnMins[i] != "" {
				if currentBlock.Columns[i].Min == "" || result.ColumnMins[i] < currentBlock.Columns[i].Min {
					currentBlock.Columns[i].Min = result.ColumnMins[i]
				}
			}
			if result.ColumnMaxs[i] != "" {
				if currentBlock.Columns[i].Max == "" || result.ColumnMaxs[i] > currentBlock.Columns[i].Max {
					currentBlock.Columns[i].Max = result.ColumnMaxs[i]
				}
			}
			currentBlock.Columns[i].EmptyCount += result.EmptyCounts[i]
		}

		rowsInBlock += result.EndRow - result.StartRow + 1
	}

	// Add final block
	if rowsInBlock > 0 {
		lastResult := results[len(results)-1]
		currentBlock.EndRow = currentBlock.StartRow + rowsInBlock - 1
		currentBlock.EndOffset = lastResult.EndOffset
		blocks = append(blocks, currentBlock)
	}

	return blocks
}
