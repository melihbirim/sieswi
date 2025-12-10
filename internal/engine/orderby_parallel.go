package engine

import (
	"sort"
	"sync"

	"github.com/melihbirim/sieswi/internal/sqlparser"
)

// parallelSort performs parallel merge sort when dataset is large enough
// This can achieve 2-3x speedup on multi-core systems (M2 has 12 cores)
func parallelSort(rows []rowWithKey, orderBy []sqlparser.OrderByColumn, numWorkers int) {
	if len(rows) < 50000 || numWorkers <= 1 {
		// Too small for parallel overhead - use standard sort
		standardSort(rows, orderBy)
		return
	}

	// Calculate chunk size (roughly equal division)
	chunkSize := (len(rows) + numWorkers - 1) / numWorkers
	
	// Sort each chunk in parallel
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(rows) {
			break
		}
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}

		wg.Add(1)
		go func(chunk []rowWithKey) {
			defer wg.Done()
			standardSort(chunk, orderBy)
		}(rows[start:end])
	}
	wg.Wait()

	// Merge sorted chunks
	mergeChunks(rows, chunkSize, numWorkers, orderBy)
}

// standardSort performs the regular sort comparison
func standardSort(rows []rowWithKey, orderBy []sqlparser.OrderByColumn) {
	sort.SliceStable(rows, func(i, j int) bool {
		return compareRows(&rows[i], &rows[j], orderBy)
	})
}

// compareRows compares two rows based on ORDER BY columns
func compareRows(a, b *rowWithKey, orderBy []sqlparser.OrderByColumn) bool {
	for k, orderCol := range orderBy {
		keyA := a.sortKeys[k]
		keyB := b.sortKeys[k]

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
			// Use pre-lowercased string comparison
			if keyA.strValueLower < keyB.strValueLower {
				cmp = -1
			} else if keyA.strValueLower > keyB.strValueLower {
				cmp = 1
			} else {
				cmp = 0
			}
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

// mergeChunks performs k-way merge of sorted chunks
func mergeChunks(rows []rowWithKey, chunkSize, numChunks int, orderBy []sqlparser.OrderByColumn) {
	// Create auxiliary array for merging
	aux := make([]rowWithKey, len(rows))
	copy(aux, rows)

	// Binary merge (more efficient than k-way for small k)
	for mergeSize := chunkSize; mergeSize < len(rows); mergeSize *= 2 {
		for start := 0; start < len(rows); start += 2 * mergeSize {
			mid := start + mergeSize
			if mid >= len(rows) {
				break
			}
			end := start + 2*mergeSize
			if end > len(rows) {
				end = len(rows)
			}

			merge(rows, aux, start, mid, end, orderBy)
		}
	}
}

// merge merges two sorted subarrays
func merge(dst, src []rowWithKey, lo, mid, hi int, orderBy []sqlparser.OrderByColumn) {
	i, j := lo, mid
	for k := lo; k < hi; k++ {
		if i >= mid {
			dst[k] = src[j]
			j++
		} else if j >= hi {
			dst[k] = src[i]
			i++
		} else if compareRows(&src[j], &src[i], orderBy) {
			dst[k] = src[j]
			j++
		} else {
			dst[k] = src[i]
			i++
		}
	}
	// Copy back to source for next iteration
	copy(src[lo:hi], dst[lo:hi])
}
