package sidx

import (
	"fmt"
	"testing"
)

func TestBlockPruning(t *testing.T) {
	// Build index for test
	builder := NewBuilder(BlockSize)
	idx, err := builder.BuildFromFile("../../fixtures/ecommerce_1m.csv")
	if err != nil {
		t.Skip("Skip: ecommerce_1m.csv not found")
		return
	}

	fmt.Printf("\n=== Index Stats ===\n")
	fmt.Printf("Total blocks: %d\n", idx.Header.NumBlocks)
	fmt.Printf("Block size: %d rows\n", idx.Header.BlockSize)

	// Find country column index
	countryIdx := -1
	for i, col := range idx.Header.Columns {
		if col.Name == "country" {
			countryIdx = i
			break
		}
	}
	if countryIdx == -1 {
		t.Fatal("country column not found in index")
	}

	// Test pruning for country = 'AU'
	fmt.Printf("\n=== Testing country = 'AU' ===\n")
	pruned := 0
	for i := range idx.Blocks {
		block := &idx.Blocks[i]
		if CanPruneBlock(idx, block, "country", "=", "AU") {
			pruned++
		} else {
			countryStats := &block.Columns[countryIdx]
			fmt.Printf("Block %d: rows %d-%d, country [%s, %s]\n",
				i, block.StartRow, block.EndRow, countryStats.Min, countryStats.Max)
		}
	}
	fmt.Printf("Pruned: %d/%d blocks (%.1f%%)\n",
		pruned, idx.Header.NumBlocks, 100.0*float64(pruned)/float64(idx.Header.NumBlocks))
}
