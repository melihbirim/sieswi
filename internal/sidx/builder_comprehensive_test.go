package sidx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCanPruneBlock_RangeQueries tests various range query scenarios
func TestCanPruneBlock_RangeQueries(t *testing.T) {
	// Create test index with numeric column
	idx := &Index{
		Header: Header{
			Columns: []ColumnInfo{
				{Name: "id", Type: ColumnTypeNumeric},
				{Name: "name", Type: ColumnTypeString},
			},
		},
	}

	tests := []struct {
		name     string
		block    BlockMeta
		column   string
		operator string
		value    string
		want     bool // true if block should be pruned
	}{
		// Numeric equality tests
		{
			name: "numeric_equals_outside_range_low",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "=",
			value:    "50",
			want:     true, // 50 < 100, prune
		},
		{
			name: "numeric_equals_outside_range_high",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "=",
			value:    "300",
			want:     true, // 300 > 200, prune
		},
		{
			name: "numeric_equals_inside_range",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "=",
			value:    "150",
			want:     false, // 150 in [100, 200], keep
		},

		// Numeric > tests
		{
			name: "numeric_greater_than_max",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: ">",
			value:    "200",
			want:     true, // all values <= 200, prune
		},
		{
			name: "numeric_greater_than_below_max",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: ">",
			value:    "150",
			want:     false, // some values > 150 exist, keep
		},

		// Numeric < tests
		{
			name: "numeric_less_than_min",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "<",
			value:    "100",
			want:     true, // all values >= 100, prune
		},
		{
			name: "numeric_less_than_above_min",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "<",
			value:    "150",
			want:     false, // some values < 150 exist, keep
		},

		// String tests (lexicographic)
		{
			name: "string_equals_outside_range",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "alice", Max: "zoe"},
				},
			},
			column:   "name",
			operator: "=",
			value:    "aaron",
			want:     true, // "aaron" < "alice", prune
		},
		{
			name: "string_equals_inside_range",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "alice", Max: "zoe"},
				},
			},
			column:   "name",
			operator: "=",
			value:    "bob",
			want:     false, // "bob" in ["alice", "zoe"], keep
		},

		// Empty column tests
		{
			name: "empty_stats",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "", Max: ""},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "=",
			value:    "100",
			want:     false, // empty stats, can't prune safely
		},

		// != operator tests
		{
			name: "not_equals_constant_block_match",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "100"}, // All rows have value 100
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "!=",
			value:    "100",
			want:     true, // all rows are 100, prune
		},
		{
			name: "not_equals_constant_block_no_match",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "100"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "!=",
			value:    "200",
			want:     false, // all rows are 100 != 200, keep
		},
		{
			name: "not_equals_range_block",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "id",
			operator: "!=",
			value:    "150",
			want:     false, // mixed values, can't prune
		},

		// Column not found
		{
			name: "column_not_found",
			block: BlockMeta{
				Columns: []ColumnStats{
					{Min: "100", Max: "200"},
					{Min: "a", Max: "z"},
				},
			},
			column:   "missing",
			operator: "=",
			value:    "100",
			want:     false, // column not found, can't prune safely
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanPruneBlock(idx, &tt.block, tt.column, tt.operator, tt.value)
			if got != tt.want {
				t.Errorf("CanPruneBlock() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNumericVsStringComparison verifies type-aware comparison
func TestNumericVsStringComparison(t *testing.T) {
	// Test that "9" < "10" numerically but "9" > "10" lexicographically
	idx := &Index{
		Header: Header{
			Columns: []ColumnInfo{
				{Name: "numeric_col", Type: ColumnTypeNumeric},
				{Name: "string_col", Type: ColumnTypeString},
			},
		},
	}

	block := BlockMeta{
		Columns: []ColumnStats{
			{Min: "10", Max: "100"}, // Numeric: 10-100
			{Min: "10", Max: "100"}, // String: lexicographic
		},
	}

	// Numeric: 9 < 10, should prune (9 is less than min)
	if !CanPruneBlock(idx, &block, "numeric_col", "=", "9") {
		t.Error("Expected to prune: numeric 9 < 10")
	}

	// String: "9" > "100" lexicographically, should prune (9 is greater than max)
	if !CanPruneBlock(idx, &block, "string_col", "=", "9") {
		t.Error("Expected to prune: string '9' > '100' lexicographically")
	}

	// Numeric: 50 in [10, 100], should NOT prune
	if CanPruneBlock(idx, &block, "numeric_col", "=", "50") {
		t.Error("Expected to keep: numeric 50 in [10, 100]")
	}

	// String: "50" < "100" but > "10" lexicographically, should NOT prune
	// Note: lexicographically "10" < "50" < "9" (not "100")
	// Actually "100" < "50" < "9" lexicographically
	// So "50" is NOT in ["10", "100"] - it's outside!
	if !CanPruneBlock(idx, &block, "string_col", "=", "50") {
		t.Error("Expected to prune: string '50' > '100' lexicographically")
	}
}

// TestIndexValidation tests file metadata validation
func TestIndexValidation(t *testing.T) {
	// Create temporary CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	content := "a,b,c\n1,2,3\n4,5,6\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("create test file: %v", err)
	}

	stat, err := os.Stat(csvPath)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}

	// Valid index
	validIdx := &Index{
		Header: Header{
			FileSize:  stat.Size(),
			FileMtime: stat.ModTime().UnixNano(),
		},
	}

	if err := ValidateIndex(validIdx, csvPath); err != nil {
		t.Errorf("ValidateIndex() failed for valid index: %v", err)
	}

	// Invalid: wrong file size
	invalidSize := &Index{
		Header: Header{
			FileSize:  stat.Size() + 1000,
			FileMtime: stat.ModTime().UnixNano(),
		},
	}

	if err := ValidateIndex(invalidSize, csvPath); err == nil {
		t.Error("ValidateIndex() should fail for wrong file size")
	}

	// Modify file (append data)
	f, err := os.OpenFile(csvPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open file for append: %v", err)
	}
	f.WriteString("7,8,9\n")
	f.Close()

	// Now mtime should be different
	if err := ValidateIndex(validIdx, csvPath); err == nil {
		t.Error("ValidateIndex() should fail after file modification")
	}
}

// TestColumnTypeInference tests automatic type detection
func TestColumnTypeInference(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   ColumnType
	}{
		{
			name:   "all_numeric",
			values: []string{"1", "2", "3", "4.5", "100"},
			want:   ColumnTypeNumeric,
		},
		{
			name:   "all_strings",
			values: []string{"alice", "bob", "charlie", "dave"},
			want:   ColumnTypeString,
		},
		{
			name:   "mixed_mostly_numeric",
			values: []string{"1", "2", "3", "4", "5", "not_a_number"},
			want:   ColumnTypeNumeric, // 5/6 = 83% numeric
		},
		{
			name:   "mixed_mostly_strings",
			values: []string{"1", "2", "alice", "bob", "charlie"},
			want:   ColumnTypeString, // 2/5 = 40% numeric
		},
		{
			name:   "with_empty_values",
			values: []string{"", "1", "", "2", "3", ""},
			want:   ColumnTypeNumeric, // 3 non-empty, all numeric
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inferColumnType(tt.values)
			if got != tt.want {
				t.Errorf("inferColumnType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRealOffsetTracking verifies byte offsets match actual file positions
func TestRealOffsetTracking(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	// Create CSV with known structure
	var sb strings.Builder
	sb.WriteString("id,name\n")

	for i := 1; i <= 200; i++ { // 200 rows to ensure >100 samples
		sb.WriteString("1,alice\n")
	}

	if err := os.WriteFile(csvPath, []byte(sb.String()), 0644); err != nil {
		t.Fatalf("create test file: %v", err)
	}

	// Build index with block size of 50
	builder := NewBuilder(50)
	idx, err := builder.BuildFromFile(csvPath)
	if err != nil {
		t.Fatalf("BuildFromFile: %v", err)
	}

	// Should have 4 blocks (50 rows each)
	if len(idx.Blocks) != 4 {
		t.Errorf("Expected 4 blocks, got %d", len(idx.Blocks))
	}

	t.Logf("File size: %d bytes", idx.Header.FileSize)
	for i, block := range idx.Blocks {
		t.Logf("Block %d: rows[%d,%d) offset[%d,%d)",
			i, block.StartRow, block.EndRow, block.StartOffset, block.EndOffset)
	}

	// Blocks should be contiguous
	for i := 1; i < len(idx.Blocks); i++ {
		if idx.Blocks[i].StartOffset != idx.Blocks[i-1].EndOffset {
			t.Errorf("Block %d StartOffset = %d, Block %d EndOffset = %d, should match",
				i, idx.Blocks[i].StartOffset, i-1, idx.Blocks[i-1].EndOffset)
		}
	}

	// Verify we can seek to block offsets
	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()

	// Seek to second block
	if _, err := f.Seek(int64(idx.Blocks[1].StartOffset), 0); err != nil {
		t.Fatalf("seek to block 1: %v", err)
	}

	// Read should give us a valid CSV row
	buf := make([]byte, 8)
	n, err := f.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("read after seek: %v, n=%d", err, n)
	}

	// Should read "1,alice\n" (or start of it)
	if buf[0] != '1' {
		t.Errorf("After seeking to block 1, read byte %v, want '1'", buf[0])
	}
}
