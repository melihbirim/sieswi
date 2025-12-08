package sidx

import (
	"encoding/binary"
	"fmt"
	"io"
)

// File format:
// Header (variable size):
//   - Magic: "SIDX" (4 bytes)
//   - Version: uint32 (4 bytes)
//   - BlockSize: uint32 (4 bytes) - rows per block
//   - NumBlocks: uint32 (4 bytes)
//   - FileSize: int64 (8 bytes) - source CSV file size
//   - FileMtime: int64 (8 bytes) - source CSV modification time (Unix nanos)
//   - NumColumns: uint32 (4 bytes) - column count in dictionary
//   - For each column in dictionary:
//     - NameLen: uint32 (4 bytes)
//     - Name: string (NameLen bytes)
//     - Type: uint8 (1 byte) - 0=string, 1=numeric
//
// For each block:
//   - StartRow: uint64 (8 bytes)
//   - EndRow: uint64 (8 bytes)
//   - StartOffset: uint64 (8 bytes) - actual byte position in CSV
//   - EndOffset: uint64 (8 bytes) - actual byte position in CSV
//   - For each column (order matches dictionary):
//     - MinLen: uint32 (4 bytes)
//     - Min: string (MinLen bytes)
//     - MaxLen: uint32 (4 bytes)
//     - Max: string (MaxLen bytes)

const (
	Magic      = "SIDX"
	Version    = 3     // Bumped to add EmptyCount to ColumnStats
	BlockSize  = 65536 // 64K rows per block
	HeaderSize = 32    // Base size without column dictionary
)

type ColumnType uint8

const (
	ColumnTypeString  ColumnType = 0
	ColumnTypeNumeric ColumnType = 1
)

type ColumnInfo struct {
	Name string
	Type ColumnType
}

type Header struct {
	Magic     [4]byte
	Version   uint32
	BlockSize uint32
	NumBlocks uint32
	FileSize  int64        // Source CSV size for validation
	FileMtime int64        // Source CSV mtime (Unix nanos) for validation
	Columns   []ColumnInfo // Column dictionary
}

type ColumnStats struct {
	Min        string // String representation, compared per column type
	Max        string
	EmptyCount uint32 // Number of empty/null values in this column for this block
}

type BlockMeta struct {
	StartRow    uint64        // First row in block (0-indexed)
	EndRow      uint64        // Last row in block (exclusive)
	StartOffset uint64        // Actual byte offset in CSV file
	EndOffset   uint64        // Actual byte offset in CSV file
	Columns     []ColumnStats // Order matches Header.Columns dictionary
}

type Index struct {
	Header Header
	Blocks []BlockMeta
}

func WriteIndex(w io.Writer, idx *Index) error {
	// Write header
	if _, err := w.Write([]byte(Magic)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Header.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Header.BlockSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Header.NumBlocks); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Header.FileSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Header.FileMtime); err != nil {
		return err
	}

	// Write column dictionary
	if err := binary.Write(w, binary.LittleEndian, uint32(len(idx.Header.Columns))); err != nil {
		return err
	}
	for _, col := range idx.Header.Columns {
		if err := binary.Write(w, binary.LittleEndian, uint32(len(col.Name))); err != nil {
			return err
		}
		if _, err := w.Write([]byte(col.Name)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint8(col.Type)); err != nil {
			return err
		}
	}

	// Write blocks (no column names, just stats)
	for _, block := range idx.Blocks {
		if err := binary.Write(w, binary.LittleEndian, block.StartRow); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, block.EndRow); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, block.StartOffset); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, block.EndOffset); err != nil {
			return err
		}

		// Write stats for each column (order matches dictionary)
		for _, col := range block.Columns {
			// Min value
			if err := binary.Write(w, binary.LittleEndian, uint32(len(col.Min))); err != nil {
				return err
			}
			if _, err := w.Write([]byte(col.Min)); err != nil {
				return err
			}

			// Max value
			if err := binary.Write(w, binary.LittleEndian, uint32(len(col.Max))); err != nil {
				return err
			}
			if _, err := w.Write([]byte(col.Max)); err != nil {
				return err
			}

			// Empty count
			if err := binary.Write(w, binary.LittleEndian, col.EmptyCount); err != nil {
				return err
			}
		}
	}

	return nil
}

func ReadIndex(r io.Reader) (*Index, error) {
	idx := &Index{}

	// Read header
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != Magic {
		return nil, fmt.Errorf("invalid magic: %s", magic)
	}
	copy(idx.Header.Magic[:], magic)

	if err := binary.Read(r, binary.LittleEndian, &idx.Header.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &idx.Header.BlockSize); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &idx.Header.NumBlocks); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &idx.Header.FileSize); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &idx.Header.FileMtime); err != nil {
		return nil, err
	}

	// Read column dictionary
	var numColumns uint32
	if err := binary.Read(r, binary.LittleEndian, &numColumns); err != nil {
		return nil, err
	}
	idx.Header.Columns = make([]ColumnInfo, numColumns)
	for i := uint32(0); i < numColumns; i++ {
		var nameLen uint32
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return nil, err
		}
		idx.Header.Columns[i].Name = string(nameBuf)

		var colType uint8
		if err := binary.Read(r, binary.LittleEndian, &colType); err != nil {
			return nil, err
		}
		idx.Header.Columns[i].Type = ColumnType(colType)
	}

	// Read blocks (stats only, no column names)
	idx.Blocks = make([]BlockMeta, idx.Header.NumBlocks)
	for i := uint32(0); i < idx.Header.NumBlocks; i++ {
		block := &idx.Blocks[i]

		if err := binary.Read(r, binary.LittleEndian, &block.StartRow); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &block.EndRow); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &block.StartOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &block.EndOffset); err != nil {
			return nil, err
		}

		// Read stats for each column (order matches dictionary)
		block.Columns = make([]ColumnStats, numColumns)
		for j := uint32(0); j < numColumns; j++ {
			col := &block.Columns[j]

			// Read min value
			var minLen uint32
			if err := binary.Read(r, binary.LittleEndian, &minLen); err != nil {
				return nil, err
			}
			minBuf := make([]byte, minLen)
			if _, err := io.ReadFull(r, minBuf); err != nil {
				return nil, err
			}
			col.Min = string(minBuf)

			// Read max value
			var maxLen uint32
			if err := binary.Read(r, binary.LittleEndian, &maxLen); err != nil {
				return nil, err
			}
			maxBuf := make([]byte, maxLen)
			if _, err := io.ReadFull(r, maxBuf); err != nil {
				return nil, err
			}
			col.Max = string(maxBuf)

			// Read empty count (version 3+)
			if idx.Header.Version >= 3 {
				if err := binary.Read(r, binary.LittleEndian, &col.EmptyCount); err != nil {
					return nil, err
				}
			}
		}
	}

	return idx, nil
}
