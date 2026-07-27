package storage

import (
	"encoding/binary"
	"io"
	"os"
)

// ============================================================================
// CONSTANTS
// Description: The byte size of a single index entry (8-byte Offset + 8-byte Position).
// ============================================================================
const IndexEntrySize = 16

// ============================================================================
// STRUCT: Index
// Description: Manages writing and reading from a sparse segment index file.
// ============================================================================
type Index struct {
	file *os.File
	size int64
}

// ============================================================================
// FUNCTION: NewIndex
// Description: Opens an existing index file or creates a new one in read-write mode.
// ============================================================================
func NewIndex(filePath string) (*Index, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &Index{
		file: file,
		size: info.Size(),
	}, nil
}

// ============================================================================
// FUNCTION: WriteEntry
// Description: Appends a new index entry (Offset -> Byte Position) to the index.
// ============================================================================
func (idx *Index) WriteEntry(offset uint64, position int64) error {
	var buf [IndexEntrySize]byte
	binary.BigEndian.PutUint64(buf[0:8], offset)
	binary.BigEndian.PutUint64(buf[8:16], uint64(position))

	_, err := idx.file.Write(buf[:])
	if err != nil {
		return err
	}

	idx.size += IndexEntrySize
	return nil
}

// ============================================================================
// FUNCTION: Lookup
// Description: Searches the index for the largest offset less than or equal to 
//              targetOffset. Uses a disk-based binary search on the index file.
// Output: The byte position in the log file, or 0 if the index is empty or target offset is not found.
// ============================================================================
func (idx *Index) Lookup(targetOffset uint64) (int64, error) {
	if idx.size == 0 {
		return 0, nil
	}

	entriesCount := idx.size / IndexEntrySize
	if entriesCount == 0 {
		return 0, nil
	}

	low := int64(0)
	high := entriesCount - 1
	var resultPos int64 = 0

	var buf [IndexEntrySize]byte

	// Binary search
	for low <= high {
		mid := low + (high-low)/2

		// Read index entry at index 'mid' directly from disk
		_, err := idx.file.ReadAt(buf[:], mid*IndexEntrySize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		midOffset := binary.BigEndian.Uint64(buf[0:8])
		midPosition := int64(binary.BigEndian.Uint64(buf[8:16]))

		if midOffset <= targetOffset {
			// Save position candidate and search right half for a potentially higher matching offset
			resultPos = midPosition
			low = mid + 1
		} else {
			// Search left half
			high = mid - 1
		}
	}

	return resultPos, nil
}

// ============================================================================
// FUNCTION: Close
// Description: Closes the underlying index file handle.
// ============================================================================
func (idx *Index) Close() error {
	return idx.file.Close()
}
