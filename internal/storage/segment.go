package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ============================================================================
// STRUCT: Segment
// Description: Manages a single pair of .log and .index files for a partition.
// ============================================================================
type Segment struct {
	baseOffset         uint64
	nextOffset         uint64
	logFile            *os.File
	index              *Index
	currentSize        int64
	maxBytes           int64
	indexIntervalBytes int64
	lastIndexBytes     int64
}

// ============================================================================
// FUNCTION: NewSegment
// Description: Creates or opens a segment in the partition directory.
// ============================================================================
func NewSegment(dir string, baseOffset uint64, maxBytes int64, indexIntervalBytes int64) (*Segment, error) {
	// Format segment names with a 20-digit zero-padded prefix (e.g., 00000000000000000000.log)
	baseName := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, baseName+".log")
	indexPath := filepath.Join(dir, baseName+".index")

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	index, err := NewIndex(indexPath)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	segment := &Segment{
		baseOffset:         baseOffset,
		nextOffset:         baseOffset,
		logFile:            logFile,
		index:              index,
		maxBytes:           maxBytes,
		indexIntervalBytes: indexIntervalBytes,
	}

	// Recover segment state by scanning the log file
	if err := segment.recoverState(); err != nil {
		_ = segment.Close()
		return nil, err
	}

	return segment, nil
}

// ============================================================================
// FUNCTION: Append
// Description: Appends a new Record to the log file and conditionally writes
//              an entry to the index file based on indexIntervalBytes.
// ============================================================================
func (s *Segment) Append(record *Record) error {
	record.Offset = s.nextOffset

	data, err := record.Marshal()
	if err != nil {
		return err
	}

	// Determine if we should append to the sparse index
	// We append if it's the first record in the segment, or if we have written indexIntervalBytes since the last index write
	if s.currentSize == 0 || s.currentSize-s.lastIndexBytes >= s.indexIntervalBytes {
		err = s.index.WriteEntry(record.Offset, s.currentSize)
		if err != nil {
			return err
		}
		s.lastIndexBytes = s.currentSize
	}

	// Write record binary bytes to the log file
	_, err = s.logFile.Write(data)
	if err != nil {
		return err
	}

	s.currentSize += int64(len(data))
	s.nextOffset++
	return nil
}

// ============================================================================
// FUNCTION: Read
// Description: Reads records starting from targetOffset up to the end of the segment.
// ============================================================================
func (s *Segment) Read(startOffset uint64) ([]*Record, error) {
	if startOffset >= s.nextOffset {
		return nil, nil
	}

	// 1. Locate starting byte position using index
	pos, err := s.index.Lookup(startOffset)
	if err != nil {
		return nil, err
	}

	// 2. Seek to position and read sequentially
	_, err = s.logFile.Seek(pos, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var records []*Record
	for {
		record, _, err := ReadRecord(s.logFile)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		if record.Offset >= startOffset {
			records = append(records, record)
		}
	}

	return records, nil
}

// ============================================================================
// FUNCTION: ReadZeroCopy
// Description: Streams raw segment bytes starting from targetOffset directly to
//              socket writer w without heap memory allocations.
// ============================================================================
func (s *Segment) ReadZeroCopy(startOffset uint64, maxBytes int64, w io.Writer) (int64, error) {
	if startOffset >= s.nextOffset || maxBytes <= 0 {
		return 0, nil
	}

	// 1. Locate starting byte position using index
	startPos, err := s.index.Lookup(startOffset)
	if err != nil {
		return 0, err
	}

	// 2. Calculate available byte length from startPos to end of segment
	availableBytes := s.currentSize - startPos
	if availableBytes <= 0 {
		return 0, nil
	}

	transferLength := availableBytes
	if maxBytes > 0 && maxBytes < availableBytes {
		transferLength = maxBytes
	}

	// 3. Stream zero-copy bytes directly to target socket writer
	return SendFileToSocket(w, s.logFile, startPos, transferLength)
}

// ============================================================================
// FUNCTION: IsFull
// Description: Returns true if the segment has exceeded its maximum byte limit.
// ============================================================================
func (s *Segment) IsFull() bool {
	return s.currentSize >= s.maxBytes
}

// ============================================================================
// FUNCTION: Close
// Description: Flushes to disk and closes both log and index file handles.
// ============================================================================
func (s *Segment) Close() error {
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	return s.index.Close()
}

// ============================================================================
// PRIVATE METHOD: recoverState
// Description: Scans the log file sequentially to compute nextOffset and currentSize.
// ============================================================================
func (s *Segment) recoverState() error {
	_, err := s.logFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var size int64 = 0
	var lastOffset int64 = -1

	for {
		_, bytesRead, err := ReadRecord(s.logFile)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		lastOffset = int64(s.nextOffset)
		s.nextOffset++
		size += bytesRead
	}

	s.currentSize = size
	if lastOffset == -1 {
		s.nextOffset = s.baseOffset
	}

	return nil
}

// ============================================================================
// PUBLIC GETTERS & HELPERS FOR SEGMENT CLEANING
// ============================================================================
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

func (s *Segment) NextOffset() uint64 {
	return s.nextOffset
}

func (s *Segment) Size() int64 {
	return s.currentSize
}

func (s *Segment) LogFilePath() string {
	if s.logFile != nil {
		return s.logFile.Name()
	}
	return ""
}

func (s *Segment) RemoveFiles() error {
	_ = s.Close()

	if s.logFile != nil {
		logPath := s.logFile.Name()
		indexPath := strings.TrimSuffix(logPath, ".log") + ".index"
		_ = os.Remove(logPath)
		_ = os.Remove(indexPath)
	}
	return nil
}

