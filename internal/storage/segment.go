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

// ============================================================================
// FUNCTION: replaceContents
// Description: Atomically rewrites this segment's own .log/.index files to
//              contain exactly the given records — each keeping its
//              original Offset, since compaction only ever removes records
//              and must never renumber survivors — then reopens the
//              segment's file handles against the new files. Used by
//              CompactLogSegments.
//
//              dir MUST be this segment's own directory: the rewritten
//              files replace seg's originals in place, they are not
//              written out as a separate copy elsewhere.
//
//              The caller MUST hold a lock excluding concurrent Read/
//              ReadZeroCopy/Append against this segment for the entire
//              call (PartitionLog.CompactSegments does this) — s.Close()
//              below invalidates the segment's file handles until they are
//              reopened a few lines later.
//
//              Known limitation: the log and index files are swapped in via
//              two separate os.Rename calls, which cannot be made atomic
//              together. If the process is interrupted between them, the
//              segment could reopen with a stale index paired to the new
//              (shorter) log file. This fails safe — reads would surface a
//              decode error rather than silently return wrong data — and is
//              an accepted, documented limitation for what is an explicitly
//              opt-in background maintenance feature (see CleanerConfig.
//              CompactionEnabled), not a correctness guarantee for the
//              broker's primary write path.
// ============================================================================
func (s *Segment) replaceContents(dir string, records []*Record) error {
	baseName := fmt.Sprintf("%020d", s.baseOffset)
	logPath := filepath.Join(dir, baseName+".log")
	indexPath := filepath.Join(dir, baseName+".index")
	tmpLogPath := logPath + ".compact.tmp"
	tmpIndexPath := indexPath + ".compact.tmp"

	// Remove any stale temp files left behind by a previously interrupted attempt.
	_ = os.Remove(tmpLogPath)
	_ = os.Remove(tmpIndexPath)

	tmpLog, err := os.OpenFile(tmpLogPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	tmpIndex, err := NewIndex(tmpIndexPath)
	if err != nil {
		_ = tmpLog.Close()
		_ = os.Remove(tmpLogPath)
		return err
	}

	swapped := false
	defer func() {
		if !swapped {
			_ = tmpLog.Close()
			_ = tmpIndex.Close()
			_ = os.Remove(tmpLogPath)
			_ = os.Remove(tmpIndexPath)
		}
	}()

	var size int64
	var lastIndexBytes int64
	for i, rec := range records {
		data, err := rec.Marshal()
		if err != nil {
			return err
		}

		// Mirror Segment.Append's own sparse-index cadence: an entry for
		// the first record, then one whenever indexIntervalBytes have
		// accumulated since the last entry.
		if i == 0 || size-lastIndexBytes >= s.indexIntervalBytes {
			if err := tmpIndex.WriteEntry(rec.Offset, size); err != nil {
				return err
			}
			lastIndexBytes = size
		}

		if _, err := tmpLog.Write(data); err != nil {
			return err
		}
		size += int64(len(data))
	}

	if err := tmpLog.Sync(); err != nil {
		return err
	}
	if err := tmpLog.Close(); err != nil {
		return err
	}
	if err := tmpIndex.Close(); err != nil {
		return err
	}

	// Close the segment's current handles before replacing the files they
	// point to — required on Windows, where an open file cannot be renamed
	// over or replaced.
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpLogPath, logPath); err != nil {
		return err
	}
	if err := os.Rename(tmpIndexPath, indexPath); err != nil {
		return err
	}
	swapped = true

	newLogFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	newIndex, err := NewIndex(indexPath)
	if err != nil {
		_ = newLogFile.Close()
		return err
	}

	s.logFile = newLogFile
	s.index = newIndex
	s.currentSize = size
	s.lastIndexBytes = lastIndexBytes
	// baseOffset and nextOffset are intentionally left unchanged: compaction
	// only removes records, it never changes the offset range this segment
	// covers or renumbers the records that survive.

	return nil
}

