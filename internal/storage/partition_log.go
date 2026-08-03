package storage

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ============================================================================
// STRUCT: PartitionLog
// Description: Manages a sequence of ordered segments representing a single partition.
//
//	Provides thread-safe operations for appending and reading records.
//
// ============================================================================
type PartitionLog struct {
	dir                string       // Path to folder (./data/orders-topic-8/)
	mu                 sync.RWMutex // Key Read/Write RWMutex to ensure Thread-Safety
	activeSegment      *Segment     // Present segment is opening to write newest info
	segments           []*Segment   // List all segments are sorted by baseOffset
	maxSegmentBytes    int64        // Max size of 1 segment
	indexIntervalBytes int64        // Distance between index writes
}

// ============================================================================
// FUNCTION: NewPartitionLog
// Description: Opens or initializes a partition log directory, loading existing segments.
// ============================================================================
func NewPartitionLog(dir string, maxSegmentBytes int64, indexIntervalBytes int64) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	pl := &PartitionLog{
		dir:                dir,
		maxSegmentBytes:    maxSegmentBytes,
		indexIntervalBytes: indexIntervalBytes,
	}

	if err := pl.loadSegments(); err != nil {
		return nil, err
	}

	return pl, nil
}

// ============================================================================
// FUNCTION: Append
// Description: Thread-safely appends a record to the active segment, rolling to a
// new segment if the current active segment reaches maxSegmentBytes.
// ============================================================================
func (p *PartitionLog) Append(record *Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if active segment is full; if so, roll over to a new segment
	if p.activeSegment.IsFull() {
		if err := p.roll(); err != nil {
			return err
		}
	}

	return p.activeSegment.Append(record)
}

// ============================================================================
// FUNCTION: Read
// Description: Thread-safely reads records across segments starting from startOffset.
// ============================================================================
func (p *PartitionLog) Read(startOffset uint64) ([]*Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var records []*Record

	// Find the index of the first segment that could contain startOffset
	startIdx := p.findSegmentIndex(startOffset)
	if startIdx == -1 {
		return nil, nil
	}

	// Read records across matching segments sequentially
	for i := startIdx; i < len(p.segments); i++ {
		segRecords, err := p.segments[i].Read(startOffset)
		if err != nil {
			return nil, err
		}
		records = append(records, segRecords...)
	}

	return records, nil
}

// ============================================================================
// FUNCTION: ReadZeroCopy
// Description: Thread-safely streams raw segment bytes starting from startOffset
//              directly to target socket writer w without intermediate memory allocations.
// ============================================================================
func (p *PartitionLog) ReadZeroCopy(startOffset uint64, maxBytes int64, w io.Writer) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	startIdx := p.findSegmentIndex(startOffset)
	if startIdx == -1 {
		return 0, nil
	}

	var totalTransferred int64
	remainingBytes := maxBytes

	for i := startIdx; i < len(p.segments); i++ {
		if maxBytes > 0 && remainingBytes <= 0 {
			break
		}

		n, err := p.segments[i].ReadZeroCopy(startOffset, remainingBytes, w)
		totalTransferred += n
		if err != nil {
			return totalTransferred, err
		}

		if maxBytes > 0 {
			remainingBytes -= n
		}
	}

	return totalTransferred, nil
}

// ============================================================================
// FUNCTION: Close
// Description: Thread-safely flushes and closes all underlying segment files.
// ============================================================================
func (p *PartitionLog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================================
// PRIVATE METHOD: loadSegments
// Description: Discovers existing segment files in the directory and restores them.
// ============================================================================
func (p *PartitionLog) loadSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			baseStr := strings.TrimSuffix(entry.Name(), ".log")
			baseOffset, err := strconv.ParseUint(baseStr, 10, 64)
			if err == nil {
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// If directory has no existing segments, initialize with baseOffset 0
	if len(baseOffsets) == 0 {
		baseOffsets = append(baseOffsets, 0)
	}

	for _, baseOffset := range baseOffsets {
		seg, err := NewSegment(p.dir, baseOffset, p.maxSegmentBytes, p.indexIntervalBytes)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}

	p.activeSegment = p.segments[len(p.segments)-1]
	return nil
}

// ============================================================================
// PRIVATE METHOD: roll
// Description: Creates a new active segment starting at the next offset.
// ============================================================================
func (p *PartitionLog) roll() error {
	newBaseOffset := p.activeSegment.nextOffset
	newSeg, err := NewSegment(p.dir, newBaseOffset, p.maxSegmentBytes, p.indexIntervalBytes)
	if err != nil {
		return err
	}

	p.segments = append(p.segments, newSeg)
	p.activeSegment = newSeg
	return nil
}

// ============================================================================
// PRIVATE METHOD: findSegmentIndex
// Description: Returns the index of the segment containing or preceding startOffset.
// ============================================================================
func (p *PartitionLog) findSegmentIndex(startOffset uint64) int {
	if len(p.segments) == 0 {
		return -1
	}

	// Binary search to find the segment with baseOffset <= startOffset
	low, high := 0, len(p.segments)-1
	result := -1

	for low <= high {
		mid := low + (high-low)/2
		if p.segments[mid].baseOffset <= startOffset {
			result = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return result
}

// ============================================================================
// PUBLIC METHOD: LEO
// Description: Thread-safely returns the current Log End Offset (nextOffset).
// ============================================================================
func (p *PartitionLog) LEO() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.activeSegment == nil {
		return 0
	}
	return p.activeSegment.nextOffset
}

// ============================================================================
// PUBLIC METHOD: BaseOffset
// Description: Thread-safely returns the base offset of the first segment.
// ============================================================================
func (p *PartitionLog) BaseOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.segments) == 0 {
		return 0
	}
	return p.segments[0].baseOffset
}

// ============================================================================
// PUBLIC METHOD: Stats
// Description: Thread-safely returns total segment count and total size in bytes.
// ============================================================================
func (p *PartitionLog) Stats() (int, int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var totalSize int64
	for _, seg := range p.segments {
		totalSize += seg.currentSize
	}
	return len(p.segments), totalSize
}

// ============================================================================
// PUBLIC METHOD: GetNonActiveSegments
// Description: Thread-safely returns all closed (non-active) segment pointers.
// ============================================================================
func (p *PartitionLog) GetNonActiveSegments() []*Segment {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.segments) <= 1 {
		return nil
	}
	result := make([]*Segment, len(p.segments)-1)
	copy(result, p.segments[:len(p.segments)-1])
	return result
}

// ============================================================================
// PUBLIC METHOD: RemoveOldestSegment
// Description: Thread-safely removes and deletes the oldest non-active segment.
// ============================================================================
func (p *PartitionLog) RemoveOldestSegment() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segments) <= 1 {
		return false, nil
	}

	oldestSeg := p.segments[0]
	p.segments = p.segments[1:]

	err := oldestSeg.RemoveFiles()
	return true, err
}

// ============================================================================
// PUBLIC METHOD: Dir
// Description: Thread-safely returns directory path of the partition log.
// ============================================================================
func (p *PartitionLog) Dir() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dir
}


