package storage

import (
	"time"
)

const (
	ControlMarkerCommit byte = 0x01
	ControlMarkerAbort  byte = 0x02
)

// ============================================================================
// FUNCTION: NewControlRecord
// Description: Constructs a special control record (COMMIT or ABORT marker)
//              appended to segment log upon transaction completion.
// ============================================================================
func NewControlRecord(markerType byte) *Record {
	return &Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("__control_marker__"),
		Value:     []byte{markerType},
	}
}

// IsControlRecord checks if a record is a transaction control marker
func IsControlRecord(r *Record) bool {
	return r != nil && string(r.Key) == "__control_marker__"
}
