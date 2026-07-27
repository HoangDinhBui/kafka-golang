package storage

import (
	"encoding/binary"
	"errors"
	"io"
)

// ============================================================================
// STRUCT: Record
// Description: Represents a single message stored on disk.
// ============================================================================
type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// ============================================================================
// FUNCTION: Marshal
// Description: Converts a Record struct into a binary byte slice for disk storage.
// Output: []byte (binary data including the 4-byte length prefix)
// ============================================================================
func (r *Record) Marshal() ([]byte, error) {
	keyLen := int32(-1)
	if r.Key != nil {
		keyLen = int32(len(r.Key))
	}

	valueLen := int32(-1)
	if r.Value != nil {
		valueLen = int32(len(r.Value))
	}

	// ------------------------------------------------------------------------
	// Calculate the actual size of the Record
	// 8 bytes (Offset) + 8 bytes (Timestamp) + 4 bytes (KeyLen) + 4 bytes (ValueLen)
	// ------------------------------------------------------------------------
	recordSize := 8 + 8 + 4 + 4
	if keyLen > 0 {
		recordSize += int(keyLen)
	}
	if valueLen > 0 {
		recordSize += int(valueLen)
	}

	// Allocate buffer for the entire record (including the 4-byte length prefix)
	buf := make([]byte, 4+recordSize)

	// ------------------------------------------------------------------------
	// Write the Length field (first 4 bytes)
	// ------------------------------------------------------------------------
	binary.BigEndian.PutUint32(buf[0:4], uint32(recordSize))

	// ------------------------------------------------------------------------
	// Write metadata (Offset & Timestamp) at non-overlapping positions:
	// - Offset: bytes 4 to 11 (8 bytes)
	// - Timestamp: bytes 12 to 19 (8 bytes)
	// ------------------------------------------------------------------------
	binary.BigEndian.PutUint64(buf[4:12], r.Offset)
	binary.BigEndian.PutUint64(buf[12:20], uint64(r.Timestamp))

	// ------------------------------------------------------------------------
	// Write Key metadata & payload:
	// - KeyLength: bytes 20 to 23 (4 bytes)
	// - Key payload: starts from byte 24
	// ------------------------------------------------------------------------
	binary.BigEndian.PutUint32(buf[20:24], uint32(keyLen))
	curr := 24
	if keyLen > 0 {
		copy(buf[curr:curr+int(keyLen)], r.Key)
		curr += int(keyLen)
	}

	// ------------------------------------------------------------------------
	// Write Value metadata & payload:
	// - ValueLength: next 4 bytes
	// - Value payload: remaining bytes
	// ------------------------------------------------------------------------
	binary.BigEndian.PutUint32(buf[curr:curr+4], uint32(valueLen))
	curr += 4
	if valueLen > 0 {
		copy(buf[curr:curr+int(valueLen)], r.Value)
	}

	return buf, nil
}

// ============================================================================
// FUNCTION: ReadRecord
// Description: Reads binary data from an io.Reader and decodes it into a Record struct.
// Output: *Record (decoded struct), int64 (total bytes read), error
// ============================================================================
func ReadRecord(reader io.Reader) (*Record, int64, error) {
	// ------------------------------------------------------------------------
	// 1. Read the 4-byte length header to determine the size of the payload
	// ------------------------------------------------------------------------
	var lenBuf [4]byte
	_, err := io.ReadFull(reader, lenBuf[:])
	if err != nil {
		return nil, 0, err
	}
	recordSize := binary.BigEndian.Uint32(lenBuf[:])

	// ------------------------------------------------------------------------
	// 2. Read the entire remaining record payload based on recordSize
	// ------------------------------------------------------------------------
	recordBuf := make([]byte, recordSize)
	_, err = io.ReadFull(reader, recordBuf)
	if err != nil {
		return nil, 4, err
	}

	// ------------------------------------------------------------------------
	// 3. Parse binary data from the record buffer
	// ------------------------------------------------------------------------
	offset := binary.BigEndian.Uint64(recordBuf[0:8])
	timestamp := int64(binary.BigEndian.Uint64(recordBuf[8:16]))

	// Read Key
	keyLen := int32(binary.BigEndian.Uint32(recordBuf[16:20]))
	curr := 20

	var key []byte
	if keyLen >= 0 {
		key = make([]byte, keyLen)
		copy(key, recordBuf[curr:curr+int(keyLen)])
		curr += int(keyLen)
	}

	if curr+4 > len(recordBuf) {
		return nil, int64(4 + recordSize), errors.New("corrupted record: invalid buffer boundaries")
	}

	// Read Value
	valueLen := int32(binary.BigEndian.Uint32(recordBuf[curr : curr+4]))
	curr += 4

	var value []byte
	if valueLen >= 0 {
		if curr+int(valueLen) > len(recordBuf) {
			return nil, int64(4 + recordSize), errors.New("corrupted record: value size exceeds buffer")
		}
		value = make([]byte, valueLen)
		copy(value, recordBuf[curr:curr+int(valueLen)])
	}

	return &Record{
		Offset:    offset,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}, int64(4 + recordSize), nil
}


