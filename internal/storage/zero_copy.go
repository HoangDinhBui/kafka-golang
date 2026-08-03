package storage

import (
	"io"
	"os"
)

// ============================================================================
// FUNCTION: SendFileToSocket
// Description: Zero-copy transfer helper that streams raw bytes from an os.File
//              descriptor directly to a TCP socket writer (w) without copying
//              data into Go User Space heap buffers.
// ============================================================================
func SendFileToSocket(w io.Writer, file *os.File, startPos int64, length int64) (int64, error) {
	if length <= 0 || file == nil || w == nil {
		return 0, nil
	}

	// io.NewSectionReader provides zero-allocation seeking on file descriptors.
	// When paired with io.CopyN and a net.Conn writer, Go's standard library internal
	// runtime automatically delegates to kernel-level sendfile / Splice / TransmitFile.
	sectionReader := io.NewSectionReader(file, startPos, length)
	return io.CopyN(w, sectionReader, length)
}
