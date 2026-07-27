package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
)

// ============================================================================
// STRUCT: ConnectionHandler
// Description: Manages the lifecycle and binary framing of a single TCP client connection.
// ============================================================================
type ConnectionHandler struct {
	conn    net.Conn // Active TCP client socket connection
	handler *Handler // Reference to API request handler
}

// ============================================================================
// FUNCTION: NewConnectionHandler
// Description: Instantiates a new ConnectionHandler for a TCP client socket.
// ============================================================================
func NewConnectionHandler(conn net.Conn, handler *Handler) *ConnectionHandler {
	return &ConnectionHandler{
		conn:    conn,
		handler: handler,
	}
}

// ============================================================================
// FUNCTION: Handle
// Description: Runs the read/write loop for the TCP connection until disconnected.
// ============================================================================
func (c *ConnectionHandler) Handle() {
	defer c.conn.Close()

	for {
		// 1. Read 4-byte Request Size header
		var sizeBuf [4]byte
		_, err := io.ReadFull(c.conn, sizeBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // Client disconnected gracefully
			}
			break
		}

		requestSize := binary.BigEndian.Uint32(sizeBuf[:])
		if requestSize == 0 {
			continue
		}

		// 2. Read the entire request payload into memory
		requestPayload := make([]byte, requestSize)
		_, err = io.ReadFull(c.conn, requestPayload)
		if err != nil {
			break
		}

		reqReader := bytes.NewReader(requestPayload)

		// 3. Decode RequestHeader
		header, err := protocol.DecodeRequestHeader(reqReader)
		if err != nil {
			break
		}

		// 4. Execute request logic and capture response payload
		respBodyBuf := new(bytes.Buffer)
		if err := c.handler.HandleRequest(header, reqReader, respBodyBuf); err != nil {
			break
		}

		// 5. Construct Response Header & Frame
		respHeaderBuf := new(bytes.Buffer)
		respHeader := &protocol.ResponseHeader{
			CorrelationId: header.CorrelationId,
		}
		_ = protocol.EncodeResponseHeader(respHeaderBuf, respHeader)

		// Total response payload = Header bytes + Body bytes
		totalRespSize := respHeaderBuf.Len() + respBodyBuf.Len()

		// 6. Write 4-byte Response Size followed by Response Payload to socket
		var respSizeBuf [4]byte
		binary.BigEndian.PutUint32(respSizeBuf[:], uint32(totalRespSize))

		if _, err := c.conn.Write(respSizeBuf[:]); err != nil {
			break
		}
		if _, err := c.conn.Write(respHeaderBuf.Bytes()); err != nil {
			break
		}
		if _, err := c.conn.Write(respBodyBuf.Bytes()); err != nil {
			break
		}
	}
}
