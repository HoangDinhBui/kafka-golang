package ui

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
)

const magicGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// ============================================================================
// STRUCT: WSHub
// Description: Zero-dependency WebSocket connection manager and broadcast hub.
// ============================================================================
type WSHub struct {
	mu      sync.RWMutex
	conns   map[net.Conn]struct{}
}

// NewWSHub initializes a new WebSocket Hub.
func NewWSHub() *WSHub {
	return &WSHub{
		conns: make(map[net.Conn]struct{}),
	}
}

// Upgrade performs RFC 6455 WebSocket handshake and hijacks net.Conn.
func (h *WSHub) Upgrade(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "Not a websocket handshake", http.StatusBadRequest)
		return nil, errors.New("invalid upgrade header")
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return nil, errors.New("missing websocket key")
	}

	// Calculate Sec-WebSocket-Accept
	hSha := sha1.New()
	hSha.Write([]byte(key + magicGUID))
	acceptKey := base64.StdEncoding.EncodeToString(hSha.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Webserver doesn't support hijacking", http.StatusInternalServerError)
		return nil, errors.New("hijack unsupported")
	}

	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	// Send 101 Switching Protocols response
	resp := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: %s\r\n\r\n", acceptKey)

	if _, err := rw.WriteString(resp); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	h.mu.Lock()
	h.conns[conn] = struct{}{}
	h.mu.Unlock()

	// Launch background reader to handle disconnects/ping-pong
	go h.readLoop(conn, rw)

	return conn, nil
}

// Broadcast sends a JSON or text payload to all active connected WS clients.
func (h *WSHub) Broadcast(payload []byte) {
	frame := encodeTextFrame(payload)

	h.mu.RLock()
	conns := make([]net.Conn, 0, len(h.conns))
	for conn := range h.conns {
		conns = append(conns, conn)
	}
	h.mu.RUnlock()

	for _, conn := range conns {
		if _, err := conn.Write(frame); err != nil {
			h.removeConn(conn)
		}
	}
}

// Client count returns number of active WS subscribers.
func (h *WSHub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns)
}

func (h *WSHub) removeConn(conn net.Conn) {
	h.mu.Lock()
	delete(h.conns, conn)
	h.mu.Unlock()
	_ = conn.Close()
}

func (h *WSHub) readLoop(conn net.Conn, rw *bufio.ReadWriter) {
	defer h.removeConn(conn)
	buf := make([]byte, 1024)
	for {
		_, err := rw.Read(buf)
		if err != nil {
			if err == io.EOF || errors.Is(err, net.ErrClosed) {
				break
			}
			break
		}
	}
}

// encodeTextFrame constructs an unmasked WS server-to-client Text Frame (Opcode 0x1).
func encodeTextFrame(payload []byte) []byte {
	length := len(payload)
	var header []byte

	if length <= 125 {
		header = []byte{0x81, byte(length)}
	} else if length <= 65535 {
		header = []byte{0x81, 126, byte(length >> 8), byte(length & 0xFF)}
	} else {
		header = []byte{
			0x81, 127,
			byte(length >> 56), byte(length >> 48), byte(length >> 40), byte(length >> 32),
			byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length & 0xFF),
		}
	}

	return append(header, payload...)
}
