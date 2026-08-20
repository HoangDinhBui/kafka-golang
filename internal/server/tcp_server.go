package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
)

// ============================================================================
// STRUCT: TCPServer
// Description: Manages the TCP listener lifecycle and spawns connection handlers.
// ============================================================================
type TCPServer struct {
	addr      string         // TCP binding address (e.g., ":9092" or "127.0.0.1:9092")
	handler   *Handler       // Reference to request router handler
	listener  net.Listener   // Active TCP network listener
	tlsConfig *tls.Config    // Optional TLS config; when set, Start() binds a TLS listener instead of plaintext TCP
	quit      chan struct{}  // Signal channel for graceful shutdown
	wg        sync.WaitGroup // WaitGroup tracking active client connection routines
	mu        sync.RWMutex   // Mutex protecting listener access
}

// ============================================================================
// FUNCTION: NewTCPServer
// Description: Instantiates a new TCPServer given an address and request Handler.
// ============================================================================
func NewTCPServer(addr string, handler *Handler) *TCPServer {
	return &TCPServer{
		addr:    addr,
		handler: handler,
		quit:    make(chan struct{}),
	}
}

// SetTLSConfig enables TLS on the listener bound by the next Start() call.
// Must be called before Start(); has no effect on an already-bound listener.
func (s *TCPServer) SetTLSConfig(cfg *tls.Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tlsConfig = cfg
}

// ============================================================================
// FUNCTION: Start
// Description: Binds to the TCP port and listens for client connections in a loop.
// ============================================================================
func (s *TCPServer) Start() error {
	s.mu.Lock()
	if s.listener == nil {
		var listener net.Listener
		var err error
		if s.tlsConfig != nil {
			listener, err = tls.Listen("tcp", s.addr, s.tlsConfig)
		} else {
			listener, err = net.Listen("tcp", s.addr)
		}
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("failed to bind to TCP address %s: %w", s.addr, err)
		}
		s.listener = listener
	}
	l := s.listener
	s.mu.Unlock()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil // Graceful shutdown initiated
			default:
				return err
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			connHandler := NewConnectionHandler(c, s.handler)
			connHandler.Handle()
		}(conn)
	}
}

// ============================================================================
// FUNCTION: Addr
// Description: Returns the net.Addr of the active listener.
// ============================================================================
func (s *TCPServer) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// ============================================================================
// FUNCTION: Stop
// Description: Gracefully closes the TCP listener and waits for active connections.
// ============================================================================
func (s *TCPServer) Stop() error {
	close(s.quit)
	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}
