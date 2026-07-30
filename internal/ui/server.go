package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/server"
)

//go:embed static/*
var staticFS embed.FS

// ============================================================================
// STRUCT: UIServer
// Description: Embedded HTTP API Server & Real-time Web UI Dashboard.
// ============================================================================
type UIServer struct {
	port       int
	handler    *server.Handler
	collector  *TelemetryCollector
	wsHub      *WSHub
	httpServer *http.Server
}

// NewServer initializes the UI HTTP Server on the specified management port.
func NewServer(handler *server.Handler, port int) *UIServer {
	collector := NewTelemetryCollector()
	wsHub := NewWSHub()
	handler.SetTelemetryListener(collector)

	ui := &UIServer{
		port:      port,
		handler:   handler,
		collector: collector,
		wsHub:     wsHub,
	}

	mux := http.NewServeMux()

	// REST API Endpoints
	mux.HandleFunc("/api/v1/cluster", ui.handleCluster)
	mux.HandleFunc("/api/v1/topics", ui.handleTopics)
	mux.HandleFunc("/api/v1/topics/", ui.handleTopicMessages) // /api/v1/topics/{name}/messages
	mux.HandleFunc("/api/v1/groups", ui.handleGroups)
	mux.HandleFunc("/api/v1/metrics", ui.handleMetrics)

	// WebSocket Endpoint
	mux.HandleFunc("/ws/stream", ui.handleWebSocket)

	// Static Web Frontend Assets (embedded index.html, app.js, styles.css)
	staticSubFS, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("Failed to load embedded static UI files: %v", err)
	}
	fileServer := http.FileServer(http.FS(staticSubFS))
	mux.Handle("/", fileServer)

	ui.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Launch background telemetry stream to WebSocket clients
	go ui.wsStreamLoop()

	return ui
}

// Start runs the HTTP management server in the current goroutine.
func (s *UIServer) Start() error {
	log.Printf("[Web UI Server] Dashboard listening on http://localhost:%d\n", s.port)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop gracefully shuts down the HTTP management server.
func (s *UIServer) Stop() error {
	return s.httpServer.Close()
}

// GetTelemetry returns the underlying TelemetryCollector instance.
func (s *UIServer) GetTelemetry() *TelemetryCollector {
	return s.collector
}

// REST HANDLER: GET /api/v1/cluster
func (s *UIServer) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	topics := GetTopicsSummary(s.handler)
	var totalTopics = len(topics)
	var totalPartitions = 0
	var totalMessages uint64 = 0
	var totalBytes int64 = 0

	for _, t := range topics {
		totalPartitions += t.PartitionsCount
		totalMessages += t.TotalMessages
		totalBytes += t.TotalSizeBytes
	}

	snapshot := s.collector.GetSnapshot()

	resp := map[string]interface{}{
		"node_id":          s.handler.GetNodeID(),
		"host":             s.handler.GetHost(),
		"port":             s.handler.GetPort(),
		"data_dir":         s.handler.GetDataDir(),
		"total_topics":     totalTopics,
		"total_partitions": totalPartitions,
		"total_messages":   totalMessages,
		"total_bytes":      totalBytes,
		"ws_subscribers":   s.wsHub.ClientCount(),
		"metrics":          snapshot,
	}

	writeJSON(w, resp)
}

// REST HANDLER: GET /api/v1/topics
func (s *UIServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	topics := GetTopicsSummary(s.handler)
	writeJSON(w, topics)
}

// REST HANDLER: GET /api/v1/topics/{name}/messages
func (s *UIServer) handleTopicMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/topics/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "messages" {
		http.Error(w, "Invalid topic messages path", http.StatusBadRequest)
		return
	}

	topicName := parts[0]
	partitionStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	partitionID := int32(0)
	if partitionStr != "" {
		if p, err := strconv.Atoi(partitionStr); err == nil {
			partitionID = int32(p)
		}
	}

	startOffset := uint64(0)
	if offsetStr != "" {
		if o, err := strconv.ParseUint(offsetStr, 10, 64); err == nil {
			startOffset = o
		}
	}

	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
			limit = l
		}
	}

	type MessageDTO struct {
		Offset    uint64 `json:"offset"`
		Timestamp int64  `json:"timestamp"`
		Key       string `json:"key"`
		Value     string `json:"value"`
		Size      int    `json:"size"`
	}

	type PaginatedMessagesResponse struct {
		Topic     string       `json:"topic"`
		Partition int32        `json:"partition"`
		Offset    uint64       `json:"offset"`
		Limit     int          `json:"limit"`
		LEO       uint64       `json:"leo"`
		Messages  []MessageDTO `json:"messages"`
	}

	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	partitionsMap := s.handler.GetPartitions()
	pl, exists := partitionsMap[key]

	if !exists {
		writeJSON(w, PaginatedMessagesResponse{
			Topic:     topicName,
			Partition: partitionID,
			Offset:    startOffset,
			Limit:     limit,
			LEO:       0,
			Messages:  []MessageDTO{},
		})
		return
	}

	leo := pl.LEO()
	records, err := pl.Read(startOffset)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading records: %v", err), http.StatusInternalServerError)
		return
	}

	msgList := make([]MessageDTO, 0, len(records))
	for i, rec := range records {
		if i >= limit {
			break
		}
		msgList = append(msgList, MessageDTO{
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
			Key:       string(rec.Key),
			Value:     string(rec.Value),
			Size:      len(rec.Value),
		})
	}

	writeJSON(w, PaginatedMessagesResponse{
		Topic:     topicName,
		Partition: partitionID,
		Offset:    startOffset,
		Limit:     limit,
		LEO:       leo,
		Messages:  msgList,
	})
}

// REST HANDLER: GET /api/v1/groups
func (s *UIServer) handleGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	groups := GetGroupsSummary(s.handler)
	writeJSON(w, groups)
}

// REST HANDLER: GET /api/v1/metrics
func (s *UIServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshot := s.collector.GetSnapshot()
	writeJSON(w, snapshot)
}

// WEBSOCKET HANDLER: GET /ws/stream
func (s *UIServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	_, err := s.wsHub.Upgrade(w, r)
	if err != nil {
		log.Printf("[Web UI WS] Handshake upgrade failed: %v", err)
	}
}

func (s *UIServer) wsStreamLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if s.wsHub.ClientCount() == 0 {
			continue
		}

		snapshot := s.collector.GetSnapshot()
		topics := GetTopicsSummary(s.handler)
		groups := GetGroupsSummary(s.handler)

		payload := map[string]interface{}{
			"type":      "telemetry_tick",
			"timestamp": time.Now().UnixMilli(),
			"metrics":   snapshot,
			"topics":    topics,
			"groups":    groups,
		}

		data, err := json.Marshal(payload)
		if err == nil {
			s.wsHub.Broadcast(data)
		}
	}
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}
