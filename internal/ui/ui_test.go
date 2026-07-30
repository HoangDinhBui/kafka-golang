package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/server"
)

func TestUIServer_Endpoints(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kafka_ui_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	handler := server.NewHandler(tempDir, 1, "127.0.0.1", 9092)
	uiServer := NewServer(handler, 8089)

	// Test 1: GET /api/v1/cluster
	t.Run("GET /api/v1/cluster", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/cluster", nil)
		rr := httptest.NewRecorder()
		uiServer.httpServer.Handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rr.Code)
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to decode JSON response: %v", err)
		}

		if resp["node_id"].(float64) != 1 {
			t.Errorf("Expected node_id 1, got %v", resp["node_id"])
		}
	})

	// Test 2: GET /api/v1/topics
	t.Run("GET /api/v1/topics", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/topics", nil)
		rr := httptest.NewRecorder()
		uiServer.httpServer.Handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rr.Code)
		}

		var topics []TopicInfo
		if err := json.Unmarshal(rr.Body.Bytes(), &topics); err != nil {
			t.Fatalf("Failed to decode JSON response: %v", err)
		}
	})

	// Test 3: GET /api/v1/groups
	t.Run("GET /api/v1/groups", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/groups", nil)
		rr := httptest.NewRecorder()
		uiServer.httpServer.Handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rr.Code)
		}
	})

	// Test 4: GET /api/v1/metrics
	t.Run("GET /api/v1/metrics", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/metrics", nil)
		rr := httptest.NewRecorder()
		uiServer.httpServer.Handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rr.Code)
		}
	})

	// Test 5: Embedded static asset GET /
	t.Run("GET / index.html", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()
		uiServer.httpServer.Handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200 for embedded index.html, got %d", rr.Code)
		}
	})
}

func TestTelemetryCollector(t *testing.T) {
	tc := NewTelemetryCollector()
	tc.RecordMsgIn(100, 1024)
	tc.RecordBytesOut(2048)

	time.Sleep(100 * time.Millisecond)
	snapshot := tc.GetSnapshot()

	if snapshot.TotalMsgIn != 100 {
		t.Errorf("Expected TotalMsgIn 100, got %d", snapshot.TotalMsgIn)
	}
	if snapshot.TotalBytesIn != 1024 {
		t.Errorf("Expected TotalBytesIn 1024, got %d", snapshot.TotalBytesIn)
	}
	if snapshot.TotalBytesOut != 2048 {
		t.Errorf("Expected TotalBytesOut 2048, got %d", snapshot.TotalBytesOut)
	}
}
