package ui

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/coordinator"
	"github.com/HoangDinhBui/kafka-golang/internal/server"
)

// ============================================================================
// STRUCT: TelemetryCollector
// Description: Collects, calculates, and stores real-time broker metrics.
// ============================================================================
type TelemetryCollector struct {
	startTime time.Time

	// Counters
	totalMsgIn   uint64
	totalBytesIn uint64
	totalBytesOut uint64

	// Rates calculated every second
	msgInRate   float64
	bytesInRate float64
	bytesOutRate float64

	lastMsgIn   uint64
	lastBytesIn uint64
	lastBytesOut uint64
	lastCalcTime time.Time

	mu sync.RWMutex
}

// NewTelemetryCollector initializes the metric collector and starts background rate ticker.
func NewTelemetryCollector() *TelemetryCollector {
	tc := &TelemetryCollector{
		startTime:    time.Now(),
		lastCalcTime: time.Now(),
	}

	go tc.rateLoop()
	return tc
}

// RecordMsgIn records incoming produced messages and byte counts.
func (tc *TelemetryCollector) RecordMsgIn(count uint64, bytes uint64) {
	atomic.AddUint64(&tc.totalMsgIn, count)
	atomic.AddUint64(&tc.totalBytesIn, bytes)
}

// RecordBytesOut records fetched outgoing bytes.
func (tc *TelemetryCollector) RecordBytesOut(bytes uint64) {
	atomic.AddUint64(&tc.totalBytesOut, bytes)
}

func (tc *TelemetryCollector) rateLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for now := range ticker.C {
		tc.mu.Lock()
		elapsed := now.Sub(tc.lastCalcTime).Seconds()
		if elapsed > 0 {
			currMsgIn := atomic.LoadUint64(&tc.totalMsgIn)
			currBytesIn := atomic.LoadUint64(&tc.totalBytesIn)
			currBytesOut := atomic.LoadUint64(&tc.totalBytesOut)

			tc.msgInRate = float64(currMsgIn-tc.lastMsgIn) / elapsed
			tc.bytesInRate = float64(currBytesIn-tc.lastBytesIn) / elapsed
			tc.bytesOutRate = float64(currBytesOut-tc.lastBytesOut) / elapsed

			tc.lastMsgIn = currMsgIn
			tc.lastBytesIn = currBytesIn
			tc.lastBytesOut = currBytesOut
			tc.lastCalcTime = now
		}
		tc.mu.Unlock()
	}
}

// Snapshot system and broker metrics.
type MetricsSnapshot struct {
	UptimeSeconds   int64   `json:"uptime_seconds"`
	TotalMsgIn      uint64  `json:"total_msg_in"`
	TotalBytesIn    uint64  `json:"total_bytes_in"`
	TotalBytesOut   uint64  `json:"total_bytes_out"`
	MsgInRate       float64 `json:"msg_in_rate"`
	BytesInRate     float64 `json:"bytes_in_rate"`
	BytesOutRate    float64 `json:"bytes_out_rate"`
	Goroutines      int     `json:"goroutines"`
	AllocHeapMB     float64 `json:"alloc_heap_mb"`
	SysMemMB        float64 `json:"sys_mem_mb"`
	NumGC           uint32  `json:"num_gc"`
}

func (tc *TelemetryCollector) GetSnapshot() MetricsSnapshot {
	tc.mu.RLock()
	msgRate := tc.msgInRate
	bInRate := tc.bytesInRate
	bOutRate := tc.bytesOutRate
	tc.mu.RUnlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MetricsSnapshot{
		UptimeSeconds: int64(time.Since(tc.startTime).Seconds()),
		TotalMsgIn:    atomic.LoadUint64(&tc.totalMsgIn),
		TotalBytesIn:  atomic.LoadUint64(&tc.totalBytesIn),
		TotalBytesOut: atomic.LoadUint64(&tc.totalBytesOut),
		MsgInRate:     msgRate,
		BytesInRate:   bInRate,
		BytesOutRate:  bOutRate,
		Goroutines:    runtime.NumGoroutine(),
		AllocHeapMB:   float64(m.Alloc) / (1024 * 1024),
		SysMemMB:      float64(m.Sys) / (1024 * 1024),
		NumGC:         m.NumGC,
	}
}

// TopicPartitionInfo struct for UI JSON payload
type TopicPartitionInfo struct {
	PartitionID   int32  `json:"partition_id"`
	LEO           uint64 `json:"leo"`
	BaseOffset    uint64 `json:"base_offset"`
	SegmentsCount int    `json:"segments_count"`
	SizeBytes     int64  `json:"size_bytes"`
}

// TopicInfo struct for UI JSON payload
type TopicInfo struct {
	TopicName      string               `json:"topic_name"`
	PartitionsCount int                 `json:"partitions_count"`
	TotalMessages  uint64               `json:"total_messages"`
	TotalSizeBytes int64                `json:"total_size_bytes"`
	Partitions     []TopicPartitionInfo `json:"partitions"`
}

// ExtractTopicsSummary returns overview of all topics & partitions from server.Handler.
func GetTopicsSummary(h *server.Handler) []TopicInfo {
	partitionsMap := h.GetPartitions()
	topicMap := make(map[string][]TopicPartitionInfo)
	topicMsgMap := make(map[string]uint64)
	topicSizeMap := make(map[string]int64)

	for key, pl := range partitionsMap {
		// Key format: topicName-partitionID
		lastDashIdx := strings.LastIndex(key, "-")
		if lastDashIdx == -1 {
			continue
		}
		topicName := key[:lastDashIdx]
		partIDStr := key[lastDashIdx+1:]
		var partID int32
		_ = parsePartitionID(partIDStr, &partID)

		leo := pl.LEO()
		baseOff := pl.BaseOffset()
		segCount, sizeBytes := pl.Stats()

		partInfo := TopicPartitionInfo{
			PartitionID:   partID,
			LEO:           leo,
			BaseOffset:    baseOff,
			SegmentsCount: segCount,
			SizeBytes:     sizeBytes,
		}

		topicMap[topicName] = append(topicMap[topicName], partInfo)
		topicMsgMap[topicName] += leo
		topicSizeMap[topicName] += sizeBytes
	}

	result := make([]TopicInfo, 0, len(topicMap))
	for tName, parts := range topicMap {
		result = append(result, TopicInfo{
			TopicName:       tName,
			PartitionsCount: len(parts),
			TotalMessages:   topicMsgMap[tName],
			TotalSizeBytes:  topicSizeMap[tName],
			Partitions:      parts,
		})
	}

	return result
}

// ConsumerLagInfo holds detailed lag breakdown for consumer groups
type ConsumerLagInfo struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	LogEndOffset    uint64 `json:"log_end_offset"`
	Lag             int64  `json:"lag"`
	CommitTime      string `json:"commit_time"`
}

type ConsumerGroupUI struct {
	GroupID      string            `json:"group_id"`
	State        string            `json:"state"`
	ProtocolType string            `json:"protocol_type"`
	LeaderID     string            `json:"leader_id"`
	GenerationID int32             `json:"generation_id"`
	MembersCount int               `json:"members_count"`
	Members      []*coordinator.GroupMember `json:"members"`
	LagInfo      []ConsumerLagInfo `json:"lag_info"`
}

// GetGroupsSummary returns consumer group lag and member details.
func GetGroupsSummary(h *server.Handler) []ConsumerGroupUI {
	coord := h.GetGroupCoordinator()
	offsetMgr := h.GetOffsetManager()
	partitionsMap := h.GetPartitions()

	rawGroups := coord.GetAllGroups()
	result := make([]ConsumerGroupUI, 0, len(rawGroups))

	for _, g := range rawGroups {
		members := make([]*coordinator.GroupMember, 0, len(g.Members))
		for _, m := range g.Members {
			members = append(members, m)
		}

		groupOffsets := offsetMgr.FetchGroupOffsets(g.GroupID)
		lagList := make([]ConsumerLagInfo, 0, len(groupOffsets))

		for tp, meta := range groupOffsets {
			key := tp.Topic + "-" + string(rune('0'+tp.Partition))
			var leo uint64 = 0
			if pl, ok := partitionsMap[key]; ok {
				leo = pl.LEO()
			}

			var lag int64 = 0
			if int64(leo) >= meta.Offset {
				lag = int64(leo) - meta.Offset
			}

			lagList = append(lagList, ConsumerLagInfo{
				Topic:           tp.Topic,
				Partition:       tp.Partition,
				CommittedOffset: meta.Offset,
				LogEndOffset:    leo,
				Lag:             lag,
				CommitTime:      meta.CommitTime.Format(time.RFC3339),
			})
		}

		result = append(result, ConsumerGroupUI{
			GroupID:      g.GroupID,
			State:        string(g.State),
			ProtocolType: g.ProtocolType,
			LeaderID:     g.LeaderID,
			GenerationID: g.GenerationID,
			MembersCount: len(members),
			Members:      members,
			LagInfo:      lagList,
		})
	}

	return result
}

func parsePartitionID(s string, out *int32) error {
	var val int32
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		val = val*10 + int32(c-'0')
	}
	*out = val
	return nil
}
