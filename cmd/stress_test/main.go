package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

// ============================================================================
// STRUCT: StressMetrics
// Description: Lock-free atomic metric collection for 1M+ stress testing.
// ============================================================================
type StressMetrics struct {
	sentCount   uint64
	failedCount uint64
	bytesCount  uint64
	latencies   []time.Duration
	mu          sync.Mutex
}

func (m *StressMetrics) RecordSuccess(bytes uint64) {
	atomic.AddUint64(&m.sentCount, 1)
	atomic.AddUint64(&m.bytesCount, bytes)
}

func (m *StressMetrics) RecordFailure() {
	atomic.AddUint64(&m.failedCount, 1)
}

func (m *StressMetrics) MergeWorkerLatencies(lats []time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, lats...)
}

func main() {
	brokerFlag := flag.String("broker", "127.0.0.1:9092", "Kafka Broker TCP address")
	topicFlag := flag.String("topic", "stress.test.topic", "Target topic name for benchmark")
	workersFlag := flag.Int("workers", 20, "Number of concurrent worker goroutines")
	countFlag := flag.Int("messages", 1000000, "Total number of messages to produce")
	payloadSizeFlag := flag.Int("payload-size", 256, "Size of message payload in bytes")
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("  Kafka Go - 1 Million Request Benchmark Tool")
	fmt.Printf("  - Target Broker : %s\n", *brokerFlag)
	fmt.Printf("  - Target Topic  : %s\n", *topicFlag)
	fmt.Printf("  - Workers       : %d concurrent goroutines\n", *workersFlag)
	fmt.Printf("  - Total Messages: %d msgs (1 MILLION)\n", *countFlag)
	fmt.Printf("  - Payload Size  : %d bytes per message\n", *payloadSizeFlag)
	fmt.Println("  - Web Dashboard : http://localhost:8085 (Watch live metrics!)")
	fmt.Println("================================================================")

	payload := bytes.Repeat([]byte("A"), *payloadSizeFlag)
	msgsPerWorker := *countFlag / *workersFlag
	if msgsPerWorker <= 0 {
		msgsPerWorker = 1
	}

	metrics := &StressMetrics{
		latencies: make([]time.Duration, 0, *countFlag),
	}

	startTime := time.Now()

	// Live ticker reporter goroutine (ticks every 1s)
	doneChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastCount uint64 = 0
		var lastBytes uint64 = 0
		var lastTime = time.Now()

		for {
			select {
			case <-doneChan:
				return
			case now := <-ticker.C:
				currCount := atomic.LoadUint64(&metrics.sentCount)
				currBytes := atomic.LoadUint64(&metrics.bytesCount)
				elapsedSec := now.Sub(lastTime).Seconds()

				msgRate := float64(currCount-lastCount) / elapsedSec
				mbRate := (float64(currBytes-lastBytes) / (1024 * 1024)) / elapsedSec

				pct := float64(currCount) / float64(*countFlag) * 100
				fmt.Printf("[PROGRESS] Sent: %d/%d (%.1f%%) | Throughput: %.1f msg/sec (%.2f MB/sec)\n",
					currCount, *countFlag, pct, msgRate, mbRate)

				lastCount = currCount
				lastBytes = currBytes
				lastTime = now
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < *workersFlag; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(*brokerFlag, *topicFlag, workerID, msgsPerWorker, payload, metrics)
		}(w)
	}

	wg.Wait()
	close(doneChan)

	totalDuration := time.Since(startTime)
	totalSent := atomic.LoadUint64(&metrics.sentCount)
	totalFailed := atomic.LoadUint64(&metrics.failedCount)
	totalBytes := atomic.LoadUint64(&metrics.bytesCount)

	avgThroughput := float64(totalSent) / totalDuration.Seconds()
	mbThroughput := (float64(totalBytes) / (1024 * 1024)) / totalDuration.Seconds()

	// Calculate latency statistics
	var minLat, maxLat, avgLat, p95Lat, p99Lat time.Duration
	metrics.mu.Lock()
	lats := metrics.latencies
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		minLat = lats[0]
		maxLat = lats[len(lats)-1]
		var sumLat time.Duration
		for _, l := range lats {
			sumLat += l
		}
		avgLat = sumLat / time.Duration(len(lats))
		p95Lat = lats[int(float64(len(lats))*0.95)]
		p99Lat = lats[int(float64(len(lats))*0.99)]
	}
	metrics.mu.Unlock()

	fmt.Println("\n================================================================")
	fmt.Println("  1 MILLION REQUEST BENCHMARK COMPLETE")
	fmt.Println("================================================================")
	fmt.Printf("  Total Execution Time : %v\n", totalDuration)
	fmt.Printf("  Successful Messages  : %d msgs\n", totalSent)
	fmt.Printf("  Failed Messages      : %d msgs\n", totalFailed)
	fmt.Printf("  Total Data Transferred: %.2f MB\n", float64(totalBytes)/(1024*1024))
	fmt.Printf("  Average Throughput   : %.2f msg/sec\n", avgThroughput)
	fmt.Printf("  Data Rate            : %.2f MB/sec\n", mbThroughput)
	fmt.Println("----------------------------------------------------------------")
	fmt.Println("  LATENCY STATS:")
	fmt.Printf("    - Min Latency      : %v\n", minLat)
	fmt.Printf("    - Avg Latency      : %v\n", avgLat)
	fmt.Printf("    - P95 Latency      : %v\n", p95Lat)
	fmt.Printf("    - P99 Latency      : %v\n", p99Lat)
	fmt.Printf("    - Max Latency      : %v\n", maxLat)
	fmt.Println("================================================================")
}

func runWorker(broker string, topic string, workerID int, count int, payload []byte, metrics *StressMetrics) {
	conn, err := net.DialTimeout("tcp", broker, 5*time.Second)
	if err != nil {
		for i := 0; i < count; i++ {
			metrics.RecordFailure()
		}
		log.Printf("[Worker %d] Failed to connect: %v", workerID, err)
		return
	}
	defer conn.Close()

	localLats := make([]time.Duration, 0, count)

	for i := 0; i < count; i++ {
		start := time.Now()

		rec := &storage.Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte(fmt.Sprintf("w%d-k%d", workerID, i)),
			Value:     payload,
		}
		recBytes, err := rec.Marshal()
		if err != nil {
			metrics.RecordFailure()
			continue
		}

		header := &protocol.RequestHeader{
			ApiKey:        protocol.ApiKeyProduce,
			ApiVersion:    0,
			CorrelationId: int32(workerID*1000000 + i),
			ClientId:      "stress-tester",
		}

		produceReq := &protocol.ProduceRequest{
			Acks:    1,
			Timeout: 5000,
			Topics: []protocol.TopicProduceData{
				{
					TopicName: topic,
					Partitions: []protocol.PartitionProduceData{
						{
							PartitionId: 0,
							RecordsData: recBytes,
						},
					},
				},
			},
		}

		var bodyBuf bytes.Buffer
		if err := protocol.EncodeProduceRequest(&bodyBuf, produceReq); err != nil {
			metrics.RecordFailure()
			continue
		}

		var headerBuf bytes.Buffer
		if err := protocol.EncodeRequestHeader(&headerBuf, header); err != nil {
			metrics.RecordFailure()
			continue
		}

		totalSize := int32(headerBuf.Len() + bodyBuf.Len())
		var frameBuf bytes.Buffer
		_ = protocol.WriteInt32(&frameBuf, totalSize)
		frameBuf.Write(headerBuf.Bytes())
		frameBuf.Write(bodyBuf.Bytes())

		if _, err := conn.Write(frameBuf.Bytes()); err != nil {
			metrics.RecordFailure()
			continue
		}

		// Read TCP response
		if _, err := protocol.ReadInt32(conn); err != nil {
			metrics.RecordFailure()
			continue
		}
		if _, err := protocol.ReadInt32(conn); err != nil {
			metrics.RecordFailure()
			continue
		}
		if _, err := decodeProduceResponse(conn); err != nil {
			metrics.RecordFailure()
			continue
		}

		lat := time.Since(start)
		metrics.RecordSuccess(uint64(len(recBytes)))
		localLats = append(localLats, lat)
	}

	metrics.MergeWorkerLatencies(localLats)
}

func decodeProduceResponse(r net.Conn) (*protocol.ProduceResponse, error) {
	topicCount, err := protocol.ReadInt32(r)
	if err != nil {
		return nil, err
	}
	var topics []protocol.TopicProduceResponse
	for i := 0; i < int(topicCount); i++ {
		tName, err := protocol.ReadString(r)
		if err != nil {
			return nil, err
		}
		partCount, err := protocol.ReadInt32(r)
		if err != nil {
			return nil, err
		}
		var parts []protocol.PartitionProduceResponse
		for j := 0; j < int(partCount); j++ {
			pId, err := protocol.ReadInt32(r)
			if err != nil {
				return nil, err
			}
			errCode, err := protocol.ReadInt16(r)
			if err != nil {
				return nil, err
			}
			baseOff, err := protocol.ReadInt64(r)
			if err != nil {
				return nil, err
			}
			logTime, err := protocol.ReadInt64(r)
			if err != nil {
				return nil, err
			}
			parts = append(parts, protocol.PartitionProduceResponse{
				PartitionId:   pId,
				ErrorCode:     errCode,
				BaseOffset:    baseOff,
				LogAppendTime: logTime,
			})
		}
		topics = append(topics, protocol.TopicProduceResponse{
			TopicName:  tName,
			Partitions: parts,
		})
	}
	return &protocol.ProduceResponse{Topics: topics}, nil
}
