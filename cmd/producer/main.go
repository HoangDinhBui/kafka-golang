package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

func main() {
	brokerFlag := flag.String("broker", "127.0.0.1:9092", "Kafka Broker TCP address")
	topicFlag := flag.String("topic", "queuing.b2b.nvr.alarm", "Target topic name")
	partitionFlag := flag.Int("partition", 0, "Target partition ID")
	msgFlag := flag.String("msg", "", "Message value payload (if empty, starts interactive console)")
	flag.Parse()

	if *msgFlag != "" {
		if err := sendRecord(*brokerFlag, *topicFlag, int32(*partitionFlag), []byte(*msgFlag)); err != nil {
			log.Fatalf("[Error] Failed to send message: %v", err)
		}
		fmt.Printf("[OK] Message published to topic '%s' (partition %d)\n", *topicFlag, *partitionFlag)
		return
	}

	fmt.Println("================================================================")
	fmt.Println("  Kafka Go - Built-in CLI Producer")
	fmt.Printf("  - Broker   : %s\n", *brokerFlag)
	fmt.Printf("  - Topic    : %s\n", *topicFlag)
	fmt.Printf("  - Partition: %d\n", *partitionFlag)
	fmt.Println("  - Mode     : Interactive (Type message & press ENTER, Ctrl+C to exit)")
	fmt.Println("================================================================")

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text != "" {
			if err := sendRecord(*brokerFlag, *topicFlag, int32(*partitionFlag), []byte(text)); err != nil {
				fmt.Printf("[FAIL] Send error: %v\n", err)
			} else {
				fmt.Printf("[OK] Sent 1 message to '%s'\n", *topicFlag)
			}
		}
		fmt.Print("> ")
	}
}

func sendRecord(broker string, topic string, partitionID int32, payload []byte) error {
	conn, err := net.DialTimeout("tcp", broker, 3*time.Second)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer conn.Close()

	rec := &storage.Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("cli-key"),
		Value:     payload,
	}
	recBytes, err := rec.Marshal()
	if err != nil {
		return fmt.Errorf("record marshal failed: %w", err)
	}

	header := &protocol.RequestHeader{
		ApiKey:        protocol.ApiKeyProduce,
		ApiVersion:    0,
		CorrelationId: 1001,
		ClientId:      "go-cli-producer",
	}

	produceReq := &protocol.ProduceRequest{
		Acks:    1,
		Timeout: 5000,
		Topics: []protocol.TopicProduceData{
			{
				TopicName: topic,
				Partitions: []protocol.PartitionProduceData{
					{
						PartitionId: partitionID,
						RecordsData: recBytes,
					},
				},
			},
		},
	}

	var bodyBuf bytes.Buffer
	if err := protocol.EncodeProduceRequest(&bodyBuf, produceReq); err != nil {
		return err
	}

	var headerBuf bytes.Buffer
	if err := protocol.EncodeRequestHeader(&headerBuf, header); err != nil {
		return err
	}

	totalSize := int32(headerBuf.Len() + bodyBuf.Len())

	var frameBuf bytes.Buffer
	if err := protocol.WriteInt32(&frameBuf, totalSize); err != nil {
		return err
	}
	frameBuf.Write(headerBuf.Bytes())
	frameBuf.Write(bodyBuf.Bytes())

	if _, err := conn.Write(frameBuf.Bytes()); err != nil {
		return err
	}

	// Read response size
	_, err = protocol.ReadInt32(conn)
	if err != nil {
		return err
	}

	// Read correlation ID header
	_, err = protocol.ReadInt32(conn)
	if err != nil {
		return err
	}

	// Decode produce response
	resp, err := decodeProduceResponse(conn)
	if err != nil {
		return err
	}

	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		if resp.Topics[0].Partitions[0].ErrorCode != 0 {
			return fmt.Errorf("broker returned error code %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	}

	return nil
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
