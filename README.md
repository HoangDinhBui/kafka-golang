# kafka-golang

> An Apache Kafka-compatible message broker, zero-copy binary wire protocol engine, and web management dashboard written in Go.

---

## 🌟 Key Features

- **Sequential Commit Log**: High-throughput append-only disk storage (`.log`).
- **4KB Sparse Indexing**: Fast $O(\log N)$ binary search on disk (`.index`).
- **Zero-Copy `sendfile` Transfer Engine**: Kernel-level zero-copy file-to-socket streaming (`zero_copy.go`) for zero-allocation Consumer Fetch requests.
- **Automated Log Retention & Compaction**: Background worker (`CleanerWorker`) for time-based retention, size-based limits, and Key Log Compaction.
- **Embedded Web Management Dashboard**: Crisp engineering UI at `http://localhost:8085` with WebSocket real-time telemetry (Throughput, RAM, Lag) and paginated message inspector.
- **Kafka Protocol Compatibility**: Supports 9 core Kafka binary APIs over raw TCP.
- **Consumer Group Coordinator**: Manages offset persistence into `__consumer_offsets`, group membership, and rebalancing.
- **Multi-Broker Replication**: High Watermark (HW) tracking and follower replication loops.
- **KRaft Consensus Engine**: Built-in Raft metadata controller state machine without ZooKeeper.
- **Built-in Benchmark Tools**: Multi-threaded stress test tool (`cmd/stress_test`) achieving **22,000+ msgs/sec** (1,000,000 requests at < 0.9ms latency).
- **Zero Dependencies**: Built entirely with Go standard library.

---

## 🚀 Quick Start

### Prerequisites
- Go `1.22` or higher.

### Build Executables

```bash
# Build Broker Binary
go build -o bin/broker cmd/broker/main.go

# Build Interactive Producer CLI
go build -o bin/producer cmd/producer/main.go

# Build 1M Request Stress Test Benchmark Tool
go build -o bin/stress_test cmd/stress_test/main.go
```

### Run Broker

Start the broker with default flags (TCP port `9092`, Web UI port `8085`, data directory `./data`):

```bash
./bin/broker
```

Access the Web Management Dashboard in your browser:
👉 **[http://localhost:8085](http://localhost:8085)**

---

## ⚙️ CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-port` | `9092` | TCP listener port for the broker binary protocol |
| `-ui-port` | `8085` | HTTP listener port for Web Management Dashboard & WebSocket Telemetry |
| `-data-dir` | `./data` | Directory path for storing partition log segments and indices |
| `-node-id` | `1` | Unique integer ID identifying this broker node |
| `-host` | `127.0.0.1` | Hostname or IP address advertised by the broker |
| `-retention-hours` | `168` | Log segment retention limit in hours (default 7 days) |
| `-retention-bytes` | `-1` | Max partition log disk limit in bytes (`-1` = unlimited) |
| `-cleaner-interval-sec` | `60` | Execution ticker interval for background cleaner worker in seconds |

---

## 📊 Benchmark & Testing

### Benchmark 1,000,000 Requests
Run the multi-threaded stress test tool with 20 worker goroutines:

```bash
go run cmd/stress_test/main.go -messages 1000000 -workers 20
```

### Run Tests & Race Detector

```bash
go test -v -race ./...
```

---

## 🏗️ Architecture Layout

```text
kafka-golang/
├── cmd/
│   ├── broker/main.go          # Broker entrypoint, CLI flags, OS signal handling
│   ├── producer/main.go        # Interactive CLI Producer tool
│   └── stress_test/main.go     # Multi-threaded 1M request benchmark tool
├── internal/
│   ├── config/                 # Default broker configuration
│   ├── storage/                # Storage engine (.log, .index, zero_copy.go, cleaner.go)
│   ├── protocol/               # Binary wire protocol encoders and decoders (9 APIs)
│   ├── server/                 # TCP server, framing decoder, request router
│   ├── coordinator/            # Consumer group coordinator and offset manager
│   ├── replication/            # Replica manager, LEO/HW tracking, follower fetcher
│   ├── consensus/              # KRaft (Raft) consensus node state machine
│   └── ui/                     # Web UI Server, static assets, WebSocket telemetry hub
└── docs/                       # Architecture documentation and Q&A guide
```

---

## 📄 License

MIT
