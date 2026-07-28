# kafka-golang

> An Apache Kafka-compatible message broker and binary wire protocol engine written in Go.

## Features

- **Sequential Commit Log**: High-throughput append-only disk storage (`.log`).
- **4KB Sparse Indexing**: Fast $O(\log N)$ binary search on disk (`.index`).
- **Kafka Protocol Compatibility**: Supports 9 core Kafka binary APIs over raw TCP.
- **Consumer Group Coordinator**: Manages offset persistence, group membership, and rebalancing.
- **Multi-Broker Replication**: High Watermark (HW) tracking and follower replication loops.
- **KRaft Consensus Engine**: Built-in Raft metadata controller state machine without ZooKeeper.
- **Zero Dependencies**: Built entirely with Go standard library.

## Install

### Prerequisites

- Go `1.22` or higher.

### Build Binary

```bash
go build -o bin/broker cmd/broker/main.go
```

## Usage

Start the broker with default options (port `9092`, data directory `./data`, node ID `1`):

```bash
./bin/broker
```

Or specify custom flags:

```bash
./bin/broker -port 9095 -data-dir ./my-data -node-id 2 -host 127.0.0.1
```

### CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-port` | `9092` | TCP listener port for the broker |
| `-data-dir` | `./data` | Directory path for storing partition log segments and indices |
| `-node-id` | `1` | Unique integer ID identifying this broker node |
| `-host` | `127.0.0.1` | Hostname or IP address advertised by the broker |

### Testing

Run all unit and integration tests with the Go race detector:

```bash
go test -v -race ./...
```

## Architecture

```text
kafka-golang/
├── cmd/broker/main.go           # CLI flags, OS signal handling (SIGINT/SIGTERM), server startup
├── internal/
│   ├── config/                  # Default broker configuration
│   ├── storage/                 # Storage engine (.log, .index, Record, Segment, PartitionLog)
│   ├── protocol/                # Binary wire protocol encoders and decoders for 9 Kafka APIs
│   ├── server/                  # TCP server, framing decoder, request router
│   ├── coordinator/             # Consumer group coordinator and offset manager
│   ├── replication/             # Replica manager, LEO/HW tracking, follower fetcher loop
│   └── consensus/               # KRaft (Raft) consensus node state machine
└── docs/
    └── architecture-faq.md      # Detailed architecture Q&A guide
```

## Supported Kafka Protocol APIs

| API Name | ApiKey | Implementation |
| :--- | :--- | :--- |
| **Produce** | `0` | `internal/protocol/produce.go` |
| **Fetch** | `1` | `internal/protocol/fetch.go` |
| **Metadata** | `3` | `internal/protocol/metadata.go` |
| **OffsetCommit** | `8` | `internal/protocol/offset.go` |
| **OffsetFetch** | `9` | `internal/protocol/offset.go` |
| **JoinGroup** | `11` | `internal/protocol/group.go` |
| **Heartbeat** | `12` | `internal/protocol/group.go` |
| **SyncGroup** | `14` | `internal/protocol/group.go` |
| **ApiVersions** | `18` | `internal/protocol/api_version.go` |

## License

MIT
