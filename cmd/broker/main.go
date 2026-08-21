package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/config"
	"github.com/HoangDinhBui/kafka-golang/internal/security"
	"github.com/HoangDinhBui/kafka-golang/internal/server"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
	"github.com/HoangDinhBui/kafka-golang/internal/ui"
)

func main() {
	// Parse CLI flags
	portFlag := flag.String("port", "9092", "TCP listener port for Kafka broker")
	dataDirFlag := flag.String("data-dir", "./data", "Directory path for log storage")
	nodeIdFlag := flag.Int("node-id", 1, "Unique integer Node ID for this broker")
	hostFlag := flag.String("host", "127.0.0.1", "Broker host or IP address")
	uiPortFlag := flag.Int("ui-port", 8080, "Port for Web Management UI (pass 0 to disable)")
	retentionHoursFlag := flag.Int("retention-hours", 168, "Log retention threshold in hours (pass <= 0 to disable)")
	retentionBytesFlag := flag.Int64("retention-bytes", -1, "Log retention size threshold in bytes per partition (pass <= 0 to disable)")
	cleanerIntervalFlag := flag.Int("cleaner-interval-sec", 60, "Interval in seconds between cleaner background runs")
	enableCompactionFlag := flag.Bool("enable-compaction", false, "Enable key-based log compaction on closed segments during cleaner runs (permanently drops superseded records and tombstones)")
	tlsFlag := flag.Bool("tls", false, "Enable TLS/SSL encryption for TCP listener")
	tlsCertFlag := flag.String("tls-cert", "", "Path to PEM certificate file for TLS (generates a self-signed cert if empty)")
	tlsKeyFlag := flag.String("tls-key", "", "Path to PEM private key file for TLS (generates a self-signed cert if empty)")
	saslEnabledFlag := flag.Bool("sasl-enabled", false, "Require SASL/PLAIN or SASL/SCRAM-SHA-256 authentication before serving any other request")
	saslUsersFlag := flag.String("sasl-users", "", "Comma-separated user:password pairs to register for SASL auth (e.g. admin:secret,alice:pass)")
	aclRulesFlag := flag.String("acl-rules", "", "Comma-separated ACL rules as principal|resourceType|resourceName|operation|permission (e.g. 'User:alice|Topic|orders|Write|Allow'); resourceType: Topic/Group/Cluster, operation: Read/Write/Describe/All, permission: Allow/Deny")
	flag.Parse()

	// Initialize configuration
	cfg := config.DefaultConfig()
	cfg.Port = *portFlag
	cfg.DataDir = *dataDirFlag

	portInt, err := strconv.Atoi(cfg.Port)
	if err != nil {
		log.Fatalf("Invalid port number: %s", cfg.Port)
	}

	// Initialize request router handler & TCP server
	handler := server.NewHandler(cfg.DataDir, int32(*nodeIdFlag), *hostFlag, int32(portInt))
	bindAddr := fmt.Sprintf(":%s", cfg.Port)
	tcpServer := server.NewTCPServer(bindAddr, handler)

	// Register SASL user credentials, if provided. No default/hardcoded
	// accounts are seeded — every principal must be supplied explicitly.
	saslUserCount := 0
	if *saslUsersFlag != "" {
		for _, pair := range strings.Split(*saslUsersFlag, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				log.Fatalf("Invalid -sasl-users entry %q: expected format user:password", pair)
			}
			if err := handler.AddSASLUser(parts[0], parts[1]); err != nil {
				log.Fatalf("Failed to register SASL user %q: %v", parts[0], err)
			}
			saslUserCount++
		}
	}
	if *saslEnabledFlag && saslUserCount == 0 {
		log.Println("[Warning] -sasl-enabled is set but no -sasl-users were registered; no client will be able to authenticate.")
	}
	handler.SetSASLRequired(*saslEnabledFlag)

	// Register ACL rules, if provided. With no rules registered, ACLManager
	// defaults to allowing all access (matching the pre-existing behavior).
	aclRuleCount := 0
	if *aclRulesFlag != "" {
		for _, entry := range strings.Split(*aclRulesFlag, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			fields := strings.Split(entry, "|")
			if len(fields) != 5 {
				log.Fatalf("Invalid -acl-rules entry %q: expected principal|resourceType|resourceName|operation|permission", entry)
			}
			handler.AddACLRule(security.ACLRule{
				Principal:      fields[0],
				ResourceType:   fields[1],
				ResourceName:   fields[2],
				Operation:      fields[3],
				PermissionType: fields[4],
			})
			aclRuleCount++
		}
	}

	// Enable TLS on the TCP listener, if requested.
	if *tlsFlag {
		tlsConfig, err := security.CreateTLSConfig(*tlsCertFlag, *tlsKeyFlag)
		if err != nil {
			log.Fatalf("Failed to initialize TLS config: %v", err)
		}
		tcpServer.SetTLSConfig(tlsConfig)
	}

	var uiServer *ui.UIServer
	if *uiPortFlag > 0 {
		uiServer = ui.NewServer(handler, *uiPortFlag)
	}

	// Initialize Background Log Retention & Compaction Worker
	var retentionDur time.Duration
	if *retentionHoursFlag > 0 {
		retentionDur = time.Duration(*retentionHoursFlag) * time.Hour
	}

	cleanerCfg := storage.CleanerConfig{
		RetentionMs:       retentionDur,
		RetentionBytes:    *retentionBytesFlag,
		CleanerInterval:   time.Duration(*cleanerIntervalFlag) * time.Second,
		CompactionEnabled: *enableCompactionFlag,
	}
	cleanerWorker := storage.NewCleanerWorker(handler.GetPartitions, cleanerCfg)

	// Print startup banner
	fmt.Println("================================================================")
	fmt.Println("  Apache Kafka Clone in Go (kafka-golang)")
	fmt.Printf("  - Node ID       : %d\n", *nodeIdFlag)
	fmt.Printf("  - Address       : %s:%s\n", *hostFlag, cfg.Port)
	fmt.Printf("  - Data Dir      : %s\n", cfg.DataDir)
	if *retentionHoursFlag > 0 {
		fmt.Printf("  - Retention Time: %d hours (%v)\n", *retentionHoursFlag, retentionDur)
	} else {
		fmt.Println("  - Retention Time: UNLIMITED")
	}
	if *retentionBytesFlag > 0 {
		fmt.Printf("  - Retention Size: %d bytes per partition\n", *retentionBytesFlag)
	} else {
		fmt.Println("  - Retention Size: UNLIMITED")
	}
	if *enableCompactionFlag {
		fmt.Println("  - Log Compaction: ENABLED (key-based, closed segments only)")
	} else {
		fmt.Println("  - Log Compaction: DISABLED")
	}
	if *uiPortFlag > 0 {
		fmt.Printf("  - Web UI        : http://localhost:%d\n", *uiPortFlag)
	} else {
		fmt.Println("  - Web UI        : DISABLED")
	}
	if *tlsFlag {
		fmt.Println("  - Security TLS  : ENABLED (TLS 1.3)")
	} else {
		fmt.Println("  - Security TLS  : PLAINTEXT")
	}
	if *saslEnabledFlag {
		fmt.Printf("  - Security SASL : ENABLED (PLAIN, SCRAM-SHA-256), required, %d user(s) registered\n", saslUserCount)
	} else {
		fmt.Println("  - Security SASL : DISABLED")
	}
	fmt.Printf("  - ACL Rules     : %d rule(s) registered (allow-all if none)\n", aclRuleCount)
	fmt.Println("  - Status        : READY & LISTENING FOR CLIENTS")
	fmt.Println("================================================================")

	// Channel for catching OS signals (SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start Background Cleaner Worker
	cleanerWorker.Start()

	// Start TCP server in a background goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		if err := tcpServer.Start(); err != nil {
			serverErrChan <- err
		}
	}()

	// Start Web UI Server if enabled
	if uiServer != nil {
		go func() {
			if err := uiServer.Start(); err != nil {
				log.Printf("Web UI Server error: %v\n", err)
			}
		}()
	}

	// Wait for termination signal or server error
	select {
	case err := <-serverErrChan:
		log.Fatalf("TCP Server error: %v", err)
	case sig := <-sigChan:
		fmt.Printf("\n[Signal Received: %v] Shutting down Kafka Broker gracefully...\n", sig)
		cleanerWorker.Stop()
		if uiServer != nil {
			if err := uiServer.Stop(); err != nil {
				log.Printf("Error stopping Web UI server: %v\n", err)
			}
		}
		if err := tcpServer.Stop(); err != nil {
			log.Printf("Error stopping TCP server: %v\n", err)
		}
		fmt.Println("[Shutdown Complete] Goodbye!")
	}
}

