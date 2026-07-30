package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/config"
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
		RetentionMs:     retentionDur,
		RetentionBytes:  *retentionBytesFlag,
		CleanerInterval: time.Duration(*cleanerIntervalFlag) * time.Second,
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
	if *uiPortFlag > 0 {
		fmt.Printf("  - Web UI        : http://localhost:%d\n", *uiPortFlag)
	} else {
		fmt.Println("  - Web UI        : DISABLED")
	}
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

