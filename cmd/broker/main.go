package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/HoangDinhBui/kafka-golang/internal/config"
	"github.com/HoangDinhBui/kafka-golang/internal/server"
)

func main() {
	// Parse CLI flags
	portFlag := flag.String("port", "9092", "TCP listener port for Kafka broker")
	dataDirFlag := flag.String("data-dir", "./data", "Directory path for log storage")
	nodeIdFlag := flag.Int("node-id", 1, "Unique integer Node ID for this broker")
	hostFlag := flag.String("host", "127.0.0.1", "Broker host or IP address")
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

	// Print startup banner
	fmt.Println("================================================================")
	fmt.Println("  Apache Kafka Clone in Go (kafka-golang)")
	fmt.Printf("  - Node ID   : %d\n", *nodeIdFlag)
	fmt.Printf("  - Address   : %s:%s\n", *hostFlag, cfg.Port)
	fmt.Printf("  - Data Dir  : %s\n", cfg.DataDir)
	fmt.Println("  - Status    : READY & LISTENING FOR CLIENTS")
	fmt.Println("================================================================")

	// Channel for catching OS signals (SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start TCP server in a background goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		if err := tcpServer.Start(); err != nil {
			serverErrChan <- err
		}
	}()

	// Wait for termination signal or server error
	select {
	case err := <-serverErrChan:
		log.Fatalf("TCP Server error: %v", err)
	case sig := <-sigChan:
		fmt.Printf("\n[Signal Received: %v] Shutting down Kafka Broker gracefully...\n", sig)
		if err := tcpServer.Stop(); err != nil {
			log.Printf("Error stopping TCP server: %v\n", err)
		}
		fmt.Println("[Shutdown Complete] Goodbye!")
	}
}
